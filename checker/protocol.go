package checker

import (
	"crypto/sha256"
	"fmt"
	"maps"
	"sort"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"
)

type rawFacts struct {
	actions   map[string]*re.Action
	platforms map[string]map[string]string
	results   map[string]*re.ActionResult
	digests   map[string][]Digest
	status    map[string]int32
}

func hashBytes(b []byte) Digest {
	return Digest{Hash: fmt.Sprintf("%x", sha256.Sum256(b)), SizeBytes: int64(len(b))}
}
func modelDigest(d *re.Digest) Digest {
	if d == nil {
		return Digest{}
	}
	return Digest{Hash: d.Hash, SizeBytes: d.SizeBytes}
}
func actionPlatform(a *re.Action) map[string]string {
	p := map[string]string{}
	for _, v := range a.GetPlatform().GetProperties() {
		p[v.Name] = v.Value
	}
	return p
}

// inspectProtocol derives all eligibility and output-reference facts from raw
// independently captured protobufs, never from scheduler-derived JSON fields.
func inspectProtocol(l Ledger, external []Evidence, issue func(int, string, string, string, string)) rawFacts {
	facts := rawFacts{
		actions:   map[string]*re.Action{},
		platforms: map[string]map[string]string{},
		results:   map[string]*re.ActionResult{},
		digests:   map[string][]Digest{},
		status:    map[string]int32{},
	}
	committed := map[string]bool{}
	for _, event := range l.Events {
		if event.Kind == "attempt_committed" {
			committed[event.AttemptID] = true
		}
	}
	content := map[Digest][]byte{}
	for _, row := range external {
		if row.Source == "cas" {
			for _, blob := range row.Blobs {
				if hashBytes(blob.Data) == blob.Digest {
					content[blob.Digest] = blob.Data
				}
			}
		}
	}
	for _, e := range l.Executions {
		if raw, ok := content[e.ActionDigest]; ok {
			var action re.Action
			if proto.Unmarshal(raw, &action) == nil {
				facts.actions[e.ID] = &action
			}
		}
		for _, row := range external {
			if row.ExecutionID != e.ID || row.Action == nil {
				continue
			}
			if row.ActionDigest != e.ActionDigest || hashBytes(row.Action) != e.ActionDigest {
				issue(
					1,
					"action_digest_mismatch",
					e.ID,
					row.AttemptID,
					"captured Action bytes do not match execution key",
				)
				continue
			}
			var a re.Action
			if err := proto.Unmarshal(row.Action, &a); err != nil {
				issue(1, "invalid_action", e.ID, row.AttemptID, err.Error())
				continue
			}
			facts.actions[e.ID] = &a
		}
		a := facts.actions[e.ID]
		if a == nil {
			issue(1, "evidence_missing", e.ID, "", "raw digest-verified Action evidence is required")
			continue
		}
		if e.Action != nil && hashBytes(e.Action) != e.ActionDigest {
			issue(1, "ledger_action_mismatch", e.ID, "", "ledger Action bytes do not match execution key")
		}
		p := actionPlatform(a)
		if len(p) == 0 && a.CommandDigest != nil {
			found := false
			if raw, ok := content[modelDigest(a.CommandDigest)]; ok {
				var command re.Command
				if proto.Unmarshal(raw, &command) == nil {
					found = true
					for _, property := range command.GetPlatform().GetProperties() {
						p[property.Name] = property.Value
					}
				}
			}
			for _, row := range external {
				if row.ExecutionID == e.ID &&
					row.Command != nil &&
					hashBytes(row.Command) == modelDigest(a.CommandDigest) {
					var command re.Command
					if proto.Unmarshal(row.Command, &command) == nil {
						found = true
						for _, property := range command.GetPlatform().GetProperties() {
							p[property.Name] = property.Value
						}
					}
				}
			}
			if !found {
				issue(
					3,
					"evidence_missing",
					e.ID,
					"",
					"Action has no platform; digest-verified original Command required",
				)
			}
		}
		facts.platforms[e.ID] = p
		if !maps.Equal(p, e.Platform) {
			issue(3, "derived_platform_mismatch", e.ID, "", "ledger platform differs from captured Action")
		}
		hw := p["remex-hw"] == "true" || p["remex-hw"] == "1"
		if e.DoNotCache != a.DoNotCache || e.HardwareTest != hw {
			issue(4, "derived_policy_mismatch", e.ID, "", "ledger cache policy differs from captured Action")
		}
	}
	for _, a := range l.Attempts {
		for _, row := range external {
			if row.Source != "worker" ||
				row.Kind != "result_reported" ||
				row.AttemptID != a.ID ||
				row.ExecutionID != a.ExecutionID ||
				row.Epoch != a.Epoch {
				continue
			}
			if row.CompleteDecision == "FENCED" || row.CompleteDecision == "REJECTED" {
				continue
			}
			if row.CompleteDecision == "UNKNOWN" && !committed[a.ID] && a.Result == nil {
				// The submission exists, but it need not have reached the server.
				// Its cgroup ordering is still checked; no accepted-result facts
				// can be inferred from the unknown transport outcome.
				continue
			}
			if row.Result == nil || row.StatusCode == nil {
				issue(
					1,
					"evidence_missing",
					a.ExecutionID,
					a.ID,
					"worker report requires raw ActionResult and numeric status",
				)
				continue
			}
			var result re.ActionResult
			if err := proto.Unmarshal(row.Result, &result); err != nil {
				issue(1, "invalid_result", a.ExecutionID, a.ID, err.Error())
				continue
			}
			if previous, ok := facts.results[a.ID]; ok && !proto.Equal(previous, &result) {
				issue(1, "conflicting_result", a.ExecutionID, a.ID, "attempt reported different result contents")
			}
			facts.results[a.ID] = &result
			facts.status[a.ID] = *row.StatusCode
			var trees []Blob
			for _, other := range external {
				trees = append(trees, other.Trees...)
				trees = append(trees, other.Blobs...)
			}
			digests, err := resultReferences(&result, trees)
			if err != nil {
				issue(1, "evidence_missing", a.ExecutionID, a.ID, err.Error())
				continue
			}
			facts.digests[a.ID] = digests
			if !containsDigests(digests, a.ResultDigests) || !containsDigests(a.ResultDigests, digests) {
				issue(
					1,
					"derived_digests_mismatch",
					a.ExecutionID,
					a.ID,
					"ledger output references differ from captured ActionResult/Tree",
				)
			}
			if int(result.ExitCode) != a.ExitCode {
				issue(
					4,
					"derived_exit_code_mismatch",
					a.ExecutionID,
					a.ID,
					"ledger exit code differs from captured ActionResult",
				)
			}
			if a.Result != nil {
				var ledgerResult re.ActionResult
				if err := proto.Unmarshal(a.Result, &ledgerResult); err != nil || !proto.Equal(&ledgerResult, &result) {
					issue(
						1,
						"ledger_result_mismatch",
						a.ExecutionID,
						a.ID,
						"ledger result differs from worker-reported result",
					)
				}
			}
		}
	}
	for _, row := range external {
		if row.Source != "ac" || row.Kind != "ac_write" {
			continue
		}
		var e *Execution
		for i := range l.Executions {
			if l.Executions[i].ID == row.ExecutionID {
				e = &l.Executions[i]
				break
			}
		}
		if e == nil {
			continue
		}
		if row.ActionDigest != e.ActionDigest {
			issue(
				1,
				"ac_key_mismatch",
				row.ExecutionID,
				row.AttemptID,
				"observed AC key differs from execution Action digest",
			)
		}
		if row.InstanceName != e.InstanceName {
			issue(
				1,
				"ac_instance_mismatch",
				row.ExecutionID,
				row.AttemptID,
				"captured AC instance differs from execution",
			)
		}
		if row.Result == nil {
			issue(1, "evidence_missing", row.ExecutionID, row.AttemptID, "AC write requires raw published ActionResult")
			continue
		}
		var published re.ActionResult
		if err := proto.Unmarshal(row.Result, &published); err != nil {
			issue(1, "invalid_ac_result", row.ExecutionID, row.AttemptID, err.Error())
			continue
		}
		winner := facts.results[row.AttemptID]
		if winner == nil {
			issue(1, "evidence_missing", row.ExecutionID, row.AttemptID, "raw winning worker result missing")
		} else if !proto.Equal(winner, &published) {
			issue(
				1,
				"ac_result_mismatch",
				row.ExecutionID,
				row.AttemptID,
				"published result differs from winning attempt",
			)
		}
		if action := facts.actions[row.ExecutionID]; action != nil {
			p := facts.platforms[row.ExecutionID]
			status, hasStatus := facts.status[row.AttemptID]
			if action.DoNotCache ||
				p["remex-hw"] == "true" ||
				p["remex-hw"] == "1" ||
				published.ExitCode != 0 ||
				(hasStatus && status != 0) {
				issue(
					4,
					"raw_uncacheable_published",
					row.ExecutionID,
					row.AttemptID,
					"raw protocol evidence forbids this AC write",
				)
			}
		}
	}
	return facts
}

func resultReferences(result *re.ActionResult, blobs []Blob) ([]Digest, error) {
	set := map[Digest]bool{}
	add := func(d *re.Digest) error {
		if d == nil {
			return fmt.Errorf("missing result digest")
		}
		x := modelDigest(d)
		if len(x.Hash) != 64 || x.SizeBytes < 0 {
			return fmt.Errorf("invalid result digest")
		}
		set[x] = true
		return nil
	}
	for _, f := range result.OutputFiles {
		if err := add(f.Digest); err != nil {
			return nil, err
		}
	}
	for _, d := range []*re.Digest{result.StdoutDigest, result.StderrDigest} {
		if d != nil {
			if err := add(d); err != nil {
				return nil, err
			}
		}
	}
	for _, output := range result.OutputDirectories {
		if err := add(output.TreeDigest); err != nil {
			return nil, err
		}
		key := modelDigest(output.TreeDigest)
		var raw []byte
		found := false
		for _, b := range blobs {
			if b.Digest == key && hashBytes(b.Data) == key {
				raw = b.Data
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("raw digest-verified output Tree %s is missing", key.Hash)
		}
		var tree re.Tree
		if err := proto.Unmarshal(raw, &tree); err != nil {
			return nil, err
		}
		if tree.Root == nil {
			return nil, fmt.Errorf("Tree has no root")
		}
		children := map[Digest]*re.Directory{}
		for _, child := range tree.Children {
			wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(child)
			if err != nil {
				return nil, err
			}
			children[hashBytes(wire)] = child
		}
		visiting := map[Digest]bool{}
		done := map[Digest]bool{}
		var walk func(*re.Directory) error
		walk = func(dir *re.Directory) error {
			for _, f := range dir.Files {
				if err := add(f.Digest); err != nil {
					return err
				}
			}
			for _, node := range dir.Directories {
				d := modelDigest(node.Digest)
				child, ok := children[d]
				if !ok {
					return fmt.Errorf("Tree child Directory %s missing", d.Hash)
				}
				if visiting[d] {
					return fmt.Errorf("Tree Directory cycle")
				}
				if !done[d] {
					visiting[d] = true
					if err := walk(child); err != nil {
						return err
					}
					delete(visiting, d)
					done[d] = true
				}
			}
			return nil
		}
		if err := walk(tree.Root); err != nil {
			return nil, err
		}
	}
	out := make([]Digest, 0, len(set))
	for d := range set {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hash < out[j].Hash })
	return out, nil
}
