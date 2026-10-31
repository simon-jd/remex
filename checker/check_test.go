package checker

import (
	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"
	"testing"
)

// These are synthetic fixtures for the checking algorithm, not fault-matrix evidence.
func goodFixture() (Ledger, []Evidence) {
	p := map[string]string{"pool": "cpu", "OSFamily": "Linux", "ISA": "x86-64", "cpu.min": "2"}
	actual := map[string]string{"pool": "cpu", "OSFamily": "Linux", "ISA": "x86-64", "cpu": "4"}
	d := []Digest{hashBytes([]byte("out"))}
	l := Ledger{SchemaVersion: 1, Executions: []Execution{{ID: "e", Platform: p, CurrentEpoch: 1, WinnerAttemptID: "a"}}, Attempts: []Attempt{{ID: "a", ExecutionID: "e", Epoch: 1, WorkerID: "w", WorkerPlatform: actual, AttemptDir: "/attempt/a", CgroupEmpty: true, Status: "OK", Published: true, ResultDigests: d}}, Events: []Event{{Sequence: 1, Kind: "execution_created", ExecutionID: "e"}, {Sequence: 3, Kind: "lease_granted", ExecutionID: "e", AttemptID: "a", Epoch: 1}, {Sequence: 7, Kind: "cas_verified", ExecutionID: "e", AttemptID: "a", Epoch: 1}, {Sequence: 8, Kind: "attempt_committed", ExecutionID: "e", AttemptID: "a", Epoch: 1}, {Sequence: 10, Kind: "ac_published", ExecutionID: "e", AttemptID: "a", Epoch: 1}}}
	rows := []Evidence{{Source: "worker", Kind: "lease_verified", Sequence: 2, ExecutionID: "e", AttemptID: "a", Epoch: 1, WorkerPlatform: actual}, {Source: "worker", Kind: "cgroup_empty", Sequence: 4, ExecutionID: "e", AttemptID: "a", Epoch: 1, AttemptDir: "/attempt/a", Empty: true}, {Source: "worker", Kind: "result_reported", Sequence: 5, ExecutionID: "e", AttemptID: "a", Epoch: 1, AttemptDir: "/attempt/a"}, {Source: "cas", Kind: "cas_verified", Sequence: 6, ExecutionID: "e", AttemptID: "a", Epoch: 1, Digests: d}, {Source: "ac", Kind: "ac_write", Sequence: 9, ExecutionID: "e", AttemptID: "a", Epoch: 1}}
	action := &re.Action{Platform: &re.Platform{}}
	for key, value := range p {
		action.Platform.Properties = append(action.Platform.Properties, &re.Platform_Property{Name: key, Value: value})
	}
	raw, _ := proto.Marshal(action)
	l.Executions[0].Action = raw
	l.Executions[0].ActionDigest = hashBytes(raw)
	rows[0].Action = raw
	rows[0].ActionDigest = hashBytes(raw)
	result, _ := proto.Marshal(&re.ActionResult{StdoutDigest: &re.Digest{Hash: d[0].Hash, SizeBytes: d[0].SizeBytes}})
	zero := int32(0)
	noOOM := int64(0)
	rows[1].OOMKill = &noOOM
	rows[2].Result = result
	rows[2].StatusCode = &zero
	rows[4].Result = result
	rows[4].ActionDigest = hashBytes(raw)
	l.Attempts[0].Result = result
	return l, rows
}
func TestIndependentEvidenceRequired(t *testing.T) {
	l, rows := goodFixture()
	if r := Check(l, rows, nil); !r.OK {
		t.Fatalf("%+v", r)
	}
	if r := Check(l, nil, nil); r.OK || len(r.MissingEvidence) == 0 {
		t.Fatalf("ledger self-report accepted: %+v", r)
	}
}
func TestInvariantMutations(t *testing.T) {
	tests := []struct {
		name      string
		invariant int
		mutate    func(*Ledger, *[]Evidence)
	}{
		{"cas-after-commit", 1, func(l *Ledger, e *[]Evidence) { (*e)[3].Sequence = 12 }},
		{"missing-blob", 1, func(l *Ledger, e *[]Evidence) { (*e)[3].Digests = nil }},
		{"stale-epoch", 2, func(l *Ledger, e *[]Evidence) { l.Executions[0].CurrentEpoch = 2 }},
		{"multiple-commit", 2, func(l *Ledger, e *[]Evidence) {
			l.Events = append(l.Events, Event{Sequence: 11, Kind: "attempt_committed", ExecutionID: "e", AttemptID: "a", Epoch: 1})
		}},
		{"platform-mismatch", 3, func(l *Ledger, e *[]Evidence) { l.Executions[0].Platform["image"] = "sha256:new" }},
		{"hardware-cached", 4, func(l *Ledger, e *[]Evidence) { l.Executions[0].HardwareTest = true }},
		{"failed-result-cached", 4, func(l *Ledger, e *[]Evidence) { l.Attempts[0].ExitCode = 1 }},
		{"cgroup-not-empty", 5, func(l *Ledger, e *[]Evidence) { (*e)[1].Empty = false }},
		{"cgroup-after-report", 5, func(l *Ledger, e *[]Evidence) { (*e)[1].Sequence = 6 }},
		{"directory-reused", 5, func(l *Ledger, e *[]Evidence) {
			l.Attempts = append(l.Attempts, Attempt{ID: "b", ExecutionID: "e", WorkerID: "w", AttemptDir: "/attempt/a"})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, rows := goodFixture()
			tt.mutate(&l, &rows)
			r := Check(l, rows, nil)
			if r.OK {
				t.Fatal("corrupt evidence accepted")
			}
			found := false
			for _, v := range append(r.Violations, r.MissingEvidence...) {
				if v.Invariant == tt.invariant {
					found = true
				}
			}
			if !found {
				t.Fatalf("wrong invariant: %+v", r)
			}
		})
	}
}
func TestRegressionInfraCannotConfirmCulprit(t *testing.T) {
	l, rows := goodFixture()
	report := RegressionReport{Conclusion: "CULPRIT", Culprit: "bad", K: 2, Runs: 0, Budget: 100, Signature: "bug"}
	r := Check(l, rows, []RegressionReport{report})
	if r.OK {
		t.Fatal("unconfirmed culprit accepted")
	}
	if len(r.Violations) != 1 || r.Violations[0].Invariant != 6 {
		t.Fatal(r)
	}
}
func TestNoDataIsNotProof(t *testing.T) {
	if r := Check(Ledger{SchemaVersion: 1}, nil, nil); r.OK {
		t.Fatal("empty evidence accepted")
	}
}

func TestRawProtocolCannotBeReplacedByLedgerClaims(t *testing.T) {
	t.Run("missing-raw-action-result", func(t *testing.T) {
		l, rows := goodFixture()
		for i := range rows {
			rows[i].Action = nil
			rows[i].Result = nil
		}
		l.Attempts[0].ResultDigests = nil
		rows[3].Digests = nil
		if r := Check(l, rows, nil); r.OK || len(r.MissingEvidence) == 0 {
			t.Fatalf("missing raw evidence accepted: %+v", r)
		}
	})
	t.Run("ledger-omits-blob", func(t *testing.T) {
		l, rows := goodFixture()
		l.Attempts[0].ResultDigests = nil
		rows[3].Digests = nil
		if r := Check(l, rows, nil); r.OK {
			t.Fatal("omitted real output accepted")
		}
	})
	t.Run("raw-do-not-cache", func(t *testing.T) {
		l, rows := goodFixture()
		var a re.Action
		proto.Unmarshal(rows[0].Action, &a)
		a.DoNotCache = true
		raw, _ := proto.Marshal(&a)
		d := hashBytes(raw)
		l.Executions[0].Action = raw
		l.Executions[0].ActionDigest = d
		rows[0].Action = raw
		rows[0].ActionDigest = d
		rows[4].ActionDigest = d
		if r := Check(l, rows, nil); r.OK {
			t.Fatal("falsified cache eligibility accepted")
		}
	})
	t.Run("wrong-ac-content", func(t *testing.T) {
		l, rows := goodFixture()
		rows[4].Result = []byte{}
		if r := Check(l, rows, nil); r.OK {
			t.Fatal("wrong AC result accepted")
		}
	})
	t.Run("wrong-ac-key", func(t *testing.T) {
		l, rows := goodFixture()
		rows[4].ActionDigest = hashBytes([]byte("different action"))
		if r := Check(l, rows, nil); r.OK {
			t.Fatal("wrong AC key accepted")
		}
	})
	t.Run("legitimate-empty-output", func(t *testing.T) {
		l, rows := goodFixture()
		l.Attempts[0].Result = []byte{}
		l.Attempts[0].ResultDigests = []Digest{}
		rows[2].Result = []byte{}
		rows[3].Digests = []Digest{}
		rows[4].Result = []byte{}
		if r := Check(l, rows, nil); !r.OK {
			t.Fatalf("legitimate empty result rejected: %+v", r)
		}
	})
	t.Run("tree-hidden-output", func(t *testing.T) {
		l, rows := goodFixture()
		file := hashBytes([]byte("hidden"))
		treeRaw, _ := proto.Marshal(&re.Tree{Root: &re.Directory{Files: []*re.FileNode{{Name: "hidden", Digest: &re.Digest{Hash: file.Hash, SizeBytes: file.SizeBytes}}}}})
		td := hashBytes(treeRaw)
		result, _ := proto.Marshal(&re.ActionResult{OutputDirectories: []*re.OutputDirectory{{Path: "out", TreeDigest: &re.Digest{Hash: td.Hash, SizeBytes: td.SizeBytes}}}})
		l.Attempts[0].Result = result
		l.Attempts[0].ResultDigests = []Digest{td}
		rows[2].Result = result
		rows[2].Trees = []Blob{{Digest: td, Data: treeRaw}}
		rows[3].Digests = []Digest{td}
		rows[4].Result = result
		if r := Check(l, rows, nil); r.OK {
			t.Fatal("hidden Tree output accepted")
		}
		l.Attempts[0].ResultDigests = append(l.Attempts[0].ResultDigests, file)
		rows[3].Digests = append(rows[3].Digests, file)
		if r := Check(l, rows, nil); !r.OK {
			t.Fatalf("full Tree proof rejected: %+v", r)
		}
	})
}
