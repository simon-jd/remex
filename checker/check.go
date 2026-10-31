package checker

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Check is fail-closed: missing independent evidence is distinct from a proven
// violation, but either prevents an OK report. An empty export proves nothing.
func Check(l Ledger, external []Evidence, regression []RegressionReport) Report {
	r := Report{OK: true, Checked: map[int]int{}, Violations: []Violation{}, MissingEvidence: []Violation{}}
	issue := func(invariant int, code string, e, a, detail string) {
		r.OK = false
		v := Violation{Invariant: invariant, Code: code, ExecutionID: e, AttemptID: a, Detail: detail}
		if code == "evidence_missing" {
			r.MissingEvidence = append(r.MissingEvidence, v)
		} else {
			r.Violations = append(r.Violations, v)
		}
	}
	if l.SchemaVersion != 1 {
		issue(1, "schema", "", "", "unsupported ledger schema")
	}
	if len(l.Executions) == 0 && len(regression) == 0 {
		issue(1, "evidence_missing", "", "", "no executions or regression reports")
	}
	facts := inspectProtocol(l, external, issue)
	for _, row := range external {
		if row.Kind == "evidence_gap" || row.Kind == "ac_write_attempt" {
			issue(1, "evidence_missing", row.ExecutionID, row.AttemptID, row.Detail)
		}
	}
	executions := map[string]Execution{}
	attempts := map[string]Attempt{}
	for _, e := range l.Executions {
		if _, ok := executions[e.ID]; ok || e.ID == "" {
			issue(2, "duplicate_execution", e.ID, "", "empty or duplicated execution ID")
		}
		executions[e.ID] = e
	}
	for _, a := range l.Attempts {
		if _, ok := attempts[a.ID]; ok || a.ID == "" {
			issue(2, "duplicate_attempt", a.ExecutionID, a.ID, "empty or duplicated attempt ID")
		}
		attempts[a.ID] = a
		if _, ok := executions[a.ExecutionID]; !ok {
			issue(2, "orphan_attempt", a.ExecutionID, a.ID, "execution missing")
		}
	}
	commits := map[string]Event{}
	leases := map[string]Event{}
	cas := map[string]Event{}
	expired := map[string]int64{}
	published := map[string]bool{}
	commitCounts := map[string]int{}
	previous := int64(0)
	for _, event := range l.Events {
		if event.Sequence <= previous {
			issue(2, "event_order", event.ExecutionID, event.AttemptID, "sequences must be positive, unique and increasing")
		}
		previous = event.Sequence
		if event.AttemptID != "" {
			if a, ok := attempts[event.AttemptID]; !ok || a.ExecutionID != event.ExecutionID || a.Epoch != event.Epoch {
				issue(2, "event_identity", event.ExecutionID, event.AttemptID, "event identity does not match attempt")
			}
		}
		switch event.Kind {
		case "lease_granted":
			leases[event.AttemptID] = event
		case "lease_expired":
			expired[event.AttemptID] = event.Sequence
		case "cas_verified":
			cas[event.AttemptID] = event
		case "attempt_committed":
			r.Checked[2]++
			commitCounts[event.ExecutionID]++
			if commitCounts[event.ExecutionID] > 1 {
				issue(2, "multiple_commits", event.ExecutionID, event.AttemptID, "execution committed more than once")
			}
			e := executions[event.ExecutionID]
			if e.CurrentEpoch != event.Epoch || e.WinnerAttemptID != event.AttemptID {
				issue(2, "stale_commit", event.ExecutionID, event.AttemptID, "commit is not current fenced winner")
			}
			if seq := expired[event.AttemptID]; seq > 0 && seq < event.Sequence {
				issue(2, "expired_commit", event.ExecutionID, event.AttemptID, "expired lease committed")
			}
			if verify, ok := cas[event.AttemptID]; !ok || verify.Sequence >= event.Sequence {
				issue(1, "cas_order", event.ExecutionID, event.AttemptID, "commit lacks preceding CAS verification")
			}
			commits[event.AttemptID] = event
		case "ac_published":
			published[event.AttemptID] = true
			c, ok := commits[event.AttemptID]
			if !ok || c.Sequence >= event.Sequence {
				issue(1, "publish_order", event.ExecutionID, event.AttemptID, "AC publish lacks preceding commit")
			}
		case "stale_complete_rejected":
			r.Checked[2]++
		}
	}
	for _, e := range l.Executions {
		if e.WinnerAttemptID != "" {
			if _, ok := commits[e.WinnerAttemptID]; !ok {
				issue(2, "evidence_missing", e.ID, e.WinnerAttemptID, "winner has no commit event")
			}
		}
	}
	find := func(source, kind, attempt string) []Evidence {
		var rows []Evidence
		for _, row := range external {
			if row.Source == source && row.Kind == kind && row.AttemptID == attempt {
				rows = append(rows, row)
			}
		}
		return rows
	}
	for _, row := range external {
		if row.Source != "ac" || row.Kind != "ac_write" {
			continue
		}
		r.Checked[1]++
		r.Checked[4]++
		a, ok := attempts[row.AttemptID]
		e := executions[row.ExecutionID]
		c, committed := commits[row.AttemptID]
		if !ok || !committed || a.ExecutionID != row.ExecutionID || row.Epoch != a.Epoch || row.Epoch != e.CurrentEpoch || e.WinnerAttemptID != a.ID {
			issue(1, "unfenced_ac_write", row.ExecutionID, row.AttemptID, "external AC write is not after a current-epoch commit")
		}
		if !acAfterCommit(row, c) {
			issue(1, "evidence_missing", row.ExecutionID, row.AttemptID, "AC request lacks independent preceding durable commit observation")
		}
		if row.OrderDomain != "" {
			verifiedBefore := false
			for _, casRow := range external {
				if casRow.Source == "cas" && casRow.Kind == "cas_verified" && casRow.AttemptID == row.AttemptID && casRow.OrderDomain == row.OrderDomain && casRow.Sequence < row.Sequence && casBeforeCommit(casRow, c) {
					verifiedBefore = true
				}
			}
			if !verifiedBefore {
				issue(1, "evidence_missing", row.ExecutionID, row.AttemptID, "capture does not establish CAS verification before AC publication")
			}
		}
		if ok {
			for _, kernel := range external {
				if kernel.Source == "worker" && kernel.Kind == "cgroup_empty" && kernel.AttemptID == a.ID && kernel.ExecutionID == e.ID && kernel.Epoch == a.Epoch && kernel.OOMKill != nil && *kernel.OOMKill > 0 {
					issue(4, "kernel_oom_published", e.ID, a.ID, "independent memory.events OOM evidence forbids AC publication")
				}
			}
			if e.DoNotCache || e.HardwareTest || a.ExitCode != 0 || a.Status != "OK" || a.OOMKill > 0 {
				issue(4, "uncacheable_published", row.ExecutionID, row.AttemptID, "uncacheable or unsuccessful result written to AC")
			}
		}
	}
	usedDirs := map[string]string{}
	for _, a := range l.Attempts {
		e := executions[a.ExecutionID]
		if action := facts.actions[e.ID]; action != nil {
			e.Platform = facts.platforms[e.ID]
			e.DoNotCache = action.DoNotCache
			e.HardwareTest = e.Platform["remex-hw"] == "true" || e.Platform["remex-hw"] == "1"
		}
		if lease, ok := leases[a.ID]; ok {
			r.Checked[3]++
			rows := find("worker", "lease_verified", a.ID)
			valid := false
			for _, row := range rows {
				leaseOrder := row.Sequence <= lease.Sequence
				if row.OrderDomain != "" {
					leaseOrder = len(row.RawRecordIDs) >= 2 && row.Action != nil
				}
				if row.Epoch == a.Epoch && row.ExecutionID == a.ExecutionID && leaseOrder && row.Sequence > 0 && platformMatches(e.Platform, row.WorkerPlatform) && (e.Platform["pool"] != "gpu" || row.DeviceVerified) {
					valid = true
				}
			}
			if !valid {
				issue(3, "evidence_missing", a.ExecutionID, a.ID, "no matching independent platform verification at lease")
			}
			if !platformMatches(e.Platform, a.WorkerPlatform) || (e.Platform["pool"] == "gpu" && !a.DeviceVerified) {
				issue(3, "platform_mismatch", a.ExecutionID, a.ID, "ledger lease properties do not satisfy action platform")
			}
		}
		if a.AttemptDir != "" {
			key := a.WorkerID + "\x00" + a.AttemptDir
			if old, ok := usedDirs[key]; ok && old != a.ID {
				issue(5, "directory_reused", a.ExecutionID, a.ID, "attempt directory reused by "+old)
			}
			usedDirs[key] = a.ID
		}
		if c, ok := commits[a.ID]; ok {
			r.Checked[1]++
			if _, leased := leases[a.ID]; !leased {
				issue(3, "evidence_missing", a.ExecutionID, a.ID, "committed attempt has no lease event")
			}
			valid := false
			required, hasRawResult := facts.digests[a.ID]
			if !hasRawResult {
				issue(1, "evidence_missing", a.ExecutionID, a.ID, "complete raw result and output Tree evidence required for committed attempt")
			}
			for _, row := range find("cas", "cas_verified", a.ID) {
				if row.InstanceName != e.InstanceName {
					issue(1, "cas_instance_mismatch", a.ExecutionID, a.ID, "captured CAS instance differs from execution")
					continue
				}
				if hasRawResult && row.ExecutionID == a.ExecutionID && row.Epoch == a.Epoch && casBeforeCommit(row, c) && containsDigests(row.Digests, required) {
					valid = true
				}
			}
			if !valid {
				issue(1, "evidence_missing", a.ExecutionID, a.ID, "external CAS verification of every result digest before commit missing")
			}
		}
		if a.Published || published[a.ID] {
			if len(find("ac", "ac_write", a.ID)) == 0 {
				issue(1, "evidence_missing", a.ExecutionID, a.ID, "published result has no external AC write")
			}
			if e.DoNotCache || e.HardwareTest || a.ExitCode != 0 || a.Status != "OK" || a.OOMKill > 0 {
				issue(4, "uncacheable_published", a.ExecutionID, a.ID, "ledger published uncacheable result")
			}
		}
		reports := find("worker", "result_reported", a.ID)
		if _, committed := commits[a.ID]; committed && len(reports) == 0 {
			issue(5, "evidence_missing", a.ExecutionID, a.ID, "committed attempt has no independent result report")
		}
		for _, reported := range reports {
			r.Checked[5]++
			rejected := reported.CompleteDecision == "FENCED" || reported.CompleteDecision == "REJECTED"
			_, committed := commits[a.ID]
			unconfirmed := reported.CompleteDecision == "UNKNOWN" && !committed && a.Result == nil
			if reported.CompleteDecision == "FENCED" {
				if _, ok := commits[a.ID]; ok && reported.Epoch == a.Epoch {
					if reported.LedgerSnapshot == nil {
						issue(2, "evidence_missing", a.ExecutionID, a.ID, "fenced/committed overlap requires durable snapshot to distinguish a post-commit duplicate")
					} else {
						alreadyCommitted := false
						for _, event := range reported.LedgerSnapshot.Events {
							if event.Kind == "attempt_committed" && event.AttemptID == a.ID && event.Epoch == a.Epoch {
								alreadyCommitted = true
							}
						}
						if !alreadyCommitted {
							issue(2, "fenced_result_committed", a.ExecutionID, a.ID, "attempt committed after a same-epoch fenced response")
						}
					}
				}
			}
			valid := false
			for _, empty := range find("worker", "cgroup_empty", a.ID) {
				if empty.OrderDomain != "" && empty.OOMKill == nil {
					issue(5, "evidence_missing", a.ExecutionID, a.ID, "independent kernel OOM counter missing")
					continue
				}
				if !rejected && !unconfirmed && empty.OOMKill != nil && *empty.OOMKill != a.OOMKill {
					issue(4, "derived_oom_mismatch", a.ExecutionID, a.ID, "ledger OOM count differs from independent memory.events")
				}
				if empty.Empty && empty.Epoch == reported.Epoch && (rejected || reported.Epoch == a.Epoch) && empty.ExecutionID == a.ExecutionID && reported.ExecutionID == a.ExecutionID && empty.AttemptDir != "" && empty.AttemptDir == reported.AttemptDir && (rejected || unconfirmed || empty.AttemptDir == a.AttemptDir) && empty.OrderDomain == reported.OrderDomain && empty.Sequence > 0 && empty.Sequence < reported.Sequence {
					valid = true
				}
			}
			if !valid {
				issue(5, "evidence_missing", a.ExecutionID, a.ID, "no independent empty cgroup evidence before result report")
			}
		}
	}
	for _, report := range regression {
		r.Checked[6]++
		if report.Conclusion != "CULPRIT" && report.Conclusion != "RANGE" && report.Conclusion != "INCONCLUSIVE" {
			issue(6, "invalid_conclusion", "", "", report.Conclusion)
		}
		if report.Runs > report.Budget || report.Budget < 1 {
			issue(6, "budget", "", "", "run count exceeds budget or invalid budget")
		}
		actualRuns := 0
		parentOK, childOK := false, false
		parent := ""
		seenRevisions := map[string]bool{}
		for i, rev := range report.Revisions {
			if rev == "" || seenRevisions[rev] {
				issue(6, "invalid_history", "", "", "history contains empty or duplicate revisions")
			}
			seenRevisions[rev] = true
			if rev == report.Culprit && i > 0 {
				parent = report.Revisions[i-1]
			}
		}
		for _, p := range report.Probes {
			actualRuns += len(p.Observations)
			passes, bad := 0, 0
			otherFailure := false
			for _, o := range p.Observations {
				if o.Category == "PASS" {
					passes++
				}
				if (o.Category == "TEST_FAILURE" || o.Category == "TIMEOUT") && o.Signature != "" && o.Signature == report.Signature {
					bad++
				}
				if (o.Category == "TEST_FAILURE" || o.Category == "TIMEOUT") && (o.Signature == "" || o.Signature != report.Signature) {
					otherFailure = true
				}
			}
			if p.Decision == "BAD" && bad == 0 {
				issue(6, "infra_as_evidence", "", "", "BAD probe has no matching test failure")
			}
			if p.Decision == "GOOD" && (bad > 0 || otherFailure) {
				issue(6, "contradictory_probe", "", "", "GOOD probe includes matching failure")
			}
			if p.Purpose == "confirm-parent" && p.Decision == "GOOD" && passes >= 2*report.K && bad == 0 && !otherFailure && parent != "" && p.Revision == parent {
				parentOK = true
			}
			if p.Purpose == "confirm-culprit" && p.Decision == "BAD" && passes+bad >= 2*report.K && bad > 0 && !otherFailure && p.Revision == report.Culprit {
				childOK = true
			}
		}
		if actualRuns != report.Runs {
			issue(6, "run_count", "", "", "probe observations do not match run count")
		}
		if report.Conclusion == "CULPRIT" && (report.K < 1 || report.Culprit == "" || !parentOK || !childOK) {
			issue(6, "confirmation_missing", "", "", "culprit needs fresh 2k parent and culprit confirmation")
		}
		if report.Conclusion == "RANGE" && len(report.Range) < 2 {
			issue(6, "invalid_range", "", "", "range requires at least two candidate revisions")
		}
	}
	return r
}

func acAfterCommit(row Evidence, commit Event) bool {
	if row.OrderDomain == "" {
		return row.Sequence > commit.Sequence
	}
	if row.LedgerSnapshot == nil || len(row.RawRecordIDs) < 3 {
		return false
	}
	for _, e := range row.LedgerSnapshot.Executions {
		if e.ID == row.ExecutionID && e.WinnerAttemptID == row.AttemptID && e.CurrentEpoch == row.Epoch {
			for _, event := range row.LedgerSnapshot.Events {
				if event.Kind == "attempt_committed" && event.AttemptID == row.AttemptID && event.Sequence == commit.Sequence && event.Epoch == row.Epoch {
					return true
				}
			}
		}
	}
	return false
}
func casBeforeCommit(row Evidence, commit Event) bool {
	if row.OrderDomain == "" {
		return row.Sequence > 0 && row.Sequence < commit.Sequence
	}
	if row.LedgerSnapshot == nil || len(row.RawRecordIDs) < 3 {
		return false
	}
	latest := int64(0)
	for _, event := range row.LedgerSnapshot.Events {
		if event.Sequence > latest {
			latest = event.Sequence
		}
		if event.Kind == "attempt_committed" && event.AttemptID == row.AttemptID {
			return false
		}
	}
	if latest >= commit.Sequence {
		return false
	}
	for _, e := range row.LedgerSnapshot.Executions {
		if e.ID == row.ExecutionID && e.WinnerAttemptID == "" && e.CurrentEpoch == row.Epoch {
			return true
		}
	}
	return false
}

func containsDigests(actual, required []Digest) bool {
	set := map[Digest]bool{}
	for _, d := range actual {
		set[d] = true
	}
	for _, d := range required {
		if !set[d] {
			return false
		}
	}
	return true
}
func platformMatches(required, actual map[string]string) bool {
	for key, want := range required {
		if key == "remex-hw" {
			continue
		}
		if key == "cpu.min" || key == "mem_gb.min" {
			got := actual[strings.TrimSuffix(key, ".min")]
			if got == "" {
				got = actual[key]
			}
			w, e1 := strconv.ParseFloat(want, 64)
			g, e2 := strconv.ParseFloat(got, 64)
			if e1 != nil || e2 != nil || w <= 0 || math.IsNaN(w) || math.IsNaN(g) || math.IsInf(w, 0) || math.IsInf(g, 0) || g < w {
				return false
			}
		} else if actual[key] != want {
			return false
		}
	}
	return true
}

func (r Report) Error() error {
	if r.OK {
		return nil
	}
	return fmt.Errorf("%d violations, %d missing evidence records", len(r.Violations), len(r.MissingEvidence))
}
