package regress

import (
	"encoding/json"
	"fmt"
)

// ParseLedgerEvidence preserves execution-level cancellation even if a worker
// has not reported a terminal attempt yet. A nonempty invocation scopes all rows.
func ParseLedgerEvidence(raw []byte, invocation string) ([]AttemptEvidence, error) {
	var ledger struct {
		Executions []struct {
			ID            string   `json:"id"`
			InvocationIDs []string `json:"invocation_ids"`
			State         string   `json:"state"`
			StatusCode    int32    `json:"status_code"`
		} `json:"executions"`
		Attempts []AttemptEvidence `json:"attempts"`
	}
	if err := json.Unmarshal(raw, &ledger); err != nil {
		return nil, err
	}
	states := map[string]string{}
	for _, e := range ledger.Executions {
		matches := invocation == ""
		for _, id := range e.InvocationIDs {
			if id == invocation {
				matches = true
			}
		}
		if matches {
			state := e.State
			if e.StatusCode == 1 {
				state = "Cancelled"
			}
			states[e.ID] = state
		}
	}
	if invocation != "" && len(states) == 0 {
		return nil, fmt.Errorf("no executions correlated with invocation %s", invocation)
	}
	out := []AttemptEvidence{}
	seen := map[string]bool{}
	for _, attempt := range ledger.Attempts {
		state, match := states[attempt.ExecutionID]
		if invocation == "" || match {
			attempt.ExecutionState = state
			out = append(out, attempt)
			seen[attempt.ExecutionID] = true
		}
	}
	for id, state := range states {
		if state == "Cancelled" && !seen[id] {
			out = append(out, AttemptEvidence{ExecutionID: id, ExecutionState: state})
		}
	}
	return out, nil
}
