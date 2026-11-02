// Package regress implements evidence-driven regression triage without depending on
// the scheduler. JSON records are intentionally portable between offline tools.
package regress

import (
	"bufio"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

type Category string

const (
	Pass           Category = "PASS"
	TestFailure    Category = "TEST_FAILURE"
	Infrastructure Category = "INFRASTRUCTURE"
	Timeout        Category = "TIMEOUT"
	Cancelled      Category = "CANCELLED"
	Unknown        Category = "UNKNOWN"
)

type Observation struct {
	Category  Category `json:"category"`
	Signature string   `json:"signature,omitempty"`
	Detail    string   `json:"detail,omitempty"`
	Evidence  []string `json:"evidence,omitempty"`
}

type AttemptEvidence struct {
	ID             string `json:"id"`
	ExecutionID    string `json:"execution_id"`
	Status         string `json:"status"`
	ExitCode       int    `json:"exit_code"`
	OOMKill        int64  `json:"oom_kill"`
	Phase          string `json:"phase,omitempty"`
	ExecutionState string `json:"execution_state,omitempty"`
}

type BEPResult struct {
	Label         string `json:"label"`
	Status        string `json:"status"`
	Cached        bool   `json:"cached"`
	Run           int    `json:"run"`
	Attempt       int    `json:"attempt"`
	StatusDetails string `json:"status_details,omitempty"`
	LogURI        string `json:"log_uri,omitempty"`
	FailureText   string `json:"failure_text,omitempty"`
}

// ReadBEP reads Bazel's newline-delimited build event JSON. Test summaries are
// deliberately ignored: a flaky summary would erase the individual attempts.
func ReadBEP(r io.Reader) ([]BEPResult, error) {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 64*1024), 32*1024*1024)
	var out []BEPResult
	for line := 1; s.Scan(); line++ {
		var event struct {
			ID struct {
				TestResult *struct {
					Label   string `json:"label"`
					Run     int    `json:"run"`
					Attempt int    `json:"attempt"`
				} `json:"testResult"`
			} `json:"id"`
			TestResult *struct {
				Status           string `json:"status"`
				StatusDetails    string `json:"statusDetails"`
				TestActionOutput []struct {
					Name     string `json:"name"`
					URI      string `json:"uri"`
					Contents []byte `json:"contents"`
				} `json:"testActionOutput"`
				CachedLocally bool `json:"cachedLocally"`
				ExecutionInfo struct {
					CachedRemotely bool `json:"cachedRemotely"`
				} `json:"executionInfo"`
			} `json:"testResult"`
		}
		if err := json.Unmarshal(s.Bytes(), &event); err != nil {
			return nil, fmt.Errorf("BEP line %d: %w", line, err)
		}
		if event.ID.TestResult != nil && event.TestResult != nil {
			x, y := event.ID.TestResult, event.TestResult
			result := BEPResult{
				Label:         x.Label,
				Status:        y.Status,
				Cached:        y.CachedLocally || y.ExecutionInfo.CachedRemotely,
				Run:           x.Run,
				Attempt:       x.Attempt,
				StatusDetails: y.StatusDetails,
			}
			for _, file := range y.TestActionOutput {
				if file.Name == "test.log" {
					result.LogURI = file.URI
					result.FailureText = string(file.Contents)
				}
			}
			out = append(out, result)
		}
	}
	return out, s.Err()
}

// Classify gives infrastructure/cancellation evidence priority over an apparent
// test failure. A bare nonzero exit code is never sufficient test evidence.
func Classify(results []BEPResult, attempts []AttemptEvidence, signature string) Observation {
	// Inspect all attempts before terminal categories: an earlier timeout must
	// not hide independent OOM or transport evidence later in the export.
	for _, a := range attempts {
		a.Status = normalizeStatus(a.Status)
		if a.OOMKill > 0 {
			return Observation{
				Category: Infrastructure,
				Detail:   "cgroup OOM kill",
				Evidence: []string{"attempt:" + a.ID},
			}
		}
		if a.ExecutionState == "Cancelled" && (a.Status == "" || a.Status == "UNKNOWN") {
			continue
		}
		if a.Status != "" && a.Status != "OK" && a.Status != "CANCELLED" && a.Status != "DEADLINE_EXCEEDED" {
			return Observation{
				Category: Infrastructure,
				Detail:   "ledger status: " + a.Status,
				Evidence: []string{"attempt:" + a.ID},
			}
		}
	}
	var deadlineAttempt string
	for _, a := range attempts {
		a.Status = normalizeStatus(a.Status)
		if a.ExecutionState == "Cancelled" {
			return Observation{
				Category: Cancelled,
				Detail:   "ledger execution cancellation",
				Evidence: []string{"execution:" + a.ExecutionID},
			}
		}
		if a.Status == "CANCELLED" {
			return Observation{
				Category: Cancelled,
				Detail:   "ledger cancellation",
				Evidence: []string{"attempt:" + a.ID},
			}
		}
		if a.Status == "DEADLINE_EXCEEDED" {
			deadlineAttempt = a.ID
		}
	}
	answer := Observation{Category: Unknown, Detail: "no fresh terminal test evidence"}
	var failureSignatures []string
	missingSignature := false
	for _, r := range results {
		if r.Cached {
			return Observation{
				Category: Unknown,
				Detail:   "cached result cannot establish a fresh regression observation",
				Evidence: []string{"BEP:" + r.Label},
			}
		}
		switch r.Status {
		case "REMOTE_FAILURE", "FAILED_TO_BUILD", "INCOMPLETE", "TOOL_HALTED_BEFORE_TESTING":
			return Observation{Category: Infrastructure, Detail: r.Status, Evidence: []string{"BEP:" + r.Label}}
		case "HALTED":
			return Observation{Category: Cancelled, Detail: r.Status, Evidence: []string{"BEP:" + r.Label}}
		case "FAILED", "TIMEOUT":
			c := TestFailure
			if r.Status == "TIMEOUT" {
				c = Timeout
			}
			sig := signature
			if sig == "" {
				// A label and FAILED status alone do not distinguish regressions.
				text := strings.TrimSpace(r.FailureText)
				if text == "" {
					text = strings.TrimSpace(r.StatusDetails)
				}
				for _, line := range strings.Split(text, "\n") {
					if marker, ok := strings.CutPrefix(
						strings.TrimSpace(line),
						"REMEX_SIGNATURE:",
					); ok && strings.TrimSpace(marker) != "" {
						if sig != "" && sig != strings.TrimSpace(marker) {
							sig = ""
							break
						}
						sig = strings.TrimSpace(marker)
					}
				}
				if sig == "" && text != "" {
					sig = fmt.Sprintf("%s:%s:%x", r.Label, r.Status, sha256.Sum256([]byte(text)))
				}
			}
			if sig == "" {
				missingSignature = true
			} else {
				failureSignatures = append(failureSignatures, sig)
			}
			answer = Observation{Category: c, Signature: sig, Evidence: []string{"BEP:" + r.Label}}
		case "PASSED":
			if answer.Category == Unknown {
				answer = Observation{Category: Pass, Evidence: []string{"BEP:" + r.Label}}
			}
		}
	}
	if len(failureSignatures) > 0 && !missingSignature {
		sort.Strings(failureSignatures)
		unique := failureSignatures[:0]
		for _, sig := range failureSignatures {
			if len(unique) == 0 || unique[len(unique)-1] != sig {
				unique = append(unique, sig)
			}
		}
		answer.Signature = strings.Join(unique, "|")
	} else if missingSignature {
		answer.Signature = ""
	}
	if deadlineAttempt != "" {
		// Preserve fresh test-log signatures for timeout regression probes.
		// Infrastructure and cancellation evidence above still take priority.
		answer.Category = Timeout
		answer.Detail = "ledger command deadline"
		answer.Evidence = append(answer.Evidence, "attempt:"+deadlineAttempt)
		if signature != "" {
			answer.Signature = signature
		}
	}
	return answer
}

// Normalize both google.rpc enum spelling and Go grpc/codes.String exports.
func normalizeStatus(s string) string {
	switch strings.ToUpper(strings.ReplaceAll(s, "_", "")) {
	case "DEADLINEEXCEEDED":
		return "DEADLINE_EXCEEDED"
	case "CANCELED", "CANCELLED":
		return "CANCELLED"
	case "OK":
		return "OK"
	default:
		return s
	}
}
