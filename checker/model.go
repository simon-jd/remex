// Package checker verifies exported evidence independently of scheduler code.
package checker

type Digest struct {
	Hash      string `json:"hash"`
	SizeBytes int64  `json:"size_bytes"`
}
type Execution struct {
	ID              string            `json:"id"`
	InstanceName    string            `json:"instance_name"`
	ActionDigest    Digest            `json:"action_digest"`
	Action          []byte            `json:"action"`
	Platform        map[string]string `json:"platform"`
	DoNotCache      bool              `json:"do_not_cache"`
	HardwareTest    bool              `json:"hardware_test"`
	State           string            `json:"state"`
	CurrentEpoch    int64             `json:"current_epoch"`
	WinnerAttemptID string            `json:"winner_attempt_id"`
}
type Attempt struct {
	ID             string            `json:"id"`
	ExecutionID    string            `json:"execution_id"`
	Epoch          int64             `json:"epoch"`
	WorkerID       string            `json:"worker_id"`
	WorkerPlatform map[string]string `json:"worker_platform"`
	DeviceVerified bool              `json:"device_verified"`
	AttemptDir     string            `json:"attempt_dir"`
	CgroupEmpty    bool              `json:"cgroup_empty"`
	ExitCode       int               `json:"exit_code"`
	Status         string            `json:"status"`
	OOMKill        int64             `json:"oom_kill"`
	Published      bool              `json:"published"`
	ResultDigests  []Digest          `json:"result_digests"`
	Result         []byte            `json:"result"`
}
type Event struct {
	Sequence    int64          `json:"sequence"`
	Time        string         `json:"time"`
	Kind        string         `json:"kind"`
	ExecutionID string         `json:"execution_id"`
	AttemptID   string         `json:"attempt_id"`
	Epoch       int64          `json:"epoch"`
	Details     map[string]any `json:"details"`
}
type Ledger struct {
	SchemaVersion int         `json:"schema_version"`
	Executions    []Execution `json:"executions"`
	Attempts      []Attempt   `json:"attempts"`
	Events        []Event     `json:"events"`
}

// Evidence is a normalized external log row. Sources must preserve ordering and
// carry a scheduler sequence correlation ID; local clock timestamps alone do not
// establish happens-before across hosts. Never synthesize these from ledger rows.
type Evidence struct {
	Source           string            `json:"source"` // cas, ac, worker or bazel
	Kind             string            `json:"kind"`
	Sequence         int64             `json:"sequence"`
	ExecutionID      string            `json:"execution_id"`
	InstanceName     string            `json:"instance_name"`
	Detail           string            `json:"detail,omitempty"`
	AttemptID        string            `json:"attempt_id"`
	Epoch            int64             `json:"epoch"`
	Digests          []Digest          `json:"digests"`
	WorkerPlatform   map[string]string `json:"worker_platform"`
	DeviceVerified   bool              `json:"device_verified"`
	AttemptDir       string            `json:"attempt_dir"`
	Empty            bool              `json:"empty"`
	OOMKill          *int64            `json:"oom_kill,omitempty"`
	ActionDigest     Digest            `json:"action_digest"`
	Action           []byte            `json:"action"`
	Command          []byte            `json:"command"`
	Result           []byte            `json:"result"`
	StatusCode       *int32            `json:"status_code"`
	CompleteDecision string            `json:"complete_decision,omitempty"`
	Trees            []Blob            `json:"trees"`
	Blobs            []Blob            `json:"blobs,omitempty"`
	// A recorder's native sequence is not a ledger sequence. Anchors retain
	// their distinct meaning instead of fabricating a merged event order.
	OrderDomain    string  `json:"order_domain,omitempty"`
	LedgerAnchor   int64   `json:"ledger_anchor,omitempty"`
	RawRecordIDs   []int64 `json:"raw_record_ids,omitempty"`
	LedgerSnapshot *Ledger `json:"ledger_snapshot,omitempty"`
}
type Blob struct {
	Digest Digest `json:"digest"`
	Data   []byte `json:"data"`
}
type RegressionReport struct {
	Revisions  []string `json:"revisions"`
	Conclusion string   `json:"conclusion"`
	Culprit    string   `json:"culprit"`
	Range      []string `json:"range"`
	Signature  string   `json:"signature"`
	K          int      `json:"k"`
	Runs       int      `json:"runs"`
	Budget     int      `json:"budget"`
	Probes     []struct {
		Revision     string `json:"revision"`
		Purpose      string `json:"purpose"`
		Decision     string `json:"decision"`
		Observations []struct {
			Category  string `json:"category"`
			Signature string `json:"signature"`
		} `json:"observations"`
	} `json:"probes"`
}
type Violation struct {
	Invariant   int    `json:"invariant"`
	Code        string `json:"code"`
	ExecutionID string `json:"execution_id,omitempty"`
	AttemptID   string `json:"attempt_id,omitempty"`
	Detail      string `json:"detail"`
}
type Report struct {
	OK              bool        `json:"ok"`
	Checked         map[int]int `json:"checked"`
	Violations      []Violation `json:"violations"`
	MissingEvidence []Violation `json:"missing_evidence"`
}
