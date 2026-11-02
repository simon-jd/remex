// Package scheduler implements a single-leader, durable REAPI execution service.
package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

type Digest struct {
	Hash      string `json:"hash"`
	SizeBytes int64  `json:"size_bytes"`
}
type Execution struct {
	ID              string            `json:"id"`
	InvocationIDs   []string          `json:"invocation_ids,omitempty"`
	ActionDigest    Digest            `json:"action_digest"`
	InstanceName    string            `json:"instance_name"`
	Platform        map[string]string `json:"platform"`
	DoNotCache      bool              `json:"do_not_cache"`
	HardwareTest    bool              `json:"hardware_test"`
	State           string            `json:"state"`
	CurrentEpoch    int64             `json:"current_epoch"`
	WinnerAttemptID string            `json:"winner_attempt_id,omitempty"`
	Action          []byte            `json:"action"`
	Command         []byte            `json:"command"`
	Result          []byte            `json:"result,omitempty"`
	StatusCode      int32             `json:"status_code"`
	StatusMessage   string            `json:"status_message,omitempty"`
	CachedResult    bool              `json:"cached_result"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	Reason          string            `json:"reason,omitempty"`
}
type Attempt struct {
	ID                 string            `json:"id"`
	ExecutionID        string            `json:"execution_id"`
	Epoch              int64             `json:"epoch"`
	WorkerID           string            `json:"worker_id"`
	WorkerPlatform     map[string]string `json:"worker_platform"`
	DeviceVerified     bool              `json:"device_verified"`
	DeviceSnapshotJSON string            `json:"device_snapshot_json,omitempty"`
	AttemptDir         string            `json:"attempt_dir,omitempty"`
	CgroupEmpty        bool              `json:"cgroup_empty"`
	Phase              string            `json:"phase"`
	CommandStartedAt   *time.Time        `json:"command_started_at,omitempty"`
	CommandTimedOut    bool              `json:"command_timed_out"`
	ExitCode           int32             `json:"exit_code"`
	Status             string            `json:"status"`
	OOMKill            int64             `json:"oom_kill"`
	Published          bool              `json:"published"`
	ResultDigests      []Digest          `json:"result_digests"`
	Result             []byte            `json:"result,omitempty"`
	ResourceUsageJSON  string            `json:"resource_usage_json,omitempty"`
	FailureCategory    string            `json:"failure_category,omitempty"`
}
type Event struct {
	Sequence    int64          `json:"sequence"`
	Time        time.Time      `json:"time"`
	Kind        string         `json:"kind"`
	ExecutionID string         `json:"execution_id,omitempty"`
	AttemptID   string         `json:"attempt_id,omitempty"`
	Epoch       int64          `json:"epoch,omitempty"`
	Details     map[string]any `json:"details,omitempty"`
}
type Ledger struct {
	SchemaVersion int          `json:"schema_version"`
	Executions    []*Execution `json:"executions"`
	Attempts      []*Attempt   `json:"attempts"`
	Events        []Event      `json:"events"`

	// Positions by ID, rebuilt whenever a lookup finds them stale (tests and
	// stores may edit the slices directly). First occurrence wins.
	executionIndex map[string]int
	attemptIndex   map[string]int
}

// Change is one atomic ledger mutation: the execution and attempt rows it
// creates or replaces, and the events it appends. Stores persist exactly
// this, so a commit costs O(rows touched) rather than O(ledger).
type Change struct {
	Executions []*Execution
	Attempts   []*Attempt
	Events     []Event
}

func NewLedger() *Ledger {
	return &Ledger{SchemaVersion: 1, Executions: []*Execution{}, Attempts: []*Attempt{}, Events: []Event{}}
}

func cloneExecution(e *Execution) *Execution {
	v := *e
	v.InvocationIDs = slices.Clone(e.InvocationIDs)
	v.Platform = maps.Clone(e.Platform)
	v.Action = bytes.Clone(e.Action)
	v.Command = bytes.Clone(e.Command)
	v.Result = bytes.Clone(e.Result)
	return &v
}
func cloneAttempt(a *Attempt) *Attempt {
	v := *a
	v.WorkerPlatform = maps.Clone(a.WorkerPlatform)
	v.ResultDigests = slices.Clone(a.ResultDigests)
	v.Result = bytes.Clone(a.Result)
	if a.CommandStartedAt != nil {
		started := *a.CommandStartedAt
		v.CommandStartedAt = &started
	}
	return &v
}

// clone deep-copies the whole ledger, for exports and the memory store.
// The scheduler's own mutations go through tx and copy only what they touch.
func (l *Ledger) clone() *Ledger {
	c := *l
	c.executionIndex, c.attemptIndex = nil, nil
	if l.Executions != nil {
		c.Executions = make([]*Execution, len(l.Executions))
		for i, e := range l.Executions {
			if e != nil {
				c.Executions[i] = cloneExecution(e)
			}
		}
	}
	if l.Attempts != nil {
		c.Attempts = make([]*Attempt, len(l.Attempts))
		for i, a := range l.Attempts {
			if a != nil {
				c.Attempts[i] = cloneAttempt(a)
			}
		}
	}
	if l.Events != nil {
		c.Events = make([]Event, len(l.Events))
		for i, event := range l.Events {
			c.Events[i] = event
			if event.Details != nil {
				c.Events[i].Details = cloneDetails(event.Details)
			}
		}
	}
	return &c
}
func cloneDetails(details map[string]any) map[string]any {
	out := make(map[string]any, len(details))
	for k, v := range details {
		out[k] = cloneDetailValue(v)
	}
	return out
}
func cloneDetailValue(value any) any {
	switch v := value.(type) {
	case nil, string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, json.Number, Digest:
		return v
	case map[string]any:
		if v == nil {
			return v
		}
		return cloneDetails(v)
	case map[string]string:
		return maps.Clone(v)
	case []any:
		if v == nil {
			return v
		}
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = cloneDetailValue(item)
		}
		return out
	case []string:
		return slices.Clone(v)
	case []Digest:
		return slices.Clone(v)
	case []byte:
		return bytes.Clone(v)
	default:
		// Preserve the prior JSON contract for a future event payload type
		// until it receives an explicit clone case; never alias mutable data.
		raw, err := json.Marshal(v)
		if err != nil {
			panic("non-JSON ledger event detail: " + err.Error())
		}
		var out any
		if err = json.Unmarshal(raw, &out); err != nil {
			panic(err)
		}
		return out
	}
}
func (l *Ledger) execution(id string) *Execution {
	if i, ok := l.executionIndex[id]; ok && i < len(l.Executions) && l.Executions[i].ID == id {
		return l.Executions[i]
	}
	if len(l.executionIndex) == len(l.Executions) && l.executionIndex != nil {
		if _, ok := l.executionIndex[id]; !ok {
			return nil
		}
	}
	l.executionIndex = make(map[string]int, len(l.Executions))
	for i := len(l.Executions) - 1; i >= 0; i-- {
		l.executionIndex[l.Executions[i].ID] = i
	}
	if i, ok := l.executionIndex[id]; ok {
		return l.Executions[i]
	}
	return nil
}
func (l *Ledger) attempt(id string) *Attempt {
	if i, ok := l.attemptIndex[id]; ok && i < len(l.Attempts) && l.Attempts[i].ID == id {
		return l.Attempts[i]
	}
	if len(l.attemptIndex) == len(l.Attempts) && l.attemptIndex != nil {
		if _, ok := l.attemptIndex[id]; !ok {
			return nil
		}
	}
	l.attemptIndex = make(map[string]int, len(l.Attempts))
	for i := len(l.Attempts) - 1; i >= 0; i-- {
		l.attemptIndex[l.Attempts[i].ID] = i
	}
	if i, ok := l.attemptIndex[id]; ok {
		return l.Attempts[i]
	}
	return nil
}

// apply installs a committed change: replaced rows take their old position,
// new rows are appended in order, and events are appended.
func (l *Ledger) apply(c *Change) {
	for _, e := range c.Executions {
		if l.execution(e.ID) != nil {
			l.Executions[l.executionIndex[e.ID]] = e
			continue
		}
		l.executionIndex[e.ID] = len(l.Executions)
		l.Executions = append(l.Executions, e)
	}
	for _, a := range c.Attempts {
		if l.attempt(a.ID) != nil {
			l.Attempts[l.attemptIndex[a.ID]] = a
			continue
		}
		l.attemptIndex[a.ID] = len(l.Attempts)
		l.Attempts = append(l.Attempts, a)
	}
	l.Events = append(l.Events, c.Events...)
}

// tx stages one mutation. A row is copied the first time the transaction
// writes it, so committed rows are never modified in place: readers that
// hold a pointer keep a consistent row, a failed commit leaves the ledger
// untouched, and a commit costs only the rows it touched.
type tx struct {
	l          *Ledger
	executions map[string]*Execution
	attempts   map[string]*Attempt
	change     Change
}

func (l *Ledger) begin() *tx {
	return &tx{l: l, executions: map[string]*Execution{}, attempts: map[string]*Attempt{}}
}

// execution returns the transaction's writable copy of an execution.
func (t *tx) execution(id string) *Execution {
	if e, ok := t.executions[id]; ok {
		return e
	}
	e := t.l.execution(id)
	if e == nil {
		return nil
	}
	e = cloneExecution(e)
	t.executions[id] = e
	t.change.Executions = append(t.change.Executions, e)
	return e
}
func (t *tx) attempt(id string) *Attempt {
	if a, ok := t.attempts[id]; ok {
		return a
	}
	a := t.l.attempt(id)
	if a == nil {
		return nil
	}
	a = cloneAttempt(a)
	t.attempts[id] = a
	t.change.Attempts = append(t.change.Attempts, a)
	return a
}
func (t *tx) addExecution(e *Execution) {
	t.executions[e.ID] = e
	t.change.Executions = append(t.change.Executions, e)
}
func (t *tx) addAttempt(a *Attempt) {
	t.attempts[a.ID] = a
	t.change.Attempts = append(t.change.Attempts, a)
}
func (t *tx) event(now time.Time, kind string, e *Execution, a *Attempt, details map[string]any) {
	t.change.Events = append(
		t.change.Events,
		newEvent(int64(len(t.l.Events)+len(t.change.Events)+1), now, kind, e, a, details),
	)
}
func (t *tx) empty() bool {
	return len(t.change.Executions) == 0 && len(t.change.Attempts) == 0 && len(t.change.Events) == 0
}
func (l *Ledger) event(now time.Time, kind string, e *Execution, a *Attempt, details map[string]any) {
	l.Events = append(l.Events, newEvent(int64(len(l.Events)+1), now, kind, e, a, details))
}
func newEvent(sequence int64, now time.Time, kind string, e *Execution, a *Attempt, details map[string]any) Event {
	ev := Event{Sequence: sequence, Time: now.UTC(), Kind: kind, Details: details}
	if e != nil {
		ev.ExecutionID = e.ID
		ev.Epoch = e.CurrentEpoch
		e.UpdatedAt = now.UTC()
	}
	if a != nil {
		ev.AttemptID = a.ID
		ev.Epoch = a.Epoch
	}
	return ev
}
func terminal(e *Execution) bool {
	switch e.State {
	case "Committed", "Published", "InfraFailed", "Cancelled", "Unschedulable":
		return true
	}
	return false
}

// Store commits ledger changes atomically. PostgreSQL persists a change's
// execution/attempt rows and events in a single batched transaction.
// A Store is owned by one scheduler; HA is deliberately out of scope.
type Store interface {
	Load(context.Context) (*Ledger, error)
	Save(context.Context, *Change) error
	Close() error
}
type MemoryStore struct {
	mu        sync.Mutex
	ledger    *Ledger
	SaveError error
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{ledger: NewLedger()} }
func (m *MemoryStore) Load(context.Context) (*Ledger, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ledger.clone(), nil
}
func (m *MemoryStore) Save(_ context.Context, c *Change) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.SaveError != nil {
		return m.SaveError
	}
	if err := checkSequence(int64(len(m.ledger.Events)), c.Events); err != nil {
		return err
	}
	copied := &Change{Events: slices.Clone(c.Events)}
	for _, e := range c.Executions {
		copied.Executions = append(copied.Executions, cloneExecution(e))
	}
	for _, a := range c.Attempts {
		copied.Attempts = append(copied.Attempts, cloneAttempt(a))
	}
	m.ledger.apply(copied)
	return nil
}

// checkSequence requires new events to continue the stored sequence exactly.
func checkSequence(last int64, events []Event) error {
	for _, ev := range events {
		if ev.Sequence != last+1 {
			return fmt.Errorf("non-contiguous new event sequence %d after %d", ev.Sequence, last)
		}
		last = ev.Sequence
	}
	return nil
}
func (m *MemoryStore) Snapshot(ctx context.Context) (*Ledger, error) { return m.Load(ctx) }
func (m *MemoryStore) Close() error                                  { return nil }
func validateLedger(l *Ledger) error {
	if l.SchemaVersion != 1 {
		return fmt.Errorf("unsupported ledger version %d", l.SchemaVersion)
	}
	return nil
}
