package scheduler

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	longrunning "cloud.google.com/go/longrunning/autogen/longrunningpb"
	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	semver "github.com/bazelbuild/remote-apis/build/bazel/semver"
	w "github.com/simon-jd/remex/api/worker/v1"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Config struct {
	CASEndpoint                  string
	LeaseDuration, RecoveryGrace time.Duration
	MaxAttempts                  int
	Now                          func() time.Time
}
type workerInfo struct {
	properties map[string]string
	snapshot   string
	deadline   time.Time
}
type Scheduler struct {
	re.UnimplementedExecutionServer
	re.UnimplementedCapabilitiesServer
	w.UnimplementedWorkerServiceServer
	mu               sync.Mutex
	store            Store
	log              *commitLog
	cache            Cache
	cfg              Config
	ledger           *Ledger
	deadlines        *leaseDeadlines
	commandDeadlines map[string]time.Time
	workers          map[string]workerInfo
	// Closed when the named execution changes; Execute/WaitExecution streams
	// wait on their own execution only.
	waiters map[string]chan struct{}
	// Committed, cacheable executions not yet in the action cache, and the
	// ones a publisher is currently working on (outside the lock).
	unpublished, publishing map[string]bool
	// Indexes over the ledger, maintained by commit, so no RPC scans history:
	// queued or retrying executions; the in-flight execution per (instance,
	// action digest) that new requests join; attempt IDs by (worker, dir).
	runnable    map[string]bool
	queues      runQueues
	inflight    map[string]string
	attemptDirs map[string]string
	// After a restart with existing state, an action no live worker matches
	// stays queued until recoveryUntil, because workers re-register within
	// one poll. Tick then marks the ones still unmatched Unschedulable.
	recoveryUntil time.Time
	graceQueued   map[string]bool
	failure       chan error
	failed        error
	// The ledger's event count, written under mu and read without it by
	// EventCount.
	events atomic.Int64
}

func New(ctx context.Context, store Store, cache Cache, cfg Config) (*Scheduler, error) {
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.RecoveryGrace <= 0 {
		cfg.RecoveryGrace = cfg.LeaseDuration
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	l, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}
	if err = validateLedger(l); err != nil {
		return nil, err
	}
	s := &Scheduler{
		store:            store,
		cache:            cache,
		cfg:              cfg,
		ledger:           l,
		deadlines:        newLeaseDeadlines(),
		commandDeadlines: map[string]time.Time{},
		workers:          map[string]workerInfo{},
		waiters:          map[string]chan struct{}{},
		unpublished:      map[string]bool{},
		publishing:       map[string]bool{},
		runnable:         map[string]bool{},
		queues:           runQueues{},
		inflight:         map[string]string{},
		attemptDirs:      map[string]string{},
		failure:          make(chan error, 1),
	}
	now := cfg.Now()
	if len(l.Executions) > 0 {
		s.recoveryUntil = now.Add(cfg.RecoveryGrace)
	}
	s.graceQueued = map[string]bool{}
	t := l.begin()
	for _, a := range l.Attempts {
		e := l.execution(a.ExecutionID)
		if e == nil {
			return nil, fmt.Errorf("attempt %s has missing execution", a.ID)
		}
		if a.Epoch == e.CurrentEpoch && !terminal(e) {
			s.deadlines.set(a.ID, a.WorkerID, now.Add(cfg.RecoveryGrace))
			if a.CommandStartedAt != nil && a.Phase == "Running" {
				var action re.Action
				if proto.Unmarshal(e.Action, &action) == nil && action.Timeout != nil {
					s.commandDeadlines[a.ID] = now.Add(cfg.RecoveryGrace)
				}
			}
			t.event(
				now,
				"lease_recovered",
				t.execution(e.ID),
				t.attempt(a.ID),
				map[string]any{"grace_ms": cfg.RecoveryGrace.Milliseconds()},
			)
		}
	}
	s.reindex()
	if !t.empty() {
		if err = store.Save(ctx, &t.change); err != nil {
			return nil, err
		}
		l.apply(&t.change)
	}
	s.log = newCommitLog(store, s.storeFailed)
	return s, nil
}

// Close writes the changes still pending and stops the commit log.
func (s *Scheduler) Close() { s.log.close() }

// CommitStats reports how many ledger changes were made and how many store
// transactions made them durable.
func (s *Scheduler) CommitStats() (changes, writes int) { return s.log.stats() }

// EventCount reports how many events the in-memory ledger holds. It does not
// take the lock, so a caller timing other calls does not queue on it.
func (s *Scheduler) EventCount() int64 { return s.events.Load() }
func newID(prefix string) string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b[:])
}

// commit installs a transaction in the in-memory ledger, wakes the streams
// of the executions it changed and appends it to the commit log. The caller
// holds s.mu. The change is not durable yet: the call that made it, and every
// call that observes it, waits for the log in settle before answering.
func (s *Scheduler) commit(_ context.Context, t *tx) error {
	if s.failed != nil {
		return status.Error(codes.Unavailable, s.failed.Error())
	}
	if t.empty() {
		return nil
	}
	s.ledger.apply(&t.change)
	s.events.Store(int64(s.ledger.eventCount()))
	s.log.append(&t.change)
	for _, e := range t.change.Executions {
		if ch, ok := s.waiters[e.ID]; ok {
			close(ch)
			delete(s.waiters, e.ID)
		}
		s.indexExecution(e)
	}
	for _, a := range t.change.Attempts {
		s.indexAttempt(a)
	}
	return nil
}

// storeFailed stops the scheduler after a failed log write. Memory may hold
// changes the store does not, so nothing may be served from it any more.
func (s *Scheduler) storeFailed(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed != nil {
		return
	}
	s.failed = err
	select {
	case s.failure <- err:
	default:
	}
	// Every stream must observe the failure, not only changed ones.
	for id, ch := range s.waiters {
		close(ch)
		delete(s.waiters, id)
	}
}

// release ends a locked section: it records the log position the call has
// observed, its own changes included, and unlocks s.mu. A call that set
// *seen to noWait keeps it.
func (s *Scheduler) release(seen *uint64) {
	if *seen != noWait {
		*seen = s.log.position()
	}
	s.mu.Unlock()
}

// settle runs after s.mu is released. It waits until everything the call
// observed is durable and reports a failed write as the call's error.
func (s *Scheduler) settle(ctx context.Context, seen *uint64, err *error) {
	if *seen == noWait {
		return
	}
	if werr := s.log.wait(ctx, *seen); werr != nil && (*err == nil || status.Code(*err) != codes.Unavailable) {
		*err = werr
	}
}

// reindex rebuilds every index from the ledger.
func (s *Scheduler) reindex() {
	s.unpublished, s.runnable, s.inflight, s.attemptDirs = map[string]bool{}, map[string]bool{}, map[string]string{}, map[string]string{}
	s.queues = runQueues{}
	for _, e := range s.ledger.Executions {
		s.indexExecution(e)
	}
	for _, a := range s.ledger.Attempts {
		s.indexAttempt(a)
	}
}
func inflightKey(e *Execution) string {
	return fmt.Sprintf("%s\x00%s/%d", e.InstanceName, e.ActionDigest.Hash, e.ActionDigest.SizeBytes)
}
func (s *Scheduler) indexExecution(e *Execution) {
	setIf(s.unpublished, e.ID, e.State == "Committed")
	runnable := e.State == "Queued" || e.State == "Retrying"
	if runnable && !s.runnable[e.ID] {
		s.ledger.execution(e.ID) // keeps the index current
		s.queues.push(e, s.ledger.executionIndex[e.ID])
	}
	setIf(s.runnable, e.ID, runnable)
	key := inflightKey(e)
	if !terminal(e) && !e.DoNotCache && !e.HardwareTest {
		if _, taken := s.inflight[key]; !taken {
			s.inflight[key] = e.ID
		}
	} else if s.inflight[key] == e.ID {
		delete(s.inflight, key)
	}
}
func (s *Scheduler) indexAttempt(a *Attempt) {
	if a.AttemptDir != "" {
		key := a.WorkerID + "\x00" + a.AttemptDir
		if _, taken := s.attemptDirs[key]; !taken {
			s.attemptDirs[key] = a.ID
		}
	}
}
func setIf(set map[string]bool, id string, member bool) {
	if member {
		set[id] = true
	} else {
		delete(set, id)
	}
}

// byLedgerOrder sorts execution IDs oldest first.
func (s *Scheduler) byLedgerOrder(ids []string) []string {
	for _, id := range ids {
		s.ledger.execution(id) // keeps the index current
	}
	sort.Slice(ids, func(i, j int) bool { return s.ledger.executionIndex[ids[i]] < s.ledger.executionIndex[ids[j]] })
	return ids
}
func (s *Scheduler) watchLocked(id string) <-chan struct{} {
	ch, ok := s.waiters[id]
	if !ok {
		ch = make(chan struct{})
		s.waiters[id] = ch
	}
	return ch
}
func (s *Scheduler) Export() *Ledger { s.mu.Lock(); defer s.mu.Unlock(); return s.ledger.clone() }
func (s *Scheduler) GetCapabilities(context.Context, *re.GetCapabilitiesRequest) (*re.ServerCapabilities, error) {
	return &re.ServerCapabilities{
		ExecutionCapabilities: &re.ExecutionCapabilities{
			DigestFunction:  re.DigestFunction_SHA256,
			DigestFunctions: []re.DigestFunction_Value{re.DigestFunction_SHA256},
			ExecEnabled:     true,
		},
		LowApiVersion:  &semver.SemVer{Major: 2},
		HighApiVersion: &semver.SemVer{Major: 2, Minor: 11},
	}, nil
}

// Start is shared by streaming Execute and protocol tests.
func (s *Scheduler) Start(ctx context.Context, r *re.ExecuteRequest) (_ string, err error) {
	if r.DigestFunction != re.DigestFunction_UNKNOWN && r.DigestFunction != re.DigestFunction_SHA256 {
		return "", status.Error(codes.InvalidArgument, "only SHA256 is supported")
	}
	if err := validDigest(r.ActionDigest); err != nil {
		return "", err
	}
	raw, err := s.cache.ReadBlob(ctx, r.InstanceName, r.ActionDigest)
	if err != nil {
		return "", err
	}
	if err = verifyBlob(r.ActionDigest, raw); err != nil {
		return "", err
	}
	var action re.Action
	if err = proto.Unmarshal(raw, &action); err != nil {
		return "", status.Error(codes.InvalidArgument, "malformed Action")
	}
	if action.Timeout != nil && (action.Timeout.CheckValid() != nil || action.Timeout.AsDuration() <= 0) {
		return "", status.Error(codes.InvalidArgument, "invalid action timeout")
	}
	if err = validDigest(action.CommandDigest); err != nil {
		return "", err
	}
	if err = validDigest(action.InputRootDigest); err != nil {
		return "", err
	}
	commandRaw, err := s.cache.ReadBlob(ctx, r.InstanceName, action.CommandDigest)
	if err != nil {
		return "", err
	}
	if err = verifyBlob(action.CommandDigest, commandRaw); err != nil {
		return "", err
	}
	var command re.Command
	if proto.Unmarshal(commandRaw, &command) != nil || len(command.Arguments) == 0 {
		return "", status.Error(codes.InvalidArgument, "malformed or empty Command")
	}
	platform, err := ParsePlatform(action.Platform)
	if err != nil {
		return "", status.Error(codes.InvalidArgument, err.Error())
	}
	if len(platform) == 0 && command.Platform != nil {
		platform, err = ParsePlatform(command.Platform)
		if err != nil {
			return "", status.Error(codes.InvalidArgument, err.Error())
		}
	}
	e := &Execution{
		ID:            newID("executions/"),
		InvocationIDs: invocationIDs(ctx),
		ActionDigest:  digestModel(r.ActionDigest),
		InstanceName:  r.InstanceName,
		Platform:      platform,
		DoNotCache:    action.DoNotCache,
		HardwareTest:  hardware(platform),
		State:         "Queued",
		CurrentEpoch:  1,
		Action:        raw,
		Command:       commandRaw,
		CreatedAt:     s.cfg.Now().UTC(),
		UpdatedAt:     s.cfg.Now().UTC(),
	}
	cacheable := !e.DoNotCache && !e.HardwareTest
	if !r.SkipCacheLookup && cacheable {
		result, cacheErr := s.cache.GetActionResult(ctx, r.InstanceName, r.ActionDigest)
		if cacheErr == nil && result.ExitCode == 0 {
			digests, verifyErr := ResultDigests(ctx, s.cache, r.InstanceName, result)
			if verifyErr == nil {
				missing, verifyErr := s.cache.Missing(ctx, r.InstanceName, digests)
				if verifyErr == nil && len(missing) == 0 {
					e.Result, _ = proto.Marshal(result)
					e.State = "Published"
					e.CachedResult = true
				}
			}
		} else if cacheErr != nil && status.Code(cacheErr) != codes.NotFound {
			return "", cacheErr
		}
	}
	var seen uint64
	defer s.settle(ctx, &seen, &err)
	s.mu.Lock()
	defer s.release(&seen)
	if s.failed != nil {
		return "", status.Error(codes.Unavailable, s.failed.Error())
	}
	if cacheable && !e.CachedResult {
		if id, ok := s.inflight[inflightKey(e)]; ok {
			existing := s.ledger.execution(id)
			t := s.ledger.begin()
			current := t.execution(existing.ID)
			joined := false
			for _, id := range e.InvocationIDs {
				found := false
				for _, old := range current.InvocationIDs {
					if old == id {
						found = true
						break
					}
				}
				if !found {
					current.InvocationIDs = append(current.InvocationIDs, id)
					joined = true
				}
			}
			if joined {
				t.event(
					s.cfg.Now(),
					"invocation_joined",
					current,
					nil,
					map[string]any{"invocation_ids": e.InvocationIDs},
				)
				if err := s.commit(ctx, t); err != nil {
					return "", err
				}
			}
			return existing.ID, nil
		}
	}
	t := s.ledger.begin()
	t.addExecution(e)
	t.event(s.cfg.Now(), "execution_created", e, nil, nil)
	if e.CachedResult {
		t.event(s.cfg.Now(), "cache_hit", e, nil, nil)
	} else {
		now := s.cfg.Now()
		reasons, matched := s.liveMatch(e, now)
		if !matched && now.Before(s.recoveryUntil) {
			s.graceQueued[e.ID] = true
		} else if !matched {
			sort.Strings(reasons)
			e.State = "Unschedulable"
			e.StatusCode = int32(codes.FailedPrecondition)
			e.Reason = "no live worker satisfies Action.platform"
			if len(reasons) > 0 {
				e.Reason += "; " + strings.Join(reasons, "; ")
			}
			e.StatusMessage = e.Reason
			t.event(now, "unschedulable", e, nil, map[string]any{"reason": e.Reason})
		}
	}
	if err = s.commit(ctx, t); err != nil {
		return "", err
	}
	return e.ID, nil
}
func (s *Scheduler) Execute(r *re.ExecuteRequest, stream re.Execution_ExecuteServer) error {
	id, err := s.Start(stream.Context(), r)
	if err != nil {
		return err
	}
	return s.stream(stream.Context(), id, stream.Send)
}
func (s *Scheduler) WaitExecution(r *re.WaitExecutionRequest, stream re.Execution_WaitExecutionServer) error {
	return s.stream(stream.Context(), r.Name, stream.Send)
}
func (s *Scheduler) stream(ctx context.Context, id string, send func(*longrunning.Operation) error) error {
	for {
		s.mu.Lock()
		if s.failed != nil {
			err := status.Error(codes.Unavailable, s.failed.Error())
			s.mu.Unlock()
			return err
		}
		e := s.ledger.execution(id)
		if e == nil {
			s.mu.Unlock()
			return status.Error(codes.NotFound, "unknown execution")
		}
		op, err := operation(e)
		changed := s.watchLocked(id)
		seen := s.log.position()
		s.mu.Unlock()
		if err != nil {
			return err
		}
		// Report only what a restart would also find.
		if err = s.log.wait(ctx, seen); err != nil {
			return err
		}
		if err = send(op); err != nil {
			return err
		}
		if op.Done {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
func operation(e *Execution) (*longrunning.Operation, error) {
	stage := re.ExecutionStage_QUEUED
	if e.State == "Leased" || e.State == "Running" || e.State == "Uploading" {
		stage = re.ExecutionStage_EXECUTING
	}
	done := terminal(e)
	if done {
		stage = re.ExecutionStage_COMPLETED
	}
	metadata, err := anypb.New(&re.ExecuteOperationMetadata{
		Stage:          stage,
		ActionDigest:   digestPB(e.ActionDigest),
		DigestFunction: re.DigestFunction_SHA256,
	})
	if err != nil {
		return nil, err
	}
	op := &longrunning.Operation{Name: e.ID, Done: done, Metadata: metadata}
	if done {
		response := &re.ExecuteResponse{
			Status:       &rpcstatus.Status{Code: e.StatusCode, Message: e.StatusMessage},
			CachedResult: e.CachedResult,
		}
		if len(e.Result) > 0 || e.WinnerAttemptID != "" || e.CachedResult {
			response.Result = &re.ActionResult{}
			if err = proto.Unmarshal(e.Result, response.Result); err != nil {
				return nil, err
			}
		}
		result, err := anypb.New(response)
		if err != nil {
			return nil, err
		}
		op.Result = &longrunning.Operation_Response{Response: result}
	}
	return op, nil
}
func validWorker(id string, props map[string]string, snapshot string) error {
	if id == "" || len(id) > 200 {
		return status.Error(codes.InvalidArgument, "worker_id is required")
	}
	if props["pool"] == "gpu" {
		var device struct {
			UUID              string `json:"uuid"`
			ComputeCapability string `json:"compute_capability"`
			DriverVersion     string `json:"driver_version"`
		}
		if snapshot == "" || json.Unmarshal([]byte(snapshot), &device) != nil || device.UUID == "" {
			return status.Error(codes.FailedPrecondition, "GPU properties require a fresh NVML device snapshot")
		}
		if props["gpu.cc"] == "" ||
			props["gpu.driver_major"] == "" ||
			props["gpu.cc"] != device.ComputeCapability ||
			props["gpu.driver_major"] != strings.Split(
				device.DriverVersion,
				".",
			)[0] {
			return status.Error(codes.FailedPrecondition, "GPU properties disagree with the NVML snapshot")
		}
	}
	return nil
}
func copyProps(p map[string]string) map[string]string {
	c := map[string]string{}
	for k, v := range p {
		c[k] = v
	}
	return c
}
func (s *Scheduler) Register(_ context.Context, r *w.RegisterRequest) (*w.RegisterResponse, error) {
	if err := validWorker(r.WorkerId, r.Properties, r.DeviceSnapshotJson); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers[r.WorkerId] = workerInfo{
		copyProps(r.Properties),
		r.DeviceSnapshotJson,
		s.cfg.Now().Add(2 * s.cfg.LeaseDuration),
	}
	return &w.RegisterResponse{
		LeaseDurationMs:     s.cfg.LeaseDuration.Milliseconds(),
		HeartbeatIntervalMs: s.cfg.LeaseDuration.Milliseconds() / 3,
	}, nil
}
func (s *Scheduler) Lease(ctx context.Context, r *w.LeaseRequest) (_ *w.LeaseResponse, err error) {
	if err := validWorker(r.WorkerId, r.Properties, r.DeviceSnapshotJson); err != nil {
		return nil, err
	}
	// Only a granted lease must wait for the log: an empty answer tells the
	// worker nothing that a lost change could contradict.
	seen := noWait
	defer s.settle(ctx, &seen, &err)
	s.mu.Lock()
	defer s.release(&seen)
	if _, ok := s.workers[r.WorkerId]; !ok {
		return nil, status.Error(codes.FailedPrecondition, "register worker first")
	}
	now := s.cfg.Now()
	s.workers[r.WorkerId] = workerInfo{copyProps(r.Properties), r.DeviceSnapshotJson, now.Add(2 * s.cfg.LeaseDuration)}
	if err := s.expireLocked(ctx, now); err != nil {
		return nil, err
	}
	// One attempt per worker: only live attempts have deadlines.
	for id, deadline := range s.deadlines.worker(r.WorkerId) {
		a := s.ledger.attempt(id)
		if a == nil || a.WorkerID != r.WorkerId || !now.Before(deadline.at) {
			continue
		}
		e := s.ledger.execution(a.ExecutionID)
		if a.Epoch == e.CurrentEpoch && !terminal(e) {
			return &w.LeaseResponse{}, nil
		}
	}
	runnable := func(position int) bool { return s.runnable[s.ledger.Executions[position].ID] }
	if position := s.queues.oldest(r.Properties, runnable); position >= 0 {
		e := s.ledger.Executions[position]
		t := s.ledger.begin()
		e = t.execution(e.ID)
		a := &Attempt{
			ID:                 newID("attempt-"),
			ExecutionID:        e.ID,
			Epoch:              e.CurrentEpoch,
			WorkerID:           r.WorkerId,
			WorkerPlatform:     copyProps(r.Properties),
			DeviceVerified:     r.Properties["pool"] != "gpu" || r.DeviceSnapshotJson != "",
			DeviceSnapshotJSON: r.DeviceSnapshotJson,
			Phase:              "Materializing",
			Status:             "UNKNOWN",
			ResultDigests:      []Digest{},
		}
		e.State = "Leased"
		t.addAttempt(a)
		t.event(
			now,
			"lease_granted",
			e,
			a,
			map[string]any{"worker_platform": a.WorkerPlatform, "device_verified": a.DeviceVerified},
		)
		if err := s.commit(ctx, t); err != nil {
			return nil, err
		}
		seen = 0
		s.deadlines.set(a.ID, a.WorkerID, now.Add(s.cfg.LeaseDuration))
		return &w.LeaseResponse{
			Available:       true,
			ExecutionId:     e.ID,
			AttemptId:       a.ID,
			Epoch:           a.Epoch,
			LeaseDurationMs: s.cfg.LeaseDuration.Milliseconds(),
			InstanceName:    e.InstanceName,
			ActionDigest:    &w.Digest{Hash: e.ActionDigest.Hash, SizeBytes: e.ActionDigest.SizeBytes},
			Action:          e.Action,
			Command:         e.Command,
			CasEndpoint:     s.cfg.CASEndpoint,
			QueuedTimestamp: timestamppb.New(e.CreatedAt),
		}, nil
	}
	return &w.LeaseResponse{}, nil
}
func (s *Scheduler) fence(worker, id string, epoch int64, now time.Time) (*Attempt, *Execution, bool) {
	a := s.ledger.attempt(id)
	if a == nil {
		return nil, nil, false
	}
	e := s.ledger.execution(a.ExecutionID)
	deadline, _ := s.deadlines.get(a.ID)
	return a, e, a.WorkerID == worker &&
		a.Epoch == epoch &&
		e.CurrentEpoch == epoch &&
		!terminal(e) &&
		now.Before(deadline)
}
func (s *Scheduler) Heartbeat(ctx context.Context, r *w.HeartbeatRequest) (_ *w.HeartbeatResponse, err error) {
	var seen uint64
	defer s.settle(ctx, &seen, &err)
	s.mu.Lock()
	defer s.release(&seen)
	now := s.cfg.Now()
	if err := s.expireLocked(ctx, now); err != nil {
		return nil, err
	}
	a, e, ok := s.fence(r.WorkerId, r.AttemptId, r.Epoch, now)
	if !ok {
		decision := w.HeartbeatResponse_FENCED
		if e != nil && e.State == "Cancelled" {
			decision = w.HeartbeatResponse_CANCEL
		}
		return &w.HeartbeatResponse{Decision: decision}, nil
	}
	phase := "Materializing"
	switch r.Phase {
	case w.HeartbeatRequest_RUNNING:
		phase = "Running"
	case w.HeartbeatRequest_UPLOADING:
		phase = "Uploading"
	case w.HeartbeatRequest_MATERIALIZING:
	default:
		return nil, status.Error(codes.InvalidArgument, "unknown phase")
	}
	if (a.Phase == "Running" && phase == "Materializing") || (a.Phase == "Uploading" && phase != "Uploading") {
		return nil, status.Error(codes.FailedPrecondition, "attempt phase cannot move backwards")
	}
	deadline, hasDeadline := s.commandDeadlines[a.ID]
	timedOut := hasDeadline && !now.Before(deadline)
	if timedOut && phase != "Uploading" && !now.Before(deadline.Add(s.cfg.LeaseDuration)) {
		return &w.HeartbeatResponse{Decision: w.HeartbeatResponse_CANCEL}, nil
	}
	committed := false
	if a.Phase != phase || (timedOut && !a.CommandTimedOut) {
		oldPhase := a.Phase
		t := s.ledger.begin()
		a = t.attempt(a.ID)
		e = t.execution(e.ID)
		a.Phase = phase
		e.State = phase
		kind := "uploading"
		if timedOut && !a.CommandTimedOut {
			a.CommandTimedOut = true
			t.event(now, "command_timed_out", e, a, nil)
		}
		if phase == "Running" && oldPhase != "Running" {
			t := now.UTC()
			a.CommandStartedAt = &t
			kind = "command_started"
		}
		if oldPhase != phase {
			t.event(now, kind, e, a, nil)
		}
		if err := s.commit(ctx, t); err != nil {
			return nil, err
		}
		committed = true
		if phase == "Running" && oldPhase != "Running" {
			var action re.Action
			_ = proto.Unmarshal(e.Action, &action)
			if action.Timeout != nil {
				s.commandDeadlines[a.ID] = now.Add(action.Timeout.AsDuration())
			}
		}
	}
	if phase == "Uploading" {
		delete(s.commandDeadlines, a.ID)
	}
	s.deadlines.set(a.ID, a.WorkerID, now.Add(s.cfg.LeaseDuration))
	if worker, ok := s.workers[r.WorkerId]; ok {
		worker.deadline = now.Add(2 * s.cfg.LeaseDuration)
		s.workers[r.WorkerId] = worker
	}
	if !committed {
		// The store holds a prefix of memory. Nothing in memory fences this
		// attempt, so nothing durable does either: CONTINUE cannot be undone
		// by a lost change, and the heartbeat need not wait for the log.
		seen = noWait
	}
	return &w.HeartbeatResponse{
		Decision:        w.HeartbeatResponse_CONTINUE,
		LeaseDurationMs: s.cfg.LeaseDuration.Milliseconds(),
	}, nil
}
func (s *Scheduler) expireLocked(ctx context.Context, now time.Time) error {
	if s.failed != nil {
		return status.Error(codes.Unavailable, s.failed.Error())
	}
	// Only due deadlines are examined, never every live attempt or the
	// attempt history. A due attempt that is no longer current (cancelled,
	// or superseded) has nothing to expire; its deadline is dropped.
	ids := []string{}
	for _, id := range s.deadlines.due(now) {
		if a := s.ledger.attempt(id); a != nil {
			if e := s.ledger.execution(a.ExecutionID); a.Epoch == e.CurrentEpoch && !terminal(e) {
				ids = append(ids, id)
				continue
			}
		}
		s.deadlines.remove(id)
		delete(s.commandDeadlines, id)
	}
	if len(ids) == 0 {
		return nil
	}
	// Expire in ledger order so event sequences are deterministic.
	sort.Slice(ids, func(i, j int) bool { return s.ledger.attemptIndex[ids[i]] < s.ledger.attemptIndex[ids[j]] })
	t := s.ledger.begin()
	for _, id := range ids {
		a := t.attempt(id)
		e := t.execution(a.ExecutionID)
		a.Phase = "Expired"
		a.Status = "UNAVAILABLE"
		a.FailureCategory = "infrastructure"
		t.event(now, "lease_expired", e, a, nil)
		if int(e.CurrentEpoch) >= s.cfg.MaxAttempts {
			e.State = "InfraFailed"
			e.StatusCode = int32(codes.Unavailable)
			e.StatusMessage = "lease expired; attempt budget exhausted"
		} else {
			e.CurrentEpoch++
			e.State = "Retrying"
		}
	}
	if err := s.commit(ctx, t); err != nil {
		return err
	}
	for _, id := range ids {
		s.deadlines.remove(id)
		delete(s.commandDeadlines, id)
	}
	return nil
}

// liveMatch reports whether a live worker satisfies the execution's platform,
// and otherwise why each live worker does not.
func (s *Scheduler) liveMatch(e *Execution, now time.Time) ([]string, bool) {
	reasons := []string{}
	for id, worker := range s.workers {
		if !now.Before(worker.deadline) {
			continue
		}
		if err := Match(e.Platform, worker.properties); err == nil {
			return nil, true
		} else {
			reasons = append(reasons, id+": "+err.Error())
		}
	}
	return reasons, false
}

// endGraceLocked marks actions queued during the recovery grace Unschedulable
// if, now that workers have had time to re-register, none matches them.
func (s *Scheduler) endGraceLocked(ctx context.Context, now time.Time) error {
	if len(s.graceQueued) == 0 || now.Before(s.recoveryUntil) {
		return nil
	}
	t := s.ledger.begin()
	for _, id := range s.byLedgerOrder(slices.Collect(maps.Keys(s.graceQueued))) {
		if e := s.ledger.execution(id); e.State == "Queued" {
			if reasons, matched := s.liveMatch(e, now); !matched {
				e = t.execution(id)
				sort.Strings(reasons)
				e.State = "Unschedulable"
				e.StatusCode = int32(codes.FailedPrecondition)
				e.Reason = "no live worker satisfies Action.platform"
				if len(reasons) > 0 {
					e.Reason += "; " + strings.Join(reasons, "; ")
				}
				e.StatusMessage = e.Reason
				t.event(now, "unschedulable", e, nil, map[string]any{"reason": e.Reason})
			}
		}
	}
	if err := s.commit(ctx, t); err != nil {
		return err
	}
	s.graceQueued = map[string]bool{}
	return nil
}
func (s *Scheduler) Tick(ctx context.Context) (err error) {
	var seen uint64
	s.mu.Lock()
	err = s.expireLocked(ctx, s.cfg.Now())
	if err == nil {
		err = s.endGraceLocked(ctx, s.cfg.Now())
	}
	s.release(&seen)
	if s.settle(ctx, &seen, &err); err != nil {
		return err
	}
	return s.PublishPending(ctx)
}

// Failure is signaled once durable state or ownership becomes uncertain.
func (s *Scheduler) Failure() <-chan error { return s.failure }
func invocationIDs(ctx context.Context) []string {
	var ids []string
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ids
	}
	seen := map[string]bool{}
	for _, raw := range md.Get("build.bazel.remote.execution.v2.requestmetadata-bin") {
		var m re.RequestMetadata
		if proto.Unmarshal([]byte(raw), &m) == nil && m.ToolInvocationId != "" && !seen[m.ToolInvocationId] {
			seen[m.ToolInvocationId] = true
			ids = append(ids, m.ToolInvocationId)
		}
	}
	return ids
}
