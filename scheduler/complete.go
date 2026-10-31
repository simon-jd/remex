package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	w "github.com/simon-jd/remex/api/worker/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *Scheduler) rejectLocked(ctx context.Context, r *w.CompleteRequest, decision w.CompleteResponse_Decision, message string) (*w.CompleteResponse, error) {
	t := s.ledger.begin()
	a := t.attempt(r.AttemptId)
	var e *Execution
	if a != nil {
		e = t.execution(a.ExecutionID)
	}
	kind := "complete_rejected"
	if decision == w.CompleteResponse_FENCED {
		kind = "stale_complete_rejected"
	}
	t.event(s.cfg.Now(), kind, e, a, map[string]any{"submitted_epoch": r.Epoch, "worker_id": r.WorkerId, "reason": message})
	if err := s.commit(ctx, t); err != nil {
		return nil, err
	}
	return &w.CompleteResponse{Decision: decision, Message: message}, nil
}
func (s *Scheduler) Complete(ctx context.Context, r *w.CompleteRequest) (_ *w.CompleteResponse, err error) {
	r = proto.Clone(r).(*w.CompleteRequest)
	var seen uint64
	defer s.settle(ctx, &seen, &err)
	s.mu.Lock()
	if err := s.expireLocked(ctx, s.cfg.Now()); err != nil {
		s.release(&seen)
		return nil, err
	}
	a, e, ok := s.fence(r.WorkerId, r.AttemptId, r.Epoch, s.cfg.Now())
	// A retried Complete can acknowledge the same committed winner without any
	// additional publication. Once its lease was lost, no acknowledgment is given.
	if !ok {
		if a != nil && e.WinnerAttemptID == a.ID && a.WorkerID == r.WorkerId && a.Epoch == r.Epoch {
			s.release(&seen)
			return &w.CompleteResponse{Decision: w.CompleteResponse_ACCEPTED, Message: "already committed"}, nil
		}
		resp, err := s.rejectLocked(ctx, r, w.CompleteResponse_FENCED, "attempt lease or epoch is no longer current")
		s.release(&seen)
		return resp, err
	}
	instance := e.InstanceName
	if !r.CgroupEmpty || r.AttemptDir == "" {
		resp, err := s.rejectLocked(ctx, r, w.CompleteResponse_REJECTED, "cgroup must be empty and attempt_dir must be recorded")
		s.release(&seen)
		return resp, err
	}
	if old, used := s.attemptDirs[a.WorkerID+"\x00"+r.AttemptDir]; used && old != a.ID {
		resp, err := s.rejectLocked(ctx, r, w.CompleteResponse_REJECTED, "attempt directory was reused")
		s.release(&seen)
		return resp, err
	}
	if r.StatusCode < 0 || r.StatusCode > 16 {
		resp, err := s.rejectLocked(ctx, r, w.CompleteResponse_REJECTED, "invalid RPC status code")
		s.release(&seen)
		return resp, err
	}
	if r.StatusCode == 0 && a.CommandStartedAt == nil {
		resp, err := s.rejectLocked(ctx, r, w.CompleteResponse_REJECTED, "successful result requires command_started evidence")
		s.release(&seen)
		return resp, err
	}
	if r.OomKill < 0 {
		resp, err := s.rejectLocked(ctx, r, w.CompleteResponse_REJECTED, "invalid OOM counter")
		s.release(&seen)
		return resp, err
	}
	ctx = s.auditContext(ctx, e, a)
	s.release(&seen)
	// The audit metadata names the ledger prefix this call observed, so
	// that prefix must be durable before the cache sees it.
	if err := s.log.wait(ctx, seen); err != nil {
		return nil, err
	}
	var result re.ActionResult
	var digests []*re.Digest
	var verifyErr error
	if len(r.ActionResult) > 0 {
		verifyErr = proto.Unmarshal(r.ActionResult, &result)
	}
	if verifyErr == nil {
		digests, verifyErr = ResultDigests(ctx, s.cache, instance, &result)
	}
	if verifyErr == nil {
		var missing []*re.Digest
		missing, verifyErr = s.cache.Missing(ctx, instance, digests)
		if verifyErr == nil && len(missing) > 0 {
			verifyErr = fmt.Errorf("%d result blobs are absent from CAS", len(missing))
		}
	}
	s.mu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			s.release(&seen)
		}
	}()
	if err := s.expireLocked(ctx, s.cfg.Now()); err != nil {
		return nil, err
	}
	a, e, ok = s.fence(r.WorkerId, r.AttemptId, r.Epoch, s.cfg.Now())
	if !ok {
		if a != nil && e.WinnerAttemptID == a.ID && a.WorkerID == r.WorkerId && a.Epoch == r.Epoch {
			return &w.CompleteResponse{Decision: w.CompleteResponse_ACCEPTED, Message: "already committed"}, nil
		}
		return s.rejectLocked(ctx, r, w.CompleteResponse_FENCED, "lease expired while verifying CAS")
	}
	if verifyErr != nil {
		return s.rejectLocked(ctx, r, w.CompleteResponse_REJECTED, verifyErr.Error())
	}
	t := s.ledger.begin()
	a = t.attempt(r.AttemptId)
	e = t.execution(a.ExecutionID)
	now := s.cfg.Now()
	deadline, hasDeadline := s.commandDeadlines[a.ID]
	if a.CommandTimedOut || (hasDeadline && !now.Before(deadline)) {
		if !a.CommandTimedOut {
			a.CommandTimedOut = true
			t.event(now, "command_timed_out", e, a, nil)
		}
		r.StatusCode = int32(codes.DeadlineExceeded)
		r.StatusMessage = "action exceeded command timeout"
		r.FailureCategory = "TIMEOUT"
	}
	// Kernel OOM evidence overrides a successful-looking process result.
	if r.OomKill > 0 {
		r.StatusCode = int32(codes.ResourceExhausted)
		r.StatusMessage = "attempt cgroup reported oom_kill"
		r.FailureCategory = "OOM"
	}
	a.AttemptDir = r.AttemptDir
	a.CgroupEmpty = true
	a.ExitCode = result.ExitCode
	a.Status = codes.Code(r.StatusCode).String()
	a.OOMKill = r.OomKill
	a.ResourceUsageJSON = r.ResourceUsageJson
	a.FailureCategory = r.FailureCategory
	a.Result = r.ActionResult
	if r.DeviceSnapshotJson != "" {
		a.DeviceSnapshotJSON = r.DeviceSnapshotJson
	}
	for _, d := range digests {
		a.ResultDigests = append(a.ResultDigests, digestModel(d))
	}
	t.event(now, "cas_verified", e, a, map[string]any{"result_digests": a.ResultDigests})
	infra := r.StatusCode == int32(codes.Unavailable) || r.StatusCode == int32(codes.Internal) || r.StatusCode == int32(codes.DataLoss) || r.StatusCode == int32(codes.ResourceExhausted)
	if infra && int(e.CurrentEpoch) < s.cfg.MaxAttempts {
		a.Phase = "InfraFailed"
		t.event(now, "attempt_infra_failed", e, a, map[string]any{"status_code": r.StatusCode})
		e.CurrentEpoch++
		e.State = "Retrying"
	} else {
		a.Phase = "Committed"
		e.WinnerAttemptID = a.ID
		e.State = "Committed"
		e.Result = r.ActionResult
		e.StatusCode = r.StatusCode
		e.StatusMessage = r.StatusMessage
		t.event(now, "attempt_committed", e, a, map[string]any{"status_code": r.StatusCode, "exit_code": result.ExitCode})
	}
	if err := s.commit(ctx, t); err != nil {
		return nil, err
	}
	delete(s.deadlines, a.ID)
	delete(s.commandDeadlines, a.ID)
	// Publishing is separate from the durable commit and runs without the
	// lock: a slow action cache must never stall leases and heartbeats. A
	// cache outage never turns an accepted winner into a new epoch; Tick
	// retries the same result.
	if e.State == "Committed" {
		unlocked = true
		s.release(&seen)
		_ = s.publish(ctx, e.ID, true)
	}
	return &w.CompleteResponse{Decision: w.CompleteResponse_ACCEPTED}, nil
}

// publish writes a committed winner to the action cache, then records that
// durably. The cache calls run without s.mu; one publisher per execution
// at a time. verified means the caller has just checked every result blob,
// so the walk is not repeated; on recovery it is, since CAS entries can be
// evicted between commit and publish.
func (s *Scheduler) publish(ctx context.Context, id string, verified bool) (err error) {
	s.mu.Lock()
	if s.failed != nil {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, s.failed.Error())
	}
	e := s.ledger.execution(id)
	if e == nil || e.State != "Committed" || s.publishing[id] {
		s.mu.Unlock()
		return nil
	}
	a := s.ledger.attempt(e.WinnerAttemptID)
	if a == nil || a.Epoch != e.CurrentEpoch {
		s.mu.Unlock()
		return status.Error(codes.DataLoss, "committed winner has inconsistent epoch")
	}
	if !cacheableResult(e, a) {
		s.mu.Unlock()
		return nil
	}
	// Committed rows are never modified in place, so e and a stay valid.
	winner := a.ID
	s.publishing[id] = true
	ctx = s.auditContext(ctx, e, a)
	seen := s.log.position()
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.publishing, id)
		s.mu.Unlock()
	}()
	// A result reaches the action cache only once its winner is durable.
	if err := s.log.wait(ctx, seen); err != nil {
		return err
	}

	var result re.ActionResult
	if err := proto.Unmarshal(e.Result, &result); err != nil {
		return err
	}
	if !verified {
		digests, err := ResultDigests(ctx, s.cache, e.InstanceName, &result)
		if err != nil {
			return err
		}
		missing, err := s.cache.Missing(ctx, e.InstanceName, digests)
		if err != nil {
			return err
		}
		if len(missing) > 0 {
			return status.Error(codes.FailedPrecondition, "committed result blobs have been evicted")
		}
	}
	if err := s.cache.UpdateActionResult(ctx, e.InstanceName, digestPB(e.ActionDigest), &result); err != nil {
		return err
	}

	var recorded uint64
	defer s.settle(ctx, &recorded, &err)
	s.mu.Lock()
	defer s.release(&recorded)
	if current := s.ledger.execution(id); current == nil || current.State != "Committed" || current.WinnerAttemptID != winner {
		return nil
	}
	t := s.ledger.begin()
	e = t.execution(id)
	a = t.attempt(winner)
	e.State = "Published"
	a.Published = true
	t.event(s.cfg.Now(), "ac_published", e, a, map[string]any{"action_digest": e.ActionDigest})
	return s.commit(ctx, t)
}

func cacheableResult(e *Execution, a *Attempt) bool {
	return !e.DoNotCache && !e.HardwareTest && e.StatusCode == 0 && a.ExitCode == 0 && a.OOMKill == 0 && !a.CommandTimedOut
}

// Correlation identifies the durable event prefix before this real RPC; it is
// not a replacement for independent capture of its request and response.
func (s *Scheduler) auditContext(ctx context.Context, e *Execution, a *Attempt) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set("x-remex-execution-id", e.ID)
	md.Set("x-remex-attempt-id", a.ID)
	md.Set("x-remex-epoch", strconv.FormatInt(a.Epoch, 10))
	md.Set("x-remex-ledger-sequence", strconv.Itoa(len(s.ledger.Events)))
	return metadata.NewOutgoingContext(ctx, md)
}

// PublishPending retries publication of committed winners, oldest first.
// A result that cannot be published, for example because its blobs were
// evicted, does not hold back the others: every pending result is tried
// and the failures are returned together, each naming its execution. It
// stops early only when the scheduler has failed or ctx is done, since
// every later attempt would fail the same way.
func (s *Scheduler) PublishPending(ctx context.Context) error {
	s.mu.Lock()
	if s.failed != nil {
		// Memory may be ahead of the store; publish nothing from it.
		err := status.Error(codes.Unavailable, s.failed.Error())
		s.mu.Unlock()
		return err
	}
	ids := []string{}
	for id := range s.unpublished {
		e := s.ledger.execution(id)
		a := s.ledger.attempt(e.WinnerAttemptID)
		// These immutable committed results can never be published. Keep
		// malformed winners and failed-state handling on the publish path.
		if s.failed == nil && a != nil && a.Epoch == e.CurrentEpoch && !cacheableResult(e, a) {
			delete(s.unpublished, id)
			continue
		}
		ids = append(ids, id)
	}
	ids = s.byLedgerOrder(ids)
	s.mu.Unlock()
	var failures []error
	for _, id := range ids {
		err := s.publish(ctx, id, false)
		if err == nil {
			continue
		}
		failures = append(failures, fmt.Errorf("publish %s: %w", id, err))
		s.mu.Lock()
		failed := s.failed != nil
		s.mu.Unlock()
		if failed || ctx.Err() != nil {
			break
		}
	}
	return errors.Join(failures...)
}
func (s *Scheduler) Cancel(ctx context.Context, id string) (err error) {
	var seen uint64
	defer s.settle(ctx, &seen, &err)
	s.mu.Lock()
	defer s.release(&seen)
	if s.ledger.execution(id) == nil {
		return status.Error(codes.NotFound, "unknown execution")
	}
	if terminal(s.ledger.execution(id)) {
		return nil
	}
	t := s.ledger.begin()
	e := t.execution(id)
	e.State = "Cancelled"
	e.StatusCode = int32(codes.Canceled)
	e.StatusMessage = "cancelled by operator"
	t.event(s.cfg.Now(), "execution_cancelled", e, nil, nil)
	return s.commit(ctx, t)
}

// Run maintains leases and recovery publication. Use a cancellable context.
func (s *Scheduler) Run(ctx context.Context, onError func(error)) {
	interval := s.cfg.LeaseDuration / 4
	if interval > time.Second {
		interval = time.Second
	}
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := s.Tick(callCtx)
			cancel()
			if err != nil && onError != nil {
				onError(err)
			}
		}
	}
}
