package scheduler

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	w "github.com/simon-jd/remex/api/worker/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Check observable dispatch order across mismatched work, an occupied worker,
// and an expired first attempt. A retry retains its place ahead of newer work.
func TestIdleHistoryLeaseOrderAndBusyWorker(t *testing.T) {
	f := setup(t, true)
	unmatched := f.start(t, true)
	first, second := f.start(t, true), f.start(t, true)
	f.s.ledger.execution(unmatched).Platform = map[string]string{"pool": "gpu"}
	lease := f.lease(t, "cpu1")
	if lease.ExecutionId != first {
		t.Fatalf("first compatible execution = %s, want %s", lease.ExecutionId, first)
	}
	response, err := f.s.Lease(f.ctx, &w.LeaseRequest{WorkerId: "cpu1", Properties: map[string]string{"pool": "cpu"}})
	if err != nil || response.Available || len(f.s.Export().Attempts) != 1 {
		t.Fatalf("occupied worker leased newer work: %v %v", response, err)
	}
	f.now = f.now.Add(time.Second)
	lease = f.lease(t, "cpu1")
	if lease.ExecutionId != first || lease.Epoch != 2 || f.s.Export().execution(second).State != "Queued" {
		t.Fatalf("retry order changed: %v", lease)
	}
}

// Expiry must be inclusive at the deadline, atomic on persistence failure,
// and deterministically logged in attempt order even with multiple deadlines.
// A failed write stops the scheduler with neither expiry durable; the
// restarted scheduler expires both, in order, after the recovery grace.
func TestIdleHistoryExpiryBoundaryAndAtomicity(t *testing.T) {
	f := setup(t, true)
	ids := []string{f.start(t, true), f.start(t, true)}
	leases := []*w.LeaseResponse{f.lease(t, "cpu1")}
	f.register(t, "cpu2", map[string]string{"pool": "cpu"})
	leases = append(leases, f.lease(t, "cpu2"))
	f.now = f.now.Add(time.Second - time.Nanosecond)
	if err := f.s.Tick(f.ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.s.deadlines) != 2 || f.s.Export().execution(ids[0]).CurrentEpoch != 1 {
		t.Fatal("lease expired before its deadline")
	}
	f.now = f.now.Add(time.Nanosecond)
	f.store.SaveError = errors.New("expiry persistence failure")
	if err := f.s.Tick(f.ctx); status.Code(err) != codes.Unavailable {
		t.Fatalf("expiry commit failure = %v", err)
	}
	f.store.SaveError = nil
	if err := f.s.Tick(f.ctx); status.Code(err) != codes.Unavailable {
		t.Fatalf("Tick after a failed write = %v, want Unavailable", err)
	}
	durable, err := f.store.Load(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if e := durable.execution(id); e.State != "Leased" || e.CurrentEpoch != 1 {
			t.Fatalf("failed write reached the store: %+v", e)
		}
	}
	f.restart(t)
	f.now = f.now.Add(2 * time.Second) // the recovery grace
	if err := f.s.Tick(f.ctx); err != nil {
		t.Fatal(err)
	}
	var expired []string
	for _, event := range f.s.Export().Events {
		if event.Kind == "lease_expired" {
			expired = append(expired, event.AttemptID)
		}
	}
	if len(expired) != 2 || expired[0] != leases[0].AttemptId || expired[1] != leases[1].AttemptId {
		t.Fatalf("expiry order = %v", expired)
	}
	for _, id := range ids {
		if e := f.s.Export().execution(id); e.State != "Retrying" || e.CurrentEpoch != 2 {
			t.Fatalf("deadline equality failed to expire: %+v", e)
		}
	}
	if len(f.s.deadlines) != 0 {
		t.Fatal("successful expiry kept live deadlines")
	}
}

func TestIdleHistoryExpiryExhaustsAttemptBudget(t *testing.T) {
	f := setup(t, true)
	f.s.cfg.MaxAttempts = 1
	id := f.start(t, true)
	f.lease(t, "cpu1")
	f.now = f.now.Add(time.Second)
	if err := f.s.Tick(f.ctx); err != nil {
		t.Fatal(err)
	}
	e := f.s.Export().execution(id)
	if e.State != "InfraFailed" || e.CurrentEpoch != 1 || e.StatusCode != int32(codes.Unavailable) {
		t.Fatalf("attempt budget bypassed: %+v", e)
	}
}

// Non-cacheable committed history is safe to skip only after error conditions
// that publishLocked exposes have been preserved.
func TestIdleHistoryPublishPendingErrorBoundaries(t *testing.T) {
	for _, name := range []string{"ordinary_noncacheable", "missing_winner", "wrong_epoch", "duplicate_winner_id", "failed_scheduler", "malformed_result"} {
		t.Run(name, func(t *testing.T) {
			f := setup(t, true)
			e := &Execution{ID: "committed", State: "Committed", CurrentEpoch: 2, WinnerAttemptID: "winner", DoNotCache: true}
			a := &Attempt{ID: "winner", ExecutionID: e.ID, Epoch: 2}
			f.s.ledger.Executions = []*Execution{e}
			f.s.ledger.Attempts = []*Attempt{a}
			want := codes.OK
			switch name {
			case "missing_winner":
				f.s.ledger.Attempts = nil
				want = codes.DataLoss
			case "wrong_epoch":
				a.Epoch = 1
				want = codes.DataLoss
			case "duplicate_winner_id":
				a.Epoch = 1
				f.s.ledger.Attempts = append(f.s.ledger.Attempts, &Attempt{ID: a.ID, ExecutionID: e.ID, Epoch: 2})
				want = codes.DataLoss
			case "failed_scheduler":
				f.s.failed = errors.New("durable ownership lost")
				want = codes.Unavailable
			case "malformed_result":
				e.DoNotCache = false
				e.Result = []byte{0xff}
				want = codes.Unknown
			}
			f.s.reindex() // the ledger was edited directly
			err := f.s.PublishPending(f.ctx)
			if status.Code(err) != want || f.c.writes != 0 || e.State != "Committed" {
				t.Fatalf("PublishPending = %v, writes=%d, state=%s", err, f.c.writes, e.State)
			}
		})
	}
}

// One committed result whose blobs were evicted must not hold back the
// recovery publication of the results committed after it.
func TestPublishPendingContinuesPastAnEvictedResult(t *testing.T) {
	f := setup(t, false)
	f.c.updateError = status.Error(codes.Unavailable, "action cache unavailable")
	var ids []string
	var outputs []*re.Digest
	for i := range 2 {
		id := f.start(t, true)
		lease := f.lease(t, "cpu1")
		f.running(t, lease, "cpu1")
		output := f.c.put([]byte(fmt.Sprintf("output %d", i)))
		result := &re.ActionResult{OutputFiles: []*re.OutputFile{{Path: "out", Digest: output}}}
		response, err := f.s.Complete(f.ctx, completion(lease, "cpu1", result))
		if err != nil || response.Decision != w.CompleteResponse_ACCEPTED {
			t.Fatalf("complete %d: %v %v", i, response, err)
		}
		ids, outputs = append(ids, id), append(outputs, output)
	}
	f.c.updateError = nil
	delete(f.c.blobs, outputs[0].Hash) // the older result's output was evicted

	err := f.s.PublishPending(f.ctx)
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), ids[0]) || strings.Contains(err.Error(), ids[1]) {
		t.Fatalf("PublishPending = %v, want only %s reported", err, ids[0])
	}
	ledger := f.s.Export()
	if older, newer := ledger.execution(ids[0]).State, ledger.execution(ids[1]).State; older != "Committed" || newer != "Published" || f.c.writes != 1 {
		t.Fatalf("states %s, %s with %d cache writes; want Committed, Published, 1", older, newer, f.c.writes)
	}

	// Once the blob is back, the next pass publishes the older result too.
	f.c.put([]byte("output 0"))
	if err := f.s.PublishPending(f.ctx); err != nil {
		t.Fatal(err)
	}
	if state := f.s.Export().execution(ids[0]).State; state != "Published" || f.c.writes != 2 {
		t.Fatalf("older result %s after recovery, %d cache writes", state, f.c.writes)
	}
}

func TestIdleHistoryPublicationCommitFailureStopsRetry(t *testing.T) {
	f := setup(t, false)
	id := f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	f.c.onWrite = func() { f.store.SaveError = ErrStoreUnavailable }
	response, err := f.s.Complete(f.ctx, completion(lease, "cpu1", &re.ActionResult{}))
	if err != nil || response.Decision != w.CompleteResponse_ACCEPTED {
		t.Fatalf("durable winner was lost: %v %v", response, err)
	}
	durable, err := f.store.Load(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if durable.execution(id).State != "Committed" || f.c.writes != 1 {
		t.Fatal("failed publication transaction changed durable winner")
	}
	f.store.SaveError = nil
	if err = f.s.PublishPending(f.ctx); status.Code(err) != codes.Unavailable || f.c.writes != 1 {
		t.Fatalf("uncertain owner repeated AC publication: %v writes=%d", err, f.c.writes)
	}
}
