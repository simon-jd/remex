package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	w "github.com/simon-jd/remex/api/worker/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A second action for a second worker, so two attempts are live at once.
func (f *fixture) otherAction(t *testing.T) string {
	t.Helper()
	digest := f.c.putProto(&re.Action{CommandDigest: f.c.putProto(&re.Command{Arguments: []string{"/bin/echo", "other"}}), InputRootDigest: f.c.putProto(&re.Directory{}), Platform: &re.Platform{Properties: []*re.Platform_Property{{Name: "pool", Value: "cpu"}}}})
	id, err := f.s.Start(f.ctx, &re.ExecuteRequest{ActionDigest: digest, SkipCacheLookup: true})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestActionCacheWriteDoesNotBlockHeartbeats(t *testing.T) {
	f := setup(t, false)
	f.register(t, "cpu2", map[string]string{"pool": "cpu"})
	id := f.start(t, true)
	first := f.lease(t, "cpu1")
	f.running(t, first, "cpu1")
	f.otherAction(t)
	second := f.lease(t, "cpu2")

	entered, release := make(chan struct{}), make(chan struct{})
	f.c.onWrite = func() { close(entered); <-release }
	done := make(chan error, 1)
	go func() {
		_, err := f.s.Complete(f.ctx, completion(first, "cpu1", &re.ActionResult{}))
		done <- err
	}()
	<-entered // Complete has committed and is now inside UpdateActionResult
	heartbeat := make(chan error, 1)
	go func() {
		r, err := f.s.Heartbeat(f.ctx, &w.HeartbeatRequest{WorkerId: "cpu2", AttemptId: second.AttemptId, Epoch: second.Epoch, Phase: w.HeartbeatRequest_RUNNING})
		if err == nil && r.Decision != w.HeartbeatResponse_CONTINUE {
			err = fmt.Errorf("decision %v", r.Decision)
		}
		heartbeat <- err
	}()
	select {
	case err := <-heartbeat:
		if err != nil {
			t.Fatalf("heartbeat during a stalled AC write: %v", err)
		}
	case <-time.After(time.Second):
		close(release)
		t.Fatal("heartbeat blocked behind an action-cache write")
	}
	if got := f.s.Export().execution(id).State; got != "Committed" {
		t.Fatalf("state while publishing = %s, want Committed", got)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := f.s.Export().execution(id).State; got != "Published" {
		t.Fatalf("state after publishing = %s, want Published", got)
	}
}

// Group commit applies a change in memory before it is durable, so a failed
// write cannot be undone in place: the scheduler stops, the store keeps the
// durable prefix, and a restart serves from it.
func TestFailedCommitStopsTheSchedulerAtTheDurablePrefix(t *testing.T) {
	f := setup(t, false)
	f.store.SaveError = errors.New("transient commit failure")
	if _, err := f.s.Start(f.ctx, &re.ExecuteRequest{ActionDigest: f.digest}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Start without a durable commit = %v, want Unavailable", err)
	}
	f.store.SaveError = nil
	if _, err := f.s.Start(f.ctx, &re.ExecuteRequest{ActionDigest: f.digest}); status.Code(err) != codes.Unavailable {
		t.Fatalf("Start after a failed write = %v, want Unavailable", err)
	}
	durable, err := f.store.Load(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(durable.Executions) != 0 || len(durable.Events) != 0 {
		t.Fatalf("failed write reached the store: %d executions, %d events", len(durable.Executions), len(durable.Events))
	}
	f.restart(t)
	id := f.start(t, true)
	lease := f.lease(t, "cpu1")
	if lease.ExecutionId != id {
		t.Fatalf("leased %s, want %s", lease.ExecutionId, id)
	}
}

func TestRestartKeepsActionsQueuedUntilWorkersReturn(t *testing.T) {
	f := setup(t, false)
	f.start(t, true) // durable state from before the restart
	restarted, err := New(f.ctx, f.store, f.c, Config{LeaseDuration: time.Second, RecoveryGrace: 2 * time.Second, Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatal(err)
	}
	f.s = restarted
	cpu := f.otherAction(t)
	gpuDigest := f.c.putProto(&re.Action{CommandDigest: f.c.putProto(&re.Command{Arguments: []string{"/bin/gpu"}}), InputRootDigest: f.c.putProto(&re.Directory{}), Platform: &re.Platform{Properties: []*re.Platform_Property{{Name: "pool", Value: "gpu"}}}})
	gpu, err := f.s.Start(f.ctx, &re.ExecuteRequest{ActionDigest: gpuDigest, SkipCacheLookup: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{cpu, gpu} {
		if got := f.s.Export().execution(id).State; got != "Queued" {
			t.Fatalf("%s submitted before any worker re-registered: %s, want Queued", id, got)
		}
	}
	f.register(t, "cpu1", map[string]string{"pool": "cpu"})
	f.lease(t, "cpu1")
	f.now = f.now.Add(3 * time.Second) // the grace is over; no GPU worker came back
	f.register(t, "cpu1", map[string]string{"pool": "cpu"})
	if err = f.s.Tick(f.ctx); err != nil {
		t.Fatal(err)
	}
	if got := f.s.Export().execution(gpu).State; got != "Unschedulable" {
		t.Fatalf("unmatched action after the grace: %s, want Unschedulable", got)
	}
}

// Records how many rows each commit writes.
type countingStore struct {
	Store
	rows []int
}

func (c *countingStore) Save(ctx context.Context, change *Change) error {
	c.rows = append(c.rows, len(change.Executions)+len(change.Attempts))
	return c.Store.Save(ctx, change)
}

func TestCommitWritesOnlyTheRowsItTouches(t *testing.T) {
	store := &countingStore{Store: NewMemoryStore()}
	history := NewLedger()
	for i := range 5000 {
		e := &Execution{ID: fmt.Sprintf("executions/old-%d", i), State: "Published", CurrentEpoch: 1, WinnerAttemptID: fmt.Sprintf("attempt-old-%d", i)}
		history.Executions = append(history.Executions, e)
		history.Attempts = append(history.Attempts, &Attempt{ID: e.WinnerAttemptID, ExecutionID: e.ID, Epoch: 1, Phase: "Committed"})
	}
	if err := store.Store.Save(context.Background(), &Change{Executions: history.Executions, Attempts: history.Attempts}); err != nil {
		t.Fatal(err)
	}
	f := &fixture{c: newCache(), now: time.Now(), ctx: context.Background()}
	var err error
	f.s, err = New(f.ctx, store, f.c, Config{LeaseDuration: time.Second, Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.s.Close)
	f.digest = f.c.putProto(&re.Action{CommandDigest: f.c.putProto(&re.Command{Arguments: []string{"/bin/true"}}), InputRootDigest: f.c.putProto(&re.Directory{}), Platform: &re.Platform{Properties: []*re.Platform_Property{{Name: "pool", Value: "cpu"}}}})
	f.register(t, "cpu1", map[string]string{"pool": "cpu"})
	store.rows = nil
	f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	if _, err = f.s.Complete(f.ctx, completion(lease, "cpu1", &re.ActionResult{})); err != nil {
		t.Fatal(err)
	}
	for i, rows := range store.rows {
		if rows > 2 {
			t.Fatalf("commit %d wrote %d rows over a 5,000-execution history; want at most the execution and its attempt", i, rows)
		}
	}
	if len(store.rows) != 5 { // created, leased, running, committed, published
		t.Fatalf("%d commits, want 5", len(store.rows))
	}
}
