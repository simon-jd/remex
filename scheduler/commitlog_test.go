package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	longrunning "cloud.google.com/go/longrunning/autogen/longrunningpb"
	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	w "github.com/simon-jd/remex/api/worker/v1"
)

// gateStore holds every Save while its gate is closed, so a test can see
// what a call does before its change is durable.
type gateStore struct {
	Store
	mu      sync.Mutex
	gate    chan struct{}
	entered chan struct{} // one value per Save that reached the gate
}

func newGateStore(inner Store) *gateStore {
	return &gateStore{Store: inner, entered: make(chan struct{}, 1024)}
}
func (g *gateStore) hold() {
	g.mu.Lock()
	g.gate = make(chan struct{})
	g.mu.Unlock()
}
func (g *gateStore) open() {
	g.mu.Lock()
	if g.gate != nil {
		close(g.gate)
		g.gate = nil
	}
	g.mu.Unlock()
}
func (g *gateStore) Save(ctx context.Context, c *Change) error {
	g.mu.Lock()
	gate := g.gate
	g.mu.Unlock()
	if gate != nil {
		g.entered <- struct{}{}
		<-gate
	}
	return g.Store.Save(ctx, c)
}

func gatedSetup(t *testing.T) (*fixture, *gateStore) {
	t.Helper()
	inner := NewMemoryStore()
	g := newGateStore(inner)
	f := setupWith(t, true, g, inner)
	t.Cleanup(g.open) // runs before the scheduler's Close
	return f, g
}

func waitEntered(t *testing.T, g *gateStore) {
	t.Helper()
	select {
	case <-g.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no write reached the store")
	}
}

func TestCallsAnswerOnlyOnceTheirChangeIsDurable(t *testing.T) {
	f, g := gatedSetup(t)
	g.hold()
	done := make(chan error, 1)
	go func() {
		_, err := f.s.Start(f.ctx, &re.ExecuteRequest{ActionDigest: f.digest, SkipCacheLookup: true})
		done <- err
	}()
	waitEntered(t, g)
	select {
	case err := <-done:
		t.Fatalf("Start answered (%v) before its change was durable", err)
	case <-time.After(50 * time.Millisecond):
	}
	g.open()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	durable, err := f.store.Load(f.ctx)
	if err != nil || len(durable.Executions) != 1 {
		t.Fatalf("durable executions = %v, %v", durable, err)
	}
}

func TestConcurrentChangesShareOneWrite(t *testing.T) {
	f, g := gatedSetup(t)
	g.hold()
	first := make(chan error, 1)
	go func() {
		_, err := f.s.Start(f.ctx, &re.ExecuteRequest{ActionDigest: f.digest, SkipCacheLookup: true})
		first <- err
	}()
	waitEntered(t, g) // the first change is in flight; the rest queue behind it
	const calls = 32
	digests := make([]*re.Digest, calls) // the fake cache is not safe for concurrent writes
	for i := range digests {
		digests[i] = f.otherDigest(t)
	}
	var answered atomic.Int32
	var wg sync.WaitGroup
	errs := make(chan error, calls)
	for _, digest := range digests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.s.Start(f.ctx, &re.ExecuteRequest{ActionDigest: digest, SkipCacheLookup: true})
			answered.Add(1)
			errs <- err
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if changes, _ := f.s.CommitStats(); changes == calls+1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("calls did not append their changes")
		}
		time.Sleep(time.Millisecond)
	}
	if n := answered.Load(); n != 0 {
		t.Fatalf("%d calls answered before their changes were durable", n)
	}
	g.open()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if changes, writes := f.s.CommitStats(); changes != calls+1 || writes != 2 {
		t.Fatalf("%d changes took %d writes, want 2", changes, writes)
	}
	durable, err := f.store.Load(f.ctx)
	if err != nil || len(durable.Executions) != calls+1 {
		t.Fatalf("durable executions = %d, %v", len(durable.Executions), err)
	}
}

// otherDigest stores a distinct action so concurrent Starts do not join one
// in-flight execution.
func (f *fixture) otherDigest(t *testing.T) *re.Digest {
	t.Helper()
	return f.c.putProto(&re.Action{CommandDigest: f.c.putProto(&re.Command{Arguments: []string{"/bin/echo", newID("")}}), InputRootDigest: f.c.putProto(&re.Directory{}), Platform: &re.Platform{Properties: []*re.Platform_Property{{Name: "pool", Value: "cpu"}}}})
}

func TestStreamsReportOnlyDurableCompletion(t *testing.T) {
	f, g := gatedSetup(t)
	id := f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	ops := make(chan *longrunning.Operation, 8)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	go func() {
		_ = f.s.stream(ctx, id, func(op *longrunning.Operation) error { ops <- op; return nil })
	}()
	if op := <-ops; op.Done {
		t.Fatal("running execution reported done")
	}
	g.hold()
	completed := make(chan error, 1)
	go func() {
		_, err := f.s.Complete(f.ctx, completion(lease, "cpu1", &re.ActionResult{}))
		completed <- err
	}()
	waitEntered(t, g) // the winner is applied in memory, not yet durable
	select {
	case op := <-ops:
		t.Fatalf("stream reported %v before the winner was durable", op)
	case <-time.After(50 * time.Millisecond):
	}
	g.open()
	if op := <-ops; !op.Done {
		t.Fatal("stream did not report the durable winner")
	}
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
}

func TestContinuingHeartbeatsDoNotWaitForTheLog(t *testing.T) {
	f, g := gatedSetup(t)
	f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	other := f.otherDigest(t)
	g.hold()
	go func() { _, _ = f.s.Start(f.ctx, &re.ExecuteRequest{ActionDigest: other, SkipCacheLookup: true}) }()
	waitEntered(t, g)
	answered := make(chan *w.HeartbeatResponse, 1)
	go func() {
		response, err := f.s.Heartbeat(f.ctx, &w.HeartbeatRequest{WorkerId: "cpu1", AttemptId: lease.AttemptId, Epoch: lease.Epoch, Phase: w.HeartbeatRequest_RUNNING})
		if err != nil {
			t.Error(err)
		}
		answered <- response
	}()
	select {
	case response := <-answered:
		if response.Decision != w.HeartbeatResponse_CONTINUE {
			t.Fatalf("heartbeat decision %v", response.Decision)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a heartbeat that changes nothing waited behind another call's write")
	}
}

func TestMergeKeepsTheLastRowVersionsAndEveryEvent(t *testing.T) {
	e1, e2 := &Execution{ID: "e", State: "Queued"}, &Execution{ID: "e", State: "Leased"}
	a := &Attempt{ID: "a"}
	merged := merge([]*Change{
		{Executions: []*Execution{e1}, Events: []Event{{Sequence: 1}}},
		{Executions: []*Execution{e2}, Attempts: []*Attempt{a}, Events: []Event{{Sequence: 2}, {Sequence: 3}}},
	})
	if len(merged.Executions) != 1 || merged.Executions[0] != e2 || len(merged.Attempts) != 1 {
		t.Fatalf("merged rows = %+v %+v", merged.Executions, merged.Attempts)
	}
	if len(merged.Events) != 3 || merged.Events[0].Sequence != 1 || merged.Events[2].Sequence != 3 {
		t.Fatalf("merged events = %+v", merged.Events)
	}
}
