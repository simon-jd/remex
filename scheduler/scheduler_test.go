package scheduler

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	w "github.com/simon-jd/remex/api/worker/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type fakeCache struct {
	mu            sync.Mutex
	blobs         map[string][]byte
	ac            map[string]*re.ActionResult
	writes        int
	updateError   error
	beforeMissing func()
	onWrite       func()
}

func newCache() *fakeCache {
	return &fakeCache{blobs: map[string][]byte{}, ac: map[string]*re.ActionResult{}}
}
func key(instance string, d *re.Digest) string { return instance + ":" + d.Hash }
func (c *fakeCache) put(b []byte) *re.Digest {
	s := sha256.Sum256(b)
	d := &re.Digest{Hash: fmt.Sprintf("%x", s), SizeBytes: int64(len(b))}
	c.blobs[d.Hash] = b
	return d
}
func (c *fakeCache) putProto(m proto.Message) *re.Digest { b, _ := proto.Marshal(m); return c.put(b) }
func (c *fakeCache) ReadBlob(_ context.Context, _ string, d *re.Digest) ([]byte, error) {
	b, ok := c.blobs[d.Hash]
	if !ok {
		return nil, status.Error(codes.NotFound, "missing")
	}
	return b, nil
}
func (c *fakeCache) Missing(_ context.Context, _ string, d []*re.Digest) ([]*re.Digest, error) {
	if c.beforeMissing != nil {
		c.beforeMissing()
	}
	var missing []*re.Digest
	for _, v := range d {
		if _, ok := c.blobs[v.Hash]; !ok {
			missing = append(missing, v)
		}
	}
	return missing, nil
}
func (c *fakeCache) GetActionResult(_ context.Context, instance string, d *re.Digest) (*re.ActionResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.ac[key(instance, d)]
	if !ok {
		return nil, status.Error(codes.NotFound, "missing")
	}
	return proto.Clone(r).(*re.ActionResult), nil
}
func (c *fakeCache) UpdateActionResult(_ context.Context, instance string, d *re.Digest, r *re.ActionResult) error {
	if c.onWrite != nil {
		c.onWrite()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.updateError != nil {
		return c.updateError
	}
	c.writes++
	c.ac[key(instance, d)] = proto.Clone(r).(*re.ActionResult)
	return nil
}

type fixture struct {
	s      *Scheduler
	c      *fakeCache
	store  *MemoryStore
	now    time.Time
	ctx    context.Context
	digest *re.Digest
}

func setup(t *testing.T, noCache bool) *fixture {
	t.Helper()
	store := NewMemoryStore()
	return setupWith(t, noCache, store, store)
}

// setupWith builds a fixture whose scheduler persists through store, which
// may wrap inner.
func setupWith(t *testing.T, noCache bool, store Store, inner *MemoryStore) *fixture {
	t.Helper()
	f := &fixture{c: newCache(), store: inner, now: time.Now(), ctx: context.Background()}
	var err error
	f.s, err = New(
		f.ctx,
		store,
		f.c,
		Config{LeaseDuration: time.Second, RecoveryGrace: 2 * time.Second, Now: func() time.Time { return f.now }},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.s.Close)
	f.digest = f.c.putProto(&re.Action{
		CommandDigest:   f.c.putProto(&re.Command{Arguments: []string{"/bin/true"}}),
		InputRootDigest: f.c.putProto(&re.Directory{}),
		Platform:        &re.Platform{Properties: []*re.Platform_Property{{Name: "pool", Value: "cpu"}}},
		DoNotCache:      noCache,
	})
	f.register(t, "cpu1", map[string]string{"pool": "cpu"})
	return f
}

// restart replaces the scheduler with one loaded from the same store, as a
// supervisor does after a failure, and registers cpu1 again.
func (f *fixture) restart(t *testing.T) {
	t.Helper()
	s, err := New(
		f.ctx,
		f.store,
		f.c,
		Config{LeaseDuration: time.Second, RecoveryGrace: 2 * time.Second, Now: func() time.Time { return f.now }},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	f.s = s
	f.register(t, "cpu1", map[string]string{"pool": "cpu"})
}
func (f *fixture) register(t *testing.T, id string, p map[string]string) {
	t.Helper()
	if _, err := f.s.Register(f.ctx, &w.RegisterRequest{WorkerId: id, Properties: p}); err != nil {
		t.Fatal(err)
	}
}
func (f *fixture) start(t *testing.T, skip bool) string {
	t.Helper()
	id, err := f.s.Start(f.ctx, &re.ExecuteRequest{ActionDigest: f.digest, SkipCacheLookup: skip})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func (f *fixture) lease(t *testing.T, id string) *w.LeaseResponse {
	t.Helper()
	r, err := f.s.Lease(f.ctx, &w.LeaseRequest{WorkerId: id, Properties: map[string]string{"pool": "cpu"}})
	if err != nil || !r.Available {
		t.Fatalf("lease = %v, %v", r, err)
	}
	return r
}
func (f *fixture) running(t *testing.T, lease *w.LeaseResponse, worker string) {
	t.Helper()
	r, err := f.s.Heartbeat(
		f.ctx,
		&w.HeartbeatRequest{
			WorkerId:  worker,
			AttemptId: lease.AttemptId,
			Epoch:     lease.Epoch,
			Phase:     w.HeartbeatRequest_RUNNING,
		},
	)
	if err != nil || r.Decision != w.HeartbeatResponse_CONTINUE {
		t.Fatalf("heartbeat %v %v", r, err)
	}
}
func completion(l *w.LeaseResponse, worker string, result *re.ActionResult) *w.CompleteRequest {
	b, _ := proto.Marshal(result)
	if len(b) == 0 {
		b = []byte{0x20, 0}
	}
	return &w.CompleteRequest{
		WorkerId:     worker,
		AttemptId:    l.AttemptId,
		Epoch:        l.Epoch,
		ActionResult: b,
		CgroupEmpty:  true,
		AttemptDir:   "/attempts/" + l.AttemptId,
	}
}
func TestCommitOrderingDedupAndCachedResult(t *testing.T) {
	f := setup(t, false)
	id := f.start(t, false)
	if id2 := f.start(t, false); id != id2 {
		t.Fatal("cacheable inflight request not deduplicated")
	}
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	out := f.c.put([]byte("hello"))
	r := completion(lease, "cpu1", &re.ActionResult{OutputFiles: []*re.OutputFile{{Path: "result", Digest: out}}})
	f.c.onWrite = func() {
		l, _ := f.store.Load(f.ctx)
		if l.execution(id).WinnerAttemptID != lease.AttemptId {
			t.Error("AC write before durable commit")
		}
	}
	resp, err := f.s.Complete(f.ctx, r)
	if err != nil || resp.Decision != w.CompleteResponse_ACCEPTED {
		t.Fatalf("complete %v %v", resp, err)
	}
	resp, err = f.s.Complete(f.ctx, r)
	if err != nil || resp.Decision != w.CompleteResponse_ACCEPTED || f.c.writes != 1 {
		t.Fatalf("idempotent completion %v %v writes=%d", resp, err, f.c.writes)
	}
	last := map[string]int64{}
	for _, ev := range f.s.Export().Events {
		last[ev.Kind] = ev.Sequence
	}
	if !(last["cas_verified"] < last["attempt_committed"] && last["attempt_committed"] < last["ac_published"]) {
		t.Fatal(last)
	}
	cachedID := f.start(t, false)
	cached := f.s.Export().execution(cachedID)
	if !cached.CachedResult {
		t.Fatal("AC was not reused")
	}
}
func TestLeaseFencingAndRecovery(t *testing.T) {
	f := setup(t, false)
	id := f.start(t, true)
	old := f.lease(t, "cpu1")
	f.running(t, old, "cpu1")
	f.now = f.now.Add(2 * time.Second)
	f.register(t, "cpu2", map[string]string{"pool": "cpu"})
	current := f.lease(t, "cpu2")
	if current.Epoch != old.Epoch+1 {
		t.Fatal("epoch not advanced")
	}
	r, err := f.s.Complete(f.ctx, completion(old, "cpu1", &re.ActionResult{}))
	if err != nil || r.Decision != w.CompleteResponse_FENCED {
		t.Fatalf("old complete %v %v", r, err)
	}
	f.running(t, current, "cpu2")
	f.c.updateError = status.Error(codes.Unavailable, "injected AC outage")
	r, err = f.s.Complete(f.ctx, completion(current, "cpu2", &re.ActionResult{}))
	if err != nil || r.Decision != w.CompleteResponse_ACCEPTED {
		t.Fatalf("complete %v %v", r, err)
	}
	if f.s.Export().execution(id).State != "Committed" {
		t.Fatal("lost durable commit")
	}
	f.c.updateError = nil
	recovered, err := New(f.ctx, f.store, f.c, Config{Now: func() time.Time { return f.now }})
	if err != nil {
		t.Fatal(err)
	}
	if err = recovered.Tick(f.ctx); err != nil {
		t.Fatal(err)
	}
	if recovered.Export().execution(id).State != "Published" || f.c.writes != 1 {
		t.Fatal("pending AC publication not recovered")
	}
}
func TestCASValidationDoesNotBypassFencing(t *testing.T) {
	f := setup(t, false)
	f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	f.c.beforeMissing = func() { f.now = f.now.Add(2 * time.Second) }
	r, err := f.s.Complete(f.ctx, completion(lease, "cpu1", &re.ActionResult{}))
	if err != nil || r.Decision != w.CompleteResponse_FENCED || f.c.writes != 0 {
		t.Fatalf("late CAS validation %v %v", r, err)
	}
}
func TestCachePolicy(t *testing.T) {
	for _, tc := range []struct {
		name        string
		noCache, hw bool
		exit        int32
		code        int32
	}{{"do_not_cache", true, false, 0, 0}, {"hardware", false, true, 0, 0}, {"test_failure", false, false, 1, 0}, {"timeout", false, false, 0, int32(codes.DeadlineExceeded)}} {
		t.Run(tc.name, func(t *testing.T) {
			f := setup(t, tc.noCache)
			if tc.hw {
				var action re.Action
				b, _ := f.c.ReadBlob(f.ctx, "", f.digest)
				_ = proto.Unmarshal(b, &action)
				action.Platform.Properties = append(
					action.Platform.Properties,
					&re.Platform_Property{Name: "remex-hw", Value: "true"},
				)
				f.digest = f.c.putProto(&action)
			}
			f.start(t, true)
			lease := f.lease(t, "cpu1")
			f.running(t, lease, "cpu1")
			r := completion(lease, "cpu1", &re.ActionResult{ExitCode: tc.exit})
			r.StatusCode = tc.code
			resp, err := f.s.Complete(f.ctx, r)
			if err != nil || resp.Decision != w.CompleteResponse_ACCEPTED || f.c.writes != 0 {
				t.Fatalf("policy violation %v %v writes=%d", resp, err, f.c.writes)
			}
		})
	}
}
func TestTreeDescendantMissingBlocksCommit(t *testing.T) {
	f := setup(t, false)
	f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	file := f.c.put([]byte("content"))
	tree := f.c.putProto(&re.Tree{Root: &re.Directory{Files: []*re.FileNode{{Name: "child", Digest: file}}}})
	delete(f.c.blobs, file.Hash)
	resp, err := f.s.Complete(
		f.ctx,
		completion(
			lease,
			"cpu1",
			&re.ActionResult{OutputDirectories: []*re.OutputDirectory{{Path: "out", TreeDigest: tree}}},
		),
	)
	if err != nil || resp.Decision != w.CompleteResponse_REJECTED || f.c.writes != 0 {
		t.Fatalf("missing nested blob accepted %v %v", resp, err)
	}
}
func TestLedgerFailureCannotAdvanceState(t *testing.T) {
	f := setup(t, false)
	id := f.start(t, true)
	f.store.SaveError = errors.New("injected persistence failure")
	_, err := f.s.Lease(f.ctx, &w.LeaseRequest{WorkerId: "cpu1", Properties: map[string]string{"pool": "cpu"}})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("lease without a durable commit = %v, want Unavailable", err)
	}
	f.store.SaveError = nil
	durable, err := f.store.Load(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if durable.execution(id).State != "Queued" || len(durable.Attempts) != 0 {
		t.Fatal("durable state advanced despite the failed commit")
	}
	if _, err = f.s.Lease(
		f.ctx,
		&w.LeaseRequest{WorkerId: "cpu1", Properties: map[string]string{"pool": "cpu"}},
	); status.Code(err) != codes.Unavailable {
		t.Fatalf("lease from memory ahead of the store = %v, want Unavailable", err)
	}
}
func TestUnschedulableAndProperties(t *testing.T) {
	f := setup(t, false)
	var action re.Action
	b, _ := f.c.ReadBlob(f.ctx, "", f.digest)
	_ = proto.Unmarshal(b, &action)
	action.Platform.Properties = append(
		action.Platform.Properties,
		&re.Platform_Property{Name: "image", Value: "sha256:required"},
	)
	f.digest = f.c.putProto(&action)
	id := f.start(t, true)
	e := f.s.Export().execution(id)
	if e.State != "Unschedulable" || e.StatusCode != int32(codes.FailedPrecondition) {
		t.Fatalf("%+v", e)
	}
	if Match(
		map[string]string{"cpu.min": "8", "mem_gb.min": "4"},
		map[string]string{"cpu": "4", "mem_gb": "8"},
	) == nil {
		t.Fatal("minimum ignored")
	}
	if Match(map[string]string{"gpu.cc": "9.0"}, map[string]string{"gpu.cc": "8.9"}) == nil {
		t.Fatal("GPU requirement downgraded")
	}
}
func TestCancellationAndCommandTimeout(t *testing.T) {
	f := setup(t, false)
	var action re.Action
	b, _ := f.c.ReadBlob(f.ctx, "", f.digest)
	_ = proto.Unmarshal(b, &action)
	action.Timeout = durationpb.New(50 * time.Millisecond)
	f.digest = f.c.putProto(&action)
	id := f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	f.now = f.now.Add(100 * time.Millisecond)
	r, err := f.s.Heartbeat(
		f.ctx,
		&w.HeartbeatRequest{
			WorkerId:  "cpu1",
			AttemptId: lease.AttemptId,
			Epoch:     lease.Epoch,
			Phase:     w.HeartbeatRequest_RUNNING,
		},
	)
	if err != nil || r.Decision != w.HeartbeatResponse_CONTINUE {
		t.Fatalf("timeout reporting lease lost %v %v", r, err)
	}
	if err = f.s.Cancel(f.ctx, id); err != nil {
		t.Fatal(err)
	}
	resp, err := f.s.Complete(f.ctx, completion(lease, "cpu1", &re.ActionResult{}))
	if err != nil || resp.Decision != w.CompleteResponse_FENCED {
		t.Fatalf("cancel complete %v %v", resp, err)
	}
	// The cancelled attempt's deadline is dropped once it is due, without
	// expiring anything.
	f.now = f.now.Add(f.s.cfg.LeaseDuration)
	if err = f.s.Tick(f.ctx); err != nil {
		t.Fatal(err)
	}
	if f.s.deadlines.len() != 0 || len(f.s.commandDeadlines) != 0 {
		t.Fatalf("cancelled attempt kept %d deadlines", f.s.deadlines.len())
	}
	if e := f.s.Export().execution(id); e.State != "Cancelled" || e.CurrentEpoch != 1 {
		t.Fatalf("dropping a cancelled deadline changed the execution: %+v", e)
	}
}
func TestRestartUsesFreshGraceNotWallClock(t *testing.T) {
	f := setup(t, false)
	id := f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	f.now = f.now.Add(-24 * time.Hour)
	s, err := New(
		f.ctx,
		f.store,
		f.c,
		Config{LeaseDuration: time.Second, RecoveryGrace: 2 * time.Second, Now: func() time.Time { return f.now }},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Tick(f.ctx); err != nil {
		t.Fatal(err)
	}
	if s.Export().execution(id).CurrentEpoch != 1 {
		t.Fatal("wall clock rollback expired lease")
	}
	f.now = f.now.Add(3 * time.Second)
	if err = s.Tick(f.ctx); err != nil {
		t.Fatal(err)
	}
	if s.Export().execution(id).CurrentEpoch != 2 {
		t.Fatal("recovered lease did not expire")
	}
}
func TestRealGRPCExecuteAndWaitExecution(t *testing.T) {
	f := setup(t, false)
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	re.RegisterExecutionServer(server, f.s)
	re.RegisterCapabilitiesServer(server, f.s)
	w.RegisterWorkerServiceServer(server, f.s)
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient(
		"passthrough:///remex",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	caps, err := re.NewCapabilitiesClient(conn).GetCapabilities(ctx, &re.GetCapabilitiesRequest{})
	if err != nil || !caps.ExecutionCapabilities.ExecEnabled {
		t.Fatalf("caps %v %v", caps, err)
	}
	client := re.NewExecutionClient(conn)
	stream, err := client.Execute(ctx, &re.ExecuteRequest{ActionDigest: f.digest, SkipCacheLookup: true})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := stream.Recv()
	if err != nil || queued.Done {
		t.Fatalf("queued %v %v", queued, err)
	}
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	if _, err = f.s.Complete(ctx, completion(lease, "cpu1", &re.ActionResult{})); err != nil {
		t.Fatal(err)
	}
	for {
		op, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if op.Done {
			var response re.ExecuteResponse
			if err = op.GetResponse().UnmarshalTo(&response); err != nil || response.Status.Code != 0 {
				t.Fatalf("response %v %v", &response, err)
			}
			break
		}
	}
	wait, err := client.WaitExecution(ctx, &re.WaitExecutionRequest{Name: queued.Name})
	if err != nil {
		t.Fatal(err)
	}
	done, err := wait.Recv()
	if err != nil || !done.Done {
		t.Fatalf("wait %v %v", done, err)
	}
	unknown, _ := client.WaitExecution(ctx, &re.WaitExecutionRequest{Name: "absent"})
	if _, err = unknown.Recv(); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown wait %v", err)
	}
}

func TestGPUDeviceSnapshotAndNumericProperties(t *testing.T) {
	props := map[string]string{"pool": "gpu", "gpu.cc": "8.9", "gpu.driver_major": "595"}
	if err := validWorker(
		"gpu1",
		props,
		`{"uuid":"GPU-123","compute_capability":"8.9","driver_version":"595.91.07"}`,
	); err != nil {
		t.Fatal(err)
	}
	if err := validWorker(
		"gpu1",
		props,
		`{"uuid":"GPU-123","compute_capability":"9.0","driver_version":"595.91.07"}`,
	); err == nil {
		t.Fatal("snapshot mismatch accepted")
	}
	for _, bad := range []string{"NaN", "+Inf", "-Inf", "0", "-1"} {
		if Match(map[string]string{"cpu.min": bad}, map[string]string{"cpu": "8"}) == nil {
			t.Fatalf("invalid requirement %s accepted", bad)
		}
	}
	if Match(map[string]string{"cpu.min": "2"}, map[string]string{"cpu": "NaN"}) == nil {
		t.Fatal("nonfinite actual capacity accepted")
	}
}
func TestEmptySuccessfulActionResult(t *testing.T) {
	f := setup(t, false)
	id := f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	r := completion(lease, "cpu1", &re.ActionResult{})
	r.ActionResult = nil
	response, err := f.s.Complete(f.ctx, r)
	if err != nil || response.Decision != w.CompleteResponse_ACCEPTED {
		t.Fatalf("empty result %v %v", response, err)
	}
	op, err := operation(f.s.Export().execution(id))
	if err != nil {
		t.Fatal(err)
	}
	var result re.ExecuteResponse
	if err = op.GetResponse().UnmarshalTo(&result); err != nil || result.Result == nil {
		t.Fatalf("missing valid empty result %v %v", &result, err)
	}
}

func timeoutFixture(t *testing.T) (*fixture, string, *w.LeaseResponse) {
	t.Helper()
	f := setup(t, false)
	var a re.Action
	b, _ := f.c.ReadBlob(f.ctx, "", f.digest)
	_ = proto.Unmarshal(b, &a)
	a.Timeout = durationpb.New(50 * time.Millisecond)
	f.digest = f.c.putProto(&a)
	id := f.start(t, true)
	lease := f.lease(t, "cpu1")
	f.running(t, lease, "cpu1")
	return f, id, lease
}
func TestWorkerTimeoutUploadingCommitsWithoutRetry(t *testing.T) {
	for _, code := range []int32{int32(codes.DeadlineExceeded), 0} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			f, id, lease := timeoutFixture(t)
			f.now = f.now.Add(100 * time.Millisecond)
			for _, phase := range []w.HeartbeatRequest_Phase{w.HeartbeatRequest_RUNNING, w.HeartbeatRequest_UPLOADING} {
				r, err := f.s.Heartbeat(
					f.ctx,
					&w.HeartbeatRequest{WorkerId: "cpu1", AttemptId: lease.AttemptId, Epoch: lease.Epoch, Phase: phase},
				)
				if err != nil || r.Decision != w.HeartbeatResponse_CONTINUE {
					t.Fatalf("timeout reporting lost lease: %v %v", r, err)
				}
			}
			req := completion(lease, "cpu1", &re.ActionResult{})
			req.StatusCode = code
			r, err := f.s.Complete(f.ctx, req)
			if err != nil || r.Decision != w.CompleteResponse_ACCEPTED {
				t.Fatalf("timeout complete: %v %v", r, err)
			}
			e := f.s.Export().execution(id)
			if e.StatusCode != int32(codes.DeadlineExceeded) ||
				e.CurrentEpoch != 1 ||
				e.State != "Committed" ||
				f.c.writes != 0 {
				t.Fatalf("timeout became success or retry: %+v writes=%d", e, f.c.writes)
			}
		})
	}
}
func TestTimeoutDuringCASVerificationCannotPublishSuccess(t *testing.T) {
	f, id, lease := timeoutFixture(t)
	f.c.beforeMissing = func() { f.now = f.now.Add(100 * time.Millisecond) }
	r, err := f.s.Complete(f.ctx, completion(lease, "cpu1", &re.ActionResult{}))
	if err != nil || r.Decision != w.CompleteResponse_ACCEPTED {
		t.Fatalf("complete %v %v", r, err)
	}
	if e := f.s.Export().execution(id); e.StatusCode != int32(codes.DeadlineExceeded) || f.c.writes != 0 {
		t.Fatalf("late success published: %+v", e)
	}
}
func TestInvocationMetadataJoinsInflightExecution(t *testing.T) {
	f := setup(t, false)
	start := func(invocation string) string {
		raw, _ := proto.Marshal(&re.RequestMetadata{ToolInvocationId: invocation})
		ctx := metadata.NewIncomingContext(
			f.ctx,
			metadata.Pairs("build.bazel.remote.execution.v2.requestmetadata-bin", string(raw)),
		)
		id, err := f.s.Start(ctx, &re.ExecuteRequest{ActionDigest: f.digest, SkipCacheLookup: true})
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	id := start("build-one")
	if other := start("build-two"); id != other {
		t.Fatal("not deduplicated")
	}
	start("build-one")
	e := f.s.Export().execution(id)
	if len(e.InvocationIDs) != 2 || e.InvocationIDs[0] != "build-one" || e.InvocationIDs[1] != "build-two" {
		t.Fatalf("invocations: %v", e.InvocationIDs)
	}
}
func TestFatalStoreFailureStopsFurtherOperations(t *testing.T) {
	f := setup(t, false)
	f.start(t, true)
	f.store.SaveError = fmt.Errorf("%w: lost database connection", ErrStoreUnavailable)
	_, err := f.s.Lease(f.ctx, &w.LeaseRequest{WorkerId: "cpu1", Properties: map[string]string{"pool": "cpu"}})
	if err == nil {
		t.Fatal("expected failed store")
	}
	select {
	case fatal := <-f.s.Failure():
		if !errors.Is(fatal, ErrStoreUnavailable) {
			t.Fatal(fatal)
		}
	default:
		t.Fatal("fatal failure not signaled")
	}
	f.store.SaveError = nil
	_, err = f.s.Lease(f.ctx, &w.LeaseRequest{WorkerId: "cpu1", Properties: map[string]string{"pool": "cpu"}})
	if err == nil {
		t.Fatal("scheduler continued from stale memory")
	}
}
