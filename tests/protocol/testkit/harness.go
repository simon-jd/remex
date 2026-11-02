// Package testkit connects the public scheduler services to an independently
// implemented REAPI cache over gRPC. It does not access scheduler internals.
package testkit

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	w "github.com/simon-jd/remex/api/worker/v1"
	"github.com/simon-jd/remex/scheduler"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

const Instance = "independent/test"

type Cache struct {
	re.UnimplementedContentAddressableStorageServer
	re.UnimplementedActionCacheServer
	mu          sync.Mutex
	blobs       map[string][]byte
	results     map[string]*re.ActionResult
	writes      int
	writeError  bool
	missingHook func(context.Context) error
}

func (c *Cache) Put(data []byte) *re.Digest {
	sum := sha256.Sum256(data)
	d := &re.Digest{Hash: fmt.Sprintf("%x", sum), SizeBytes: int64(len(data))}
	c.mu.Lock()
	c.blobs[d.Hash] = append([]byte(nil), data...)
	c.mu.Unlock()
	return d
}
func (c *Cache) PutProto(m proto.Message) *re.Digest {
	b, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return c.Put(b)
}
func (c *Cache) Corrupt(d *re.Digest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blobs[d.Hash] = []byte("independent corruption")
}
func (c *Cache) Evict(d *re.Digest) { c.mu.Lock(); defer c.mu.Unlock(); delete(c.blobs, d.Hash) }
func (c *Cache) Writes() int        { c.mu.Lock(); defer c.mu.Unlock(); return c.writes }
func (c *Cache) FailWrites(v bool)  { c.mu.Lock(); defer c.mu.Unlock(); c.writeError = v }
func (c *Cache) OnMissing(f func(context.Context) error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.missingHook = f
}
func instance(s string) error {
	if s != Instance {
		return status.Errorf(codes.InvalidArgument, "wrong instance: %q", s)
	}
	return nil
}
func (c *Cache) BatchReadBlobs(_ context.Context, req *re.BatchReadBlobsRequest) (*re.BatchReadBlobsResponse, error) {
	if err := instance(req.InstanceName); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := &re.BatchReadBlobsResponse{}
	for _, d := range req.Digests {
		b, ok := c.blobs[d.Hash]
		r := &re.BatchReadBlobsResponse_Response{Digest: d, Data: b, Status: &rpcstatus.Status{}}
		if !ok {
			r.Status.Code = int32(codes.NotFound)
		}
		out.Responses = append(out.Responses, r)
	}
	return out, nil
}
func (c *Cache) FindMissingBlobs(
	ctx context.Context,
	req *re.FindMissingBlobsRequest,
) (*re.FindMissingBlobsResponse, error) {
	if err := instance(req.InstanceName); err != nil {
		return nil, err
	}
	c.mu.Lock()
	hook := c.missingHook
	c.mu.Unlock()
	if hook != nil {
		if err := hook(ctx); err != nil {
			return nil, err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := &re.FindMissingBlobsResponse{}
	for _, d := range req.BlobDigests {
		if b, ok := c.blobs[d.Hash]; !ok || int64(len(b)) != d.SizeBytes {
			out.MissingBlobDigests = append(out.MissingBlobDigests, d)
		}
	}
	return out, nil
}
func (c *Cache) GetActionResult(_ context.Context, req *re.GetActionResultRequest) (*re.ActionResult, error) {
	if err := instance(req.InstanceName); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r := c.results[req.ActionDigest.Hash]
	if r == nil {
		return nil, status.Error(codes.NotFound, "cache miss")
	}
	return proto.Clone(r).(*re.ActionResult), nil
}
func (c *Cache) UpdateActionResult(_ context.Context, req *re.UpdateActionResultRequest) (*re.ActionResult, error) {
	if err := instance(req.InstanceName); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeError {
		return nil, status.Error(codes.Unavailable, "independent AC outage")
	}
	c.writes++
	c.results[req.ActionDigest.Hash] = proto.Clone(req.ActionResult).(*re.ActionResult)
	return req.ActionResult, nil
}

type Harness struct {
	T            *testing.T
	Ctx          context.Context
	Scheduler    *scheduler.Scheduler
	Cache        *Cache
	Worker       w.WorkerServiceClient
	Execution    re.ExecutionClient
	Capabilities re.CapabilitiesClient
	Digest       *re.Digest
	clock        atomic.Int64
	base         time.Time
}

func New(t *testing.T, store scheduler.Store) *Harness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	h := &Harness{
		T:     t,
		Ctx:   ctx,
		base:  time.Now(),
		Cache: &Cache{blobs: map[string][]byte{}, results: map[string]*re.ActionResult{}},
	}
	cacheConn := serve(t, func(server *grpc.Server) {
		re.RegisterContentAddressableStorageServer(server, h.Cache)
		re.RegisterActionCacheServer(server, h.Cache)
	})
	if store == nil {
		store = scheduler.NewMemoryStore()
	}
	s, err := scheduler.New(
		ctx,
		store,
		scheduler.NewGRPCCache(cacheConn),
		scheduler.Config{
			LeaseDuration: time.Minute,
			Now:           func() time.Time { return h.base.Add(time.Duration(h.clock.Load())) },
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	h.Scheduler = s
	conn := serve(t, func(server *grpc.Server) {
		re.RegisterExecutionServer(server, s)
		re.RegisterCapabilitiesServer(server, s)
		w.RegisterWorkerServiceServer(server, s)
	})
	h.Worker = w.NewWorkerServiceClient(conn)
	h.Execution = re.NewExecutionClient(conn)
	h.Capabilities = re.NewCapabilitiesClient(conn)
	_, err = h.Worker.Register(
		ctx,
		&w.RegisterRequest{
			WorkerId:   "independent-worker",
			Properties: map[string]string{"pool": "cpu", "OSFamily": "Linux"},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	h.Digest = h.Cache.PutProto(&re.Action{
		CommandDigest:   h.Cache.PutProto(&re.Command{Arguments: []string{"/bin/true"}}),
		InputRootDigest: h.Cache.PutProto(&re.Directory{}),
		Platform:        &re.Platform{Properties: []*re.Platform_Property{{Name: "pool", Value: "cpu"}}},
	})
	return h
}
func serve(t *testing.T, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient(
		"passthrough:///independent",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); server.Stop(); _ = listener.Close() })
	return conn
}
func (h *Harness) Advance(d time.Duration) { h.clock.Add(int64(d)) }
func (h *Harness) Execute(ctx context.Context) re.Execution_ExecuteClient {
	h.T.Helper()
	stream, err := h.Execution.Execute(
		ctx,
		&re.ExecuteRequest{InstanceName: Instance, ActionDigest: h.Digest, SkipCacheLookup: true},
	)
	if err != nil {
		h.T.Fatal(err)
	}
	return stream
}
func (h *Harness) Start() *w.LeaseResponse {
	h.T.Helper()
	stream := h.Execute(h.Ctx)
	first, err := stream.Recv()
	if err != nil || first.Done {
		h.T.Fatalf("queued operation = %v, %v", first, err)
	}
	return h.Lease()
}
func (h *Harness) Lease() *w.LeaseResponse {
	h.T.Helper()
	lease, err := h.Worker.Lease(
		h.Ctx,
		&w.LeaseRequest{
			WorkerId:   "independent-worker",
			Properties: map[string]string{"pool": "cpu", "OSFamily": "Linux"},
		},
	)
	if err != nil || !lease.Available {
		h.T.Fatalf("lease = %v, %v", lease, err)
	}
	if lease.InstanceName != Instance {
		h.T.Fatalf("worker instance = %q", lease.InstanceName)
	}
	r, err := h.Worker.Heartbeat(
		h.Ctx,
		&w.HeartbeatRequest{
			WorkerId:  "independent-worker",
			AttemptId: lease.AttemptId,
			Epoch:     lease.Epoch,
			Phase:     w.HeartbeatRequest_RUNNING,
		},
	)
	if err != nil || r.Decision != w.HeartbeatResponse_CONTINUE {
		h.T.Fatalf("running heartbeat = %v, %v", r, err)
	}
	return lease
}
func (h *Harness) Request(lease *w.LeaseResponse, result *re.ActionResult) *w.CompleteRequest {
	h.T.Helper()
	b, err := proto.Marshal(result)
	if err != nil {
		h.T.Fatal(err)
	}
	return &w.CompleteRequest{
		WorkerId:     "independent-worker",
		AttemptId:    lease.AttemptId,
		Epoch:        lease.Epoch,
		ActionResult: b,
		CgroupEmpty:  true,
		AttemptDir:   "/independent/attempts/" + lease.AttemptId,
	}
}
func (h *Harness) Complete(req *w.CompleteRequest) *w.CompleteResponse {
	h.T.Helper()
	r, err := h.Worker.Complete(h.Ctx, req)
	if err != nil {
		h.T.Fatal(err)
	}
	return r
}
