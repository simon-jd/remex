// cachegate is a test-only real gRPC proxy that pauses one AC publication at a
// selected observable boundary. It never serves a simulated CAS or AC result.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type proxy struct {
	re.UnimplementedContentAddressableStorageServer
	re.UnimplementedActionCacheServer
	cas                      re.ContentAddressableStorageClient
	ac                       re.ActionCacheClient
	gate, observed, position string
	first                    atomic.Bool
	mu                       sync.Mutex
	log                      *os.File
}

func forwarded(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	return metadata.NewOutgoingContext(ctx, md.Copy())
}
func (p *proxy) record(kind string, req *re.UpdateActionResultRequest) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = json.NewEncoder(p.log).Encode(map[string]any{"kind": kind, "time": time.Now().UTC(), "instance": req.InstanceName, "action_digest": req.ActionDigest, "result": req.ActionResult})
}
func (p *proxy) pause(ctx context.Context, req *re.UpdateActionResultRequest) error {
	p.record("gate_"+p.position, req)
	data, _ := json.Marshal(map[string]any{"position": p.position, "action_digest": req.ActionDigest, "instance": req.InstanceName, "time": time.Now().UTC()})
	if err := os.WriteFile(p.observed, data, 0600); err != nil {
		return err
	}
	for {
		if _, err := os.Stat(p.gate); os.IsNotExist(err) {
			return nil
		}
		select {
		case <-ctx.Done():
			p.record("gate_cancelled", req)
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}
func (p *proxy) BatchReadBlobs(ctx context.Context, req *re.BatchReadBlobsRequest) (*re.BatchReadBlobsResponse, error) {
	return p.cas.BatchReadBlobs(forwarded(ctx), req)
}
func (p *proxy) FindMissingBlobs(ctx context.Context, req *re.FindMissingBlobsRequest) (*re.FindMissingBlobsResponse, error) {
	return p.cas.FindMissingBlobs(forwarded(ctx), req)
}
func (p *proxy) GetActionResult(ctx context.Context, req *re.GetActionResultRequest) (*re.ActionResult, error) {
	return p.ac.GetActionResult(forwarded(ctx), req)
}
func (p *proxy) UpdateActionResult(ctx context.Context, req *re.UpdateActionResultRequest) (*re.ActionResult, error) {
	first := p.first.CompareAndSwap(false, true)
	p.record("ac_request", req)
	if first && p.position == "before" {
		if err := p.pause(ctx, req); err != nil {
			return nil, err
		}
	}
	response, err := p.ac.UpdateActionResult(forwarded(ctx), req)
	if err != nil {
		p.record("upstream_error", req)
		return nil, err
	}
	p.record("upstream_write_succeeded", req)
	if first && p.position == "after" {
		if err := p.pause(ctx, req); err != nil {
			return nil, err
		}
	}
	return response, nil
}
func main() {
	listen := flag.String("listen", "", "test proxy address")
	upstream := flag.String("upstream", "", "actual bazel-remote address")
	gate := flag.String("gate", "", "pause while this file exists")
	observed := flag.String("observed", "", "write when selected AC boundary is reached")
	position := flag.String("position", "before", "before or after upstream AC write")
	logPath := flag.String("log", "", "original proxy observations JSONL")
	flag.Parse()
	if *position != "before" && *position != "after" {
		panic("position must be before or after")
	}
	conn, err := grpc.NewClient(*upstream, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	log, err := os.Create(*logPath)
	if err != nil {
		panic(err)
	}
	defer log.Close()
	p := &proxy{cas: re.NewContentAddressableStorageClient(conn), ac: re.NewActionCacheClient(conn), gate: *gate, observed: *observed, position: *position, log: log}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(64 << 20))
	re.RegisterContentAddressableStorageServer(server, p)
	re.RegisterActionCacheServer(server, p)
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		panic(err)
	}
	if err := server.Serve(listener); err != nil {
		panic(err)
	}
}
