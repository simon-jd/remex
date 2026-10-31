// streamprobe adds real, readable stdout/stderr stream names to operations
// forwarded from a real executor. It logs every advertised stream and Read.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	op "cloud.google.com/go/longrunning/autogen/longrunningpb"
	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/anypb"
)

type proxy struct {
	re.UnimplementedExecutionServer
	re.UnimplementedCapabilitiesServer
	bs.UnimplementedByteStreamServer
	exec  re.ExecutionClient
	cap   re.CapabilitiesClient
	cache bs.ByteStreamClient
	mu    sync.Mutex
	log   *json.Encoder
}

func (p *proxy) record(kind string, value any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_ = p.log.Encode(map[string]any{"time": time.Now().UTC(), "kind": kind, "value": value})
}
func forward(ctx context.Context) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	return metadata.NewOutgoingContext(ctx, md.Copy())
}
func (p *proxy) GetCapabilities(ctx context.Context, r *re.GetCapabilitiesRequest) (*re.ServerCapabilities, error) {
	return p.cap.GetCapabilities(forward(ctx), r)
}
func (p *proxy) send(operation *op.Operation, send func(*op.Operation) error) error {
	if !operation.Done {
		var m re.ExecuteOperationMetadata
		if operation.Metadata != nil {
			if err := operation.Metadata.UnmarshalTo(&m); err != nil {
				return err
			}
		}
		m.StdoutStreamName = "remex-live-stdout/" + operation.Name
		m.StderrStreamName = "remex-live-stderr/" + operation.Name
		operation.Metadata, _ = anypb.New(&m)
		p.record("advertised", map[string]any{"operation": operation.Name, "stdout": m.StdoutStreamName, "stderr": m.StderrStreamName, "stage": m.Stage.String()})
	}
	return send(operation)
}
func (p *proxy) Execute(r *re.ExecuteRequest, s re.Execution_ExecuteServer) error {
	stream, err := p.exec.Execute(forward(s.Context()), r)
	if err != nil {
		return err
	}
	for {
		o, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = p.send(o, s.Send); err != nil {
			return err
		}
	}
}
func (p *proxy) WaitExecution(r *re.WaitExecutionRequest, s re.Execution_WaitExecutionServer) error {
	stream, err := p.exec.WaitExecution(forward(s.Context()), r)
	if err != nil {
		return err
	}
	for {
		o, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = p.send(o, s.Send); err != nil {
			return err
		}
	}
}
func (p *proxy) Read(r *bs.ReadRequest, s bs.ByteStream_ReadServer) error {
	p.record("read", r.ResourceName)
	if strings.HasPrefix(r.ResourceName, "remex-live-") {
		return s.Send(&bs.ReadResponse{Data: []byte("remex live stream fixture\n")})
	}
	stream, err := p.cache.Read(forward(s.Context()), r)
	if err != nil {
		return err
	}
	for {
		v, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = s.Send(v); err != nil {
			return err
		}
	}
}
func main() {
	listen := flag.String("listen", "127.0.0.1:42980", "proxy endpoint")
	upstream := flag.String("executor", "127.0.0.1:18980", "real execution service")
	cas := flag.String("cas", "127.0.0.1:19092", "real CAS")
	logfile := flag.String("log", "", "JSONL observations")
	probe := flag.Bool("probe", false, "positive-control read of an advertised stream fixture")
	flag.Parse()
	if *probe {
		conn, err := grpc.NewClient(*listen, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stream, err := bs.NewByteStreamClient(conn).Read(ctx, &bs.ReadRequest{ResourceName: "remex-live-stdout/selfcheck"})
		if err != nil {
			panic(err)
		}
		value, err := stream.Recv()
		if err != nil {
			panic(err)
		}
		if string(value.Data) != "remex live stream fixture\n" {
			panic("wrong live stream payload")
		}
		_, _ = os.Stdout.Write(value.Data)
		return
	}
	f, err := os.Create(*logfile)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	dial := func(addr string) *grpc.ClientConn {
		c, e := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if e != nil {
			panic(e)
		}
		return c
	}
	e, c := dial(*upstream), dial(*cas)
	defer e.Close()
	defer c.Close()
	p := &proxy{exec: re.NewExecutionClient(e), cap: re.NewCapabilitiesClient(e), cache: bs.NewByteStreamClient(c), log: json.NewEncoder(f)}
	s := grpc.NewServer()
	re.RegisterExecutionServer(s, p)
	re.RegisterCapabilitiesServer(s, p)
	bs.RegisterByteStreamServer(s, p)
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		panic(err)
	}
	if err = s.Serve(l); err != nil {
		panic(err)
	}
}
