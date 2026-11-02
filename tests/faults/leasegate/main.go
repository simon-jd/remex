// leasegate holds leasing until the test has mutated the queued action's CAS.
// While held, it refreshes registration using the actual worker request fields.
// Once released, all traffic is transparently forwarded through remex-record.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"net"
	"os"
	"sync/atomic"
	"time"

	w "github.com/simon-jd/remex/api/worker/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type proxy struct {
	w.UnimplementedWorkerServiceServer
	client              w.WorkerServiceClient
	gate                string
	afterGate, observed string
	first               atomic.Bool
}

func (p *proxy) Register(ctx context.Context, r *w.RegisterRequest) (*w.RegisterResponse, error) {
	return p.client.Register(ctx, r)
}
func (p *proxy) Heartbeat(ctx context.Context, r *w.HeartbeatRequest) (*w.HeartbeatResponse, error) {
	return p.client.Heartbeat(ctx, r)
}
func (p *proxy) Complete(ctx context.Context, r *w.CompleteRequest) (*w.CompleteResponse, error) {
	return p.client.Complete(ctx, r)
}
func (p *proxy) Lease(ctx context.Context, r *w.LeaseRequest) (*w.LeaseResponse, error) {
	if _, err := os.Stat(p.gate); err == nil {
		_, err = p.client.Register(
			ctx,
			&w.RegisterRequest{
				WorkerId:           r.WorkerId,
				Properties:         r.Properties,
				DeviceSnapshotJson: r.DeviceSnapshotJson,
			},
		)
		return &w.LeaseResponse{}, err
	}
	response, err := p.client.Lease(ctx, r)
	if err == nil && response.Available && p.afterGate != "" && p.first.CompareAndSwap(false, true) {
		data, _ := json.Marshal(response)
		if err := os.WriteFile(p.observed, data, 0600); err != nil {
			return nil, err
		}
		for {
			if _, err := os.Stat(p.afterGate); os.IsNotExist(err) {
				break
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	return response, err
}
func main() {
	listen := flag.String("listen", "", "worker-facing held-lease address")
	target := flag.String("target", "", "recorder worker address")
	gate := flag.String("gate", "", "hold leasing while file exists")
	afterGate := flag.String("after-lease-gate", "", "hold first available Lease response after upstream persistence")
	observed := flag.String("observed", "", "save original available Lease response before holding")
	flag.Parse()
	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	server := grpc.NewServer()
	w.RegisterWorkerServiceServer(
		server,
		&proxy{client: w.NewWorkerServiceClient(conn), gate: *gate, afterGate: *afterGate, observed: *observed},
	)
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		panic(err)
	}
	if err = server.Serve(l); err != nil {
		panic(err)
	}
}
