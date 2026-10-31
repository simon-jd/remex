// uploadgate forwards actual worker CAS traffic, pausing the response of one
// already persisted final stderr upload before the worker can call Complete.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type proxy struct {
	re.UnimplementedContentAddressableStorageServer
	bs.UnimplementedByteStreamServer
	cas              re.ContentAddressableStorageClient
	stream           bs.ByteStreamClient
	digest, observed string
	gated            atomic.Bool
}

func (p *proxy) FindMissingBlobs(ctx context.Context, req *re.FindMissingBlobsRequest) (*re.FindMissingBlobsResponse, error) {
	return p.cas.FindMissingBlobs(ctx, req)
}
func (p *proxy) Read(req *bs.ReadRequest, server bs.ByteStream_ReadServer) error {
	reader, err := p.stream.Read(server.Context(), req)
	if err != nil {
		return err
	}
	for {
		row, err := reader.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err = server.Send(row); err != nil {
			return err
		}
	}
}
func (p *proxy) Write(server bs.ByteStream_WriteServer) error {
	writer, err := p.stream.Write(server.Context())
	if err != nil {
		return err
	}
	resource := ""
	for {
		row, err := server.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if row.ResourceName != "" {
			resource = row.ResourceName
		}
		if err = writer.Send(row); err != nil {
			return err
		}
	}
	response, err := writer.CloseAndRecv()
	if err != nil {
		return err
	}
	if strings.Contains(resource, "/blobs/"+p.digest+"/") && p.gated.CompareAndSwap(false, true) {
		data, _ := json.Marshal(map[string]any{"resource": resource, "committed_size": response.CommittedSize, "time": time.Now().UTC(), "boundary": "actual upstream final stderr upload succeeded; response withheld before worker Complete"})
		if err := os.WriteFile(p.observed, data, 0600); err != nil {
			return err
		}
		<-server.Context().Done()
		return server.Context().Err()
	}
	return server.SendAndClose(response)
}
func main() {
	listen := flag.String("listen", "", "worker-only proxy address")
	upstream := flag.String("upstream", "", "recorder cache address")
	digest := flag.String("digest", "", "final stderr digest to pause once")
	observed := flag.String("observed", "", "original observation file")
	flag.Parse()
	conn, err := grpc.NewClient(*upstream, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	p := &proxy{cas: re.NewContentAddressableStorageClient(conn), stream: bs.NewByteStreamClient(conn), digest: *digest, observed: *observed}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(64 << 20))
	re.RegisterContentAddressableStorageServer(server, p)
	bs.RegisterByteStreamServer(server, p)
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		panic(err)
	}
	if err = server.Serve(l); err != nil {
		panic(err)
	}
}
