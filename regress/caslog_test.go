package regress

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"net/url"
	"testing"

	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type logFixture struct {
	bs.UnimplementedByteStreamServer
	chunks  [][]byte
	missing bool
}

func (f *logFixture) Read(_ *bs.ReadRequest, stream bs.ByteStream_ReadServer) error {
	if f.missing {
		return status.Error(codes.NotFound, "evicted")
	}
	for _, chunk := range f.chunks {
		if err := stream.Send(&bs.ReadResponse{Data: chunk}); err != nil {
			return err
		}
	}
	return nil
}
func TestRemoteBEPLogEvidence(t *testing.T) {
	good := []byte("REMEX_SIGNATURE: fifo-ordering\n")
	for _, tc := range []struct {
		name    string
		fixture logFixture
		ok      bool
	}{
		{"streamed-valid", logFixture{chunks: [][]byte{good[:7], good[7:]}}, true},
		{"truncated", logFixture{chunks: [][]byte{good[:7]}}, false},
		{"oversized", logFixture{chunks: [][]byte{good, []byte("extra")}}, false},
		{"same-size-corrupt", logFixture{chunks: [][]byte{append([]byte("X"), good[1:]...)}}, false},
		{"evicted", logFixture{missing: true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			bs.RegisterByteStreamServer(server, &tc.fixture)
			go server.Serve(listener)
			defer server.Stop()
			defer listener.Close()
			uri, _ := url.Parse(fmt.Sprintf("bytestream://127.0.0.1:1/instance/blobs/%x/%d", sha256.Sum256(good), len(good)))
			data, err := readRemoteTestLog(context.Background(), uri, grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			if tc.ok && (err != nil || string(data) != string(good)) {
				t.Fatalf("lost valid log %q: %v", data, err)
			}
			if !tc.ok && err == nil {
				t.Fatal("invalid log accepted")
			}
		})
	}
}
