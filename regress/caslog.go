package regress

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// readRemoteTestLog fetches the exact uncompressed SHA256 resource named by BEP.
// A missing, truncated, oversized or corrupt log cannot supply failure evidence.
func readRemoteTestLog(ctx context.Context, uri *url.URL, dialOptions ...grpc.DialOption) ([]byte, error) {
	if uri.Scheme != "bytestream" || uri.Host == "" || uri.User != nil || uri.RawQuery != "" || uri.Fragment != "" {
		return nil, fmt.Errorf("unsupported test log URI")
	}
	resource := strings.TrimPrefix(uri.Path, "/")
	parts := strings.Split(resource, "/")
	if len(parts) < 3 || parts[len(parts)-3] != "blobs" {
		return nil, fmt.Errorf("test log must name an uncompressed blob")
	}
	hash := parts[len(parts)-2]
	decoded, err := hex.DecodeString(hash)
	if err != nil || len(decoded) != sha256.Size {
		return nil, fmt.Errorf("invalid test log SHA256")
	}
	size, err := strconv.ParseInt(parts[len(parts)-1], 10, 64)
	if err != nil || size < 0 || size > 16<<20 {
		return nil, fmt.Errorf("test log size is outside 0..16 MiB")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	options := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, dialOptions...)
	conn, err := grpc.NewClient(uri.Host, options...)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	stream, err := bs.NewByteStreamClient(conn).Read(ctx, &bs.ReadRequest{ResourceName: resource})
	if err != nil {
		return nil, err
	}
	data := make([]byte, 0, int(size))
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if int64(len(data))+int64(len(chunk.Data)) > size {
			return nil, fmt.Errorf("test log exceeds declared size")
		}
		data = append(data, chunk.Data...)
	}
	actual := sha256.Sum256(data)
	if int64(len(data)) != size || !strings.EqualFold(hex.EncodeToString(actual[:]), hash) {
		return nil, fmt.Errorf("test log digest mismatch")
	}
	return data, nil
}
