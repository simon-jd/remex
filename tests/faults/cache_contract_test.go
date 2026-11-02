package faults_test

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Only an explicitly isolated 32 MiB cache is suitable for this destructive test.
func TestIsolatedCacheUploadAndDiskFull(t *testing.T) {
	endpoint := os.Getenv("REMEX_TEST_ISOLATED_SMALL_CAS")
	if endpoint == "" {
		t.Skip("REMEX_TEST_ISOLATED_SMALL_CAS must name an isolated 32 MiB tmpfs cache")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cas := re.NewContentAddressableStorageClient(conn)
	streamClient := bs.NewByteStreamClient(conn)
	if _, err := re.NewCapabilitiesClient(conn).GetCapabilities(ctx, &re.GetCapabilitiesRequest{}); err != nil {
		t.Fatalf("isolated cache must be healthy before injecting upload faults: %v", err)
	}
	t.Run("wrong-upload-digest", func(t *testing.T) {
		intended := []byte(fmt.Sprintf("expected %d", time.Now().UnixNano()))
		actual := append([]byte(nil), intended...)
		actual[0] = 'X'
		sum := sha256.Sum256(intended)
		result, err := cas.BatchUpdateBlobs(
			ctx,
			&re.BatchUpdateBlobsRequest{
				Requests: []*re.BatchUpdateBlobsRequest_Request{
					{Digest: &re.Digest{Hash: fmt.Sprintf("%x", sum), SizeBytes: int64(len(actual))}, Data: actual},
				},
				DigestFunction: re.DigestFunction_SHA256,
			},
		)
		if err != nil {
			t.Fatalf("batch mismatch must be an observed per-blob validation rejection: %v", err)
		}
		if len(result.Responses) != 1 || result.Responses[0].GetStatus().GetCode() == 0 {
			t.Fatalf("original cache accepted mismatching bytes: %v", result)
		}
		missing, err := cas.FindMissingBlobs(
			ctx,
			&re.FindMissingBlobsRequest{
				BlobDigests: []*re.Digest{{Hash: fmt.Sprintf("%x", sum), SizeBytes: int64(len(actual))}},
			},
		)
		if err != nil || len(missing.GetMissingBlobDigests()) != 1 {
			t.Fatalf("bad BatchUpdate data became visible: %v %v", missing, err)
		}
		t.Logf("mismatch response=%v error=%v", result, err)
	})
	t.Run("byte-stream-wrong-upload-digest", func(t *testing.T) {
		intended := []byte(fmt.Sprintf("expected-byte-stream-%d", time.Now().UnixNano()))
		actual := append([]byte(nil), intended...)
		actual[0] ^= 1
		sum := sha256.Sum256(intended)
		digest := &re.Digest{Hash: fmt.Sprintf("%x", sum), SizeBytes: int64(len(actual))}
		stream, err := streamClient.Write(ctx)
		if err != nil {
			t.Fatal(err)
		}
		resource := fmt.Sprintf(
			"fault/uploads/mismatch-%d/blobs/%s/%d",
			time.Now().UnixNano(),
			digest.Hash,
			digest.SizeBytes,
		)
		if err := stream.Send(&bs.WriteRequest{ResourceName: resource, Data: actual, FinishWrite: true}); err != nil {
			t.Fatal(err)
		}
		response, err := stream.CloseAndRecv()
		if status.Code(err) != codes.Internal &&
			status.Code(err) != codes.InvalidArgument &&
			status.Code(err) != codes.DataLoss {
			t.Fatalf("expected digest rejection, got %v %v", response, err)
		}
		missing, findErr := cas.FindMissingBlobs(
			ctx,
			&re.FindMissingBlobsRequest{InstanceName: "fault", BlobDigests: []*re.Digest{digest}},
		)
		if findErr != nil || len(missing.GetMissingBlobDigests()) != 1 {
			t.Fatalf("bad ByteStream data became visible: %v %v", missing, findErr)
		}
		t.Logf("ByteStream mismatch rejected: %v; claimed digest absent", err)
	})
	t.Run("action-cache-instance-isolation", func(t *testing.T) {
		ac := re.NewActionCacheClient(conn)
		sum := sha256.Sum256([]byte(fmt.Sprintf("same-key-two-instances-%d", time.Now().UnixNano())))
		key := &re.Digest{Hash: fmt.Sprintf("%x", sum), SizeBytes: 7}
		for _, instance := range []string{"fault-tenant-a", "fault-tenant-b"} {
			_, err := ac.UpdateActionResult(
				ctx,
				&re.UpdateActionResultRequest{
					InstanceName: instance,
					ActionDigest: key,
					ActionResult: &re.ActionResult{StdoutRaw: []byte(instance)},
				},
			)
			if err != nil {
				t.Fatal(err)
			}
		}
		for _, instance := range []string{"fault-tenant-a", "fault-tenant-b"} {
			result, err := ac.GetActionResult(
				ctx,
				&re.GetActionResultRequest{InstanceName: instance, ActionDigest: key, InlineStdout: true},
			)
			if err != nil || string(result.GetStdoutRaw()) != instance {
				t.Fatalf("cross-instance AC result: instance=%s result=%v err=%v", instance, result, err)
			}
		}
		t.Log("same action key retains different results in two non-empty AC instances")
	})
	t.Run("byte-stream-disk-full", func(t *testing.T) {
		data := make([]byte, 40<<20)
		if _, err := io.ReadFull(rand.Reader, data); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		digest := &re.Digest{Hash: fmt.Sprintf("%x", sum), SizeBytes: int64(len(data))}
		stream, err := streamClient.Write(ctx)
		if err != nil {
			t.Fatal(err)
		}
		resource := fmt.Sprintf("fault/uploads/%d/blobs/%s/%d", time.Now().UnixNano(), digest.Hash, digest.SizeBytes)
		var sendErr error
		for offset := 0; offset < len(data); offset += 1 << 20 {
			end := offset + (1 << 20)
			if end > len(data) {
				end = len(data)
			}
			if sendErr = stream.Send(&bs.WriteRequest{
				ResourceName: resource,
				WriteOffset:  int64(offset),
				Data:         data[offset:end],
				FinishWrite:  end == len(data),
			}); sendErr != nil {
				break
			}
		}
		response, err := stream.CloseAndRecv()
		if err == nil {
			t.Fatalf("32 MiB tmpfs unexpectedly accepted 40 MiB: %+v send=%v", response, sendErr)
		}
		if status.Code(err) != codes.Internal || !strings.Contains(
			strings.ToLower(err.Error()),
			"no space left on device",
		) {
			t.Fatalf("upload failed for a reason other than observed ENOSPC: %v", err)
		}
		missing, findErr := cas.FindMissingBlobs(
			ctx,
			&re.FindMissingBlobsRequest{
				InstanceName:   "fault",
				BlobDigests:    []*re.Digest{digest},
				DigestFunction: re.DigestFunction_SHA256,
			},
		)
		if findErr != nil || len(missing.GetMissingBlobDigests()) != 1 {
			t.Fatalf("failed write left a visible blob: %v %v", missing, findErr)
		}
		t.Logf("40 MiB upload error=%v; incomplete blob absent", err)
	})
}
