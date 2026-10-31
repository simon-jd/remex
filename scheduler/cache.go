package scheduler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"strings"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Cache interface {
	ReadBlob(context.Context, string, *re.Digest) ([]byte, error)
	Missing(context.Context, string, []*re.Digest) ([]*re.Digest, error)
	GetActionResult(context.Context, string, *re.Digest) (*re.ActionResult, error)
	UpdateActionResult(context.Context, string, *re.Digest, *re.ActionResult) error
}
type GRPCCache struct {
	CAS re.ContentAddressableStorageClient
	AC  re.ActionCacheClient
}

func NewGRPCCache(conn grpc.ClientConnInterface) *GRPCCache {
	return &GRPCCache{CAS: re.NewContentAddressableStorageClient(conn), AC: re.NewActionCacheClient(conn)}
}
func (c *GRPCCache) ReadBlob(ctx context.Context, instance string, d *re.Digest) ([]byte, error) {
	if err := validDigest(d); err != nil {
		return nil, err
	}
	r, err := c.CAS.BatchReadBlobs(ctx, &re.BatchReadBlobsRequest{InstanceName: instance, Digests: []*re.Digest{d}, DigestFunction: re.DigestFunction_SHA256})
	if err != nil {
		return nil, err
	}
	if len(r.Responses) != 1 {
		return nil, status.Error(codes.DataLoss, "CAS did not return exactly one blob")
	}
	b := r.Responses[0]
	if b.Status != nil && b.Status.Code != 0 {
		return nil, status.Error(codes.Code(b.Status.Code), b.Status.Message)
	}
	if b.Compressor != re.Compressor_IDENTITY {
		return nil, status.Error(codes.DataLoss, "unexpected compressed CAS blob")
	}
	if err = verifyBlob(d, b.Data); err != nil {
		return nil, err
	}
	return b.Data, nil
}
func (c *GRPCCache) Missing(ctx context.Context, instance string, d []*re.Digest) ([]*re.Digest, error) {
	r, err := c.CAS.FindMissingBlobs(ctx, &re.FindMissingBlobsRequest{InstanceName: instance, BlobDigests: d, DigestFunction: re.DigestFunction_SHA256})
	if err != nil {
		return nil, err
	}
	return r.MissingBlobDigests, nil
}
func (c *GRPCCache) GetActionResult(ctx context.Context, instance string, d *re.Digest) (*re.ActionResult, error) {
	return c.AC.GetActionResult(ctx, &re.GetActionResultRequest{InstanceName: instance, ActionDigest: d, DigestFunction: re.DigestFunction_SHA256})
}
func (c *GRPCCache) UpdateActionResult(ctx context.Context, instance string, d *re.Digest, r *re.ActionResult) error {
	_, err := c.AC.UpdateActionResult(ctx, &re.UpdateActionResultRequest{InstanceName: instance, ActionDigest: d, ActionResult: r, DigestFunction: re.DigestFunction_SHA256})
	return err
}
func validDigest(d *re.Digest) error {
	if d == nil || len(d.Hash) != 64 || d.SizeBytes < 0 {
		return status.Error(codes.InvalidArgument, "invalid SHA256 digest")
	}
	b, err := hex.DecodeString(d.Hash)
	if err != nil || len(b) != 32 || strings.ToLower(d.Hash) != d.Hash {
		return status.Error(codes.InvalidArgument, "invalid SHA256 digest")
	}
	return nil
}
func verifyBlob(d *re.Digest, b []byte) error {
	s := sha256.Sum256(b)
	if int64(len(b)) != d.SizeBytes || hex.EncodeToString(s[:]) != d.Hash {
		return status.Error(codes.DataLoss, "CAS blob digest mismatch")
	}
	return nil
}
func digestPB(d Digest) *re.Digest    { return &re.Digest{Hash: d.Hash, SizeBytes: d.SizeBytes} }
func digestModel(d *re.Digest) Digest { return Digest{Hash: d.Hash, SizeBytes: d.SizeBytes} }
func safeOutput(p string) bool {
	return p != "" && !strings.HasPrefix(p, "/") && path.Clean(p) == p && p != ".." && !strings.HasPrefix(p, "../")
}

// ResultDigests walks output Trees as well as top-level output files. Checking
// only tree_digest would allow publishing an AC result whose children are gone.
func ResultDigests(ctx context.Context, c Cache, instance string, r *re.ActionResult) ([]*re.Digest, error) {
	seen := map[string]*re.Digest{}
	add := func(d *re.Digest) error {
		if err := validDigest(d); err != nil {
			return err
		}
		seen[fmt.Sprintf("%s/%d", d.Hash, d.SizeBytes)] = d
		return nil
	}
	paths := map[string]bool{}
	checkPath := func(p string, workingDirectory bool) error {
		// REAPI OutputDirectory.path="" denotes the entire working directory.
		// Files and symlinks still need a nonempty relative path.
		if (!safeOutput(p) && !(workingDirectory && p == "")) || paths[p] {
			return status.Error(codes.InvalidArgument, "invalid or repeated output path: "+p)
		}
		paths[p] = true
		return nil
	}
	for _, f := range r.OutputFiles {
		if err := checkPath(f.Path, false); err != nil {
			return nil, err
		}
		if err := add(f.Digest); err != nil {
			return nil, err
		}
	}
	for _, s := range r.OutputFileSymlinks {
		if err := checkPath(s.Path, false); err != nil {
			return nil, err
		}
	}
	for _, s := range r.OutputDirectorySymlinks {
		if err := checkPath(s.Path, false); err != nil {
			return nil, err
		}
	}
	for _, s := range r.OutputSymlinks {
		if err := checkPath(s.Path, false); err != nil {
			return nil, err
		}
	}
	for _, d := range []*re.Digest{r.StdoutDigest, r.StderrDigest} {
		if d != nil {
			if err := add(d); err != nil {
				return nil, err
			}
		}
	}
	for _, o := range r.OutputDirectories {
		if err := checkPath(o.Path, true); err != nil {
			return nil, err
		}
		if err := add(o.TreeDigest); err != nil {
			return nil, err
		}
		b, err := c.ReadBlob(ctx, instance, o.TreeDigest)
		if err != nil {
			return nil, err
		}
		if err = verifyBlob(o.TreeDigest, b); err != nil {
			return nil, err
		}
		var tree re.Tree
		if err = proto.Unmarshal(b, &tree); err != nil || tree.Root == nil {
			return nil, status.Error(codes.DataLoss, "malformed output Tree")
		}
		dirs := append([]*re.Directory{tree.Root}, tree.Children...)
		contained := map[string]bool{}
		for _, d := range tree.Children {
			b, err := proto.MarshalOptions{Deterministic: true}.Marshal(d)
			if err != nil {
				return nil, err
			}
			s := sha256.Sum256(b)
			contained[fmt.Sprintf("%x/%d", s, len(b))] = true
		}
		for _, d := range dirs {
			for _, f := range d.Files {
				if err := add(f.Digest); err != nil {
					return nil, err
				}
			}
			for _, child := range d.Directories {
				if err := validDigest(child.Digest); err != nil {
					return nil, err
				}
				if !contained[fmt.Sprintf("%s/%d", child.Digest.Hash, child.Digest.SizeBytes)] {
					return nil, status.Error(codes.DataLoss, "output Tree is missing child Directory")
				}
			}
		}
	}
	result := make([]*re.Digest, 0, len(seen))
	for _, d := range seen {
		result = append(result, d)
	}
	return result, nil
}
