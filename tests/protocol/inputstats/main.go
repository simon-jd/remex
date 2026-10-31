// inputstats measures an actual Action's CAS input Merkle tree. Directory bytes
// are independently hash verified; file sizes are references, not downloads.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func main() {
	ledgerPath := flag.String("ledger", "", "original ledger")
	contains := flag.String("command-contains", "nvcc", "select first matching Command argument")
	cas := flag.String("cas", "127.0.0.1:19092", "CAS endpoint")
	flag.Parse()
	if err := run(*ledgerPath, *contains, *cas); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run(file, contains, endpoint string) error {
	raw, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	var ledger struct {
		Executions []struct {
			ID       string `json:"id"`
			Instance string `json:"instance_name"`
			Action   []byte `json:"action"`
			Command  []byte `json:"command"`
			Digest   struct {
				Hash string `json:"hash"`
				Size int64  `json:"size_bytes"`
			} `json:"action_digest"`
		} `json:"executions"`
	}
	if err = json.Unmarshal(raw, &ledger); err != nil {
		return err
	}
	connection, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer connection.Close()
	client := bs.NewByteStreamClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	for _, e := range ledger.Executions {
		var command re.Command
		if err = proto.Unmarshal(e.Command, &command); err != nil {
			return err
		}
		match := false
		for _, arg := range command.Arguments {
			if strings.Contains(arg, contains) {
				match = true
			}
		}
		if !match {
			continue
		}
		read := func(d *re.Digest) ([]byte, error) {
			if d == nil || d.SizeBytes < 0 || d.SizeBytes > 64<<20 || len(d.Hash) != 64 {
				return nil, fmt.Errorf("invalid or oversized metadata digest")
			}
			resource := fmt.Sprintf("%s/blobs/%s/%d", e.Instance, d.Hash, d.SizeBytes)
			resource = strings.TrimPrefix(resource, "/")
			stream, err := client.Read(ctx, &bs.ReadRequest{ResourceName: resource})
			if err != nil {
				return nil, err
			}
			var data []byte
			for {
				part, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					return nil, err
				}
				if int64(len(data)+len(part.Data)) > d.SizeBytes {
					return nil, fmt.Errorf("oversized metadata blob")
				}
				data = append(data, part.Data...)
			}
			sum := sha256.Sum256(data)
			if int64(len(data)) != d.SizeBytes || hex.EncodeToString(sum[:]) != d.Hash {
				return nil, fmt.Errorf("metadata digest mismatch")
			}
			return data, nil
		}
		actionData, err := read(&re.Digest{Hash: e.Digest.Hash, SizeBytes: e.Digest.Size})
		if err != nil {
			return err
		}
		var action re.Action
		if err = proto.Unmarshal(actionData, &action); err != nil {
			return err
		}
		commandData, err := read(action.CommandDigest)
		if err != nil {
			return err
		}
		if !bytes.Equal(commandData, e.Command) {
			return fmt.Errorf("ledger Command differs from verified CAS Command")
		}
		files := map[string]int64{}
		directories := map[string]*re.Directory{}
		directorySizes := map[string]int64{}
		directoryBytes := int64(0)
		pathFiles, pathDirs, links := 0, 0, 0
		logicalBytes := int64(0)
		var walk func(*re.Digest, int) error
		walk = func(d *re.Digest, depth int) error {
			if depth > 256 || pathDirs > 100000 {
				return fmt.Errorf("input tree traversal limit")
			}
			pathDirs++
			dir := directories[d.Hash]
			if dir != nil && directorySizes[d.Hash] != d.SizeBytes {
				return fmt.Errorf("inconsistent directory digest size")
			}
			if dir == nil {
				data, err := read(d)
				if err != nil {
					return err
				}
				dir = &re.Directory{}
				if err = proto.Unmarshal(data, dir); err != nil {
					return err
				}
				directories[d.Hash] = dir
				directorySizes[d.Hash] = d.SizeBytes
				directoryBytes += int64(len(data))
			}
			for _, f := range dir.Files {
				if f.Digest == nil || f.Digest.SizeBytes < 0 || len(f.Digest.Hash) != 64 {
					return fmt.Errorf("invalid file digest")
				}
				if _, err := hex.DecodeString(f.Digest.Hash); err != nil {
					return err
				}
				if old, exists := files[f.Digest.Hash]; exists && old != f.Digest.SizeBytes {
					return fmt.Errorf("inconsistent file digest size")
				}
				pathFiles++
				logicalBytes += f.Digest.SizeBytes
				files[f.Digest.Hash] = f.Digest.SizeBytes
			}
			links += len(dir.Symlinks)
			for _, child := range dir.Directories {
				if child.Digest == nil {
					return fmt.Errorf("missing directory digest")
				}
				if err = walk(child.Digest, depth+1); err != nil {
					return err
				}
			}
			return nil
		}
		if action.InputRootDigest == nil {
			return fmt.Errorf("missing input root")
		}
		if err = walk(action.InputRootDigest, 0); err != nil {
			return err
		}
		uniqueBytes := int64(0)
		for _, size := range files {
			uniqueBytes += size
		}
		result := map[string]any{"execution": e.ID, "action_digest": e.Digest, "input_root_digest": action.InputRootDigest, "command_arguments": command.Arguments, "file_paths": pathFiles, "directory_paths": pathDirs, "symlink_paths": links, "logical_file_bytes": logicalBytes, "unique_file_digests": len(files), "unique_file_bytes": uniqueBytes, "unique_directory_digests": len(directories), "verified_directory_bytes": directoryBytes, "scope": "Action, Command, Directory bytes verified against CAS SHA256; file sizes are declared Merkle references, not fresh file-content or availability proof."}
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(result)
	}
	return fmt.Errorf("no matching execution")
}
