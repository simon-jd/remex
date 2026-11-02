package regress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

type BundleDigest struct {
	Hash      string `json:"hash"`
	SizeBytes int64  `json:"size_bytes"`
}
type DirectoryProof struct {
	Path   string       `json:"path"`
	Digest BundleDigest `json:"digest"`
	Data   []byte       `json:"data"`
}
type ActionBundleSource struct {
	ExecutionID     string           `json:"execution_id"`
	AttemptID       string           `json:"attempt_id"`
	InstanceName    string           `json:"instance_name"`
	ActionDigest    BundleDigest     `json:"action_digest"`
	CommandDigest   BundleDigest     `json:"command_digest"`
	InputRootDigest BundleDigest     `json:"input_root_digest"`
	Action          []byte           `json:"action"`
	Command         []byte           `json:"command"`
	Directories     []DirectoryProof `json:"directories"`
}

// ActionBlobReader returns CAS content; the exporter independently checks every
// returned byte against its requested SHA-256 and size, including Directory nodes.
type ActionBlobReader interface {
	ReadBlob(context.Context, string, *re.Digest) ([]byte, error)
}
type byteStreamActionReader struct{ client bs.ByteStreamClient }

func (r byteStreamActionReader) ReadBlob(ctx context.Context, instance string, d *re.Digest) ([]byte, error) {
	resource := "blobs/" + d.Hash + "/" + strconv.FormatInt(d.SizeBytes, 10)
	if instance != "" {
		resource = instance + "/" + resource
	}
	stream, err := r.client.Read(ctx, &bs.ReadRequest{ResourceName: resource})
	if err != nil {
		return nil, err
	}
	var result bytes.Buffer
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if int64(result.Len())+int64(len(response.Data)) > d.SizeBytes {
			return nil, fmt.Errorf("CAS sent more than declared blob size")
		}
		result.Write(response.Data)
	}
	return result.Bytes(), nil
}

// ExportActionBundle connects to the original CAS and reconstructs only the
// immutable Action.input_root_digest tree. It never reads an attempt directory.
func ExportActionBundle(
	ctx context.Context,
	ledger io.Reader,
	executionID,
	casEndpoint,
	destination string,
	metadata Bundle,
) error {
	endpoint := strings.TrimPrefix(casEndpoint, "grpc://")
	if endpoint == "" || strings.Contains(endpoint, "://") {
		return fmt.Errorf("--cas must be a host:port or grpc:// endpoint")
	}
	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)),
	)
	if err != nil {
		return err
	}
	defer conn.Close()
	return ExportActionBundleFromCAS(
		ctx,
		ledger,
		executionID,
		destination,
		metadata,
		byteStreamActionReader{client: bs.NewByteStreamClient(conn)},
	)
}

func ExportActionBundleFromCAS(
	ctx context.Context,
	reader io.Reader,
	executionID,
	destination string,
	metadata Bundle,
	cas ActionBlobReader,
) error {
	var ledger struct {
		SchemaVersion int `json:"schema_version"`
		Executions    []struct {
			ID           string            `json:"id"`
			InstanceName string            `json:"instance_name"`
			ActionDigest BundleDigest      `json:"action_digest"`
			Action       []byte            `json:"action"`
			Command      []byte            `json:"command"`
			Platform     map[string]string `json:"platform"`
			Winner       string            `json:"winner_attempt_id"`
			Epoch        int64             `json:"current_epoch"`
		} `json:"executions"`
		Attempts []struct {
			ID             string            `json:"id"`
			ExecutionID    string            `json:"execution_id"`
			Epoch          int64             `json:"epoch"`
			WorkerID       string            `json:"worker_id"`
			WorkerPlatform map[string]string `json:"worker_platform"`
			DeviceSnapshot string            `json:"device_snapshot_json"`
			DeviceVerified bool              `json:"device_verified"`
		} `json:"attempts"`
	}
	if err := json.NewDecoder(reader).Decode(&ledger); err != nil {
		return err
	}
	if ledger.SchemaVersion != 1 {
		return fmt.Errorf("unsupported ledger schema")
	}
	index := -1
	for i, e := range ledger.Executions {
		if e.ID == executionID {
			if index != -1 {
				return fmt.Errorf("duplicate execution ID")
			}
			index = i
		}
	}
	if index < 0 {
		return fmt.Errorf("execution %q not found", executionID)
	}
	e := ledger.Executions[index]
	read := func(d *re.Digest) ([]byte, error) {
		if err := validActionDigest(d); err != nil {
			return nil, err
		}
		if d.SizeBytes > 1<<30 {
			return nil, fmt.Errorf("blob exceeds 1 GiB export limit")
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := cas.ReadBlob(ctx, e.InstanceName, d)
		if err != nil {
			return nil, err
		}
		if err := verifyActionBlob(d, data); err != nil {
			return nil, err
		}
		return data, nil
	}
	actionDigest := &re.Digest{Hash: e.ActionDigest.Hash, SizeBytes: e.ActionDigest.SizeBytes}
	rawAction, err := read(actionDigest)
	if err != nil {
		return fmt.Errorf("Action: %w", err)
	}
	if e.Action != nil && !bytes.Equal(e.Action, rawAction) {
		return fmt.Errorf("ledger Action differs from digest-verified CAS Action")
	}
	var action re.Action
	if err := proto.Unmarshal(rawAction, &action); err != nil {
		return err
	}
	rawCommand, err := read(action.CommandDigest)
	if err != nil {
		return fmt.Errorf("Command: %w", err)
	}
	if e.Command != nil && !bytes.Equal(e.Command, rawCommand) {
		return fmt.Errorf("ledger Command differs from digest-verified CAS Command")
	}
	var command re.Command
	if err := proto.Unmarshal(rawCommand, &command); err != nil {
		return err
	}
	if len(command.Arguments) == 0 {
		return fmt.Errorf("captured Command is empty")
	}
	environment := map[string]string{}
	for _, v := range command.EnvironmentVariables {
		if v.Name == "" || strings.ContainsAny(v.Name, "=\x00") || strings.ContainsRune(v.Value, 0) {
			return fmt.Errorf("invalid Command environment")
		}
		if _, duplicate := environment[v.Name]; duplicate {
			return fmt.Errorf("duplicate Command environment variable")
		}
		environment[v.Name] = v.Value
	}
	platform := map[string]string{}
	p := action.Platform
	if p == nil || len(p.Properties) == 0 {
		p = command.Platform
	}
	for _, v := range p.GetProperties() {
		if _, duplicate := platform[v.Name]; duplicate {
			return fmt.Errorf("duplicate Command/Action platform property")
		}
		platform[v.Name] = v.Value
	}
	if !maps.Equal(platform, e.Platform) {
		return fmt.Errorf("ledger platform differs from digest-verified action")
	}
	var worker map[string]any
	var gpu map[string]any
	for _, a := range ledger.Attempts {
		if a.ID != e.Winner {
			continue
		}
		if a.ExecutionID != e.ID || a.Epoch != e.Epoch {
			return fmt.Errorf("winning attempt identity/epoch mismatch")
		}
		if worker != nil {
			return fmt.Errorf("duplicate winning attempt")
		}
		worker = map[string]any{
			"worker_id":       a.WorkerID,
			"attempt_id":      a.ID,
			"epoch":           a.Epoch,
			"platform":        a.WorkerPlatform,
			"device_verified": a.DeviceVerified,
		}
		if a.DeviceSnapshot != "" && a.DeviceSnapshot != "{}" {
			if err := json.Unmarshal([]byte(a.DeviceSnapshot), &gpu); err != nil {
				return fmt.Errorf("invalid GPU snapshot: %w", err)
			}
		}
	}
	if worker == nil {
		return fmt.Errorf("execution has no recorded winning attempt; actual worker provenance is unavailable")
	}
	if platform["pool"] == "gpu" && (gpu == nil || worker["device_verified"] != true) {
		return fmt.Errorf("GPU action lacks verified device provenance")
	}
	wd := command.WorkingDirectory
	if wd == "" {
		wd = "."
	}
	// Supplementary revision/Bazel version/flags are user-supplied context. Facts
	// determined by the original Action/Command/attempt cannot be overridden.
	if metadata.Command != nil && !reflect.DeepEqual(metadata.Command, command.Arguments) {
		return fmt.Errorf("manifest command conflicts with original Command")
	}
	if metadata.Environment != nil && !maps.Equal(metadata.Environment, environment) {
		return fmt.Errorf("manifest environment conflicts with original Command")
	}
	if metadata.Platform != nil && !maps.Equal(metadata.Platform, platform) {
		return fmt.Errorf("manifest platform conflicts with original Action")
	}
	if metadata.WorkingDirectory != "" && metadata.WorkingDirectory != wd {
		return fmt.Errorf("manifest working directory conflicts with original Command")
	}
	if metadata.Worker != nil && !jsonEquivalent(metadata.Worker, worker) {
		return fmt.Errorf("manifest worker conflicts with winning attempt")
	}
	if metadata.GPU != nil && !jsonEquivalent(metadata.GPU, gpu) {
		return fmt.Errorf("manifest GPU snapshot conflicts with winning attempt")
	}
	if metadata.Source != nil {
		return fmt.Errorf("source provenance is generated from CAS and cannot be supplied by manifest")
	}
	metadata.Command = append([]string(nil), command.Arguments...)
	metadata.Environment = environment
	metadata.Platform = platform
	metadata.WorkingDirectory = wd
	metadata.Worker = worker
	metadata.GPU = gpu
	for _, key := range []string{"REMEX_REPLAY_SEED", "REMEX_SEED", "TEST_RANDOM_SEED"} {
		if raw, ok := environment[key]; ok {
			seed, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid original seed %s", key)
			}
			if metadata.Seed != 0 && metadata.Seed != seed {
				return fmt.Errorf("manifest seed conflicts with original environment")
			}
			metadata.Seed = seed
			break
		}
	}
	source := &ActionBundleSource{
		ExecutionID:     e.ID,
		AttemptID:       e.Winner,
		InstanceName:    e.InstanceName,
		ActionDigest:    e.ActionDigest,
		CommandDigest:   bundleDigest(action.CommandDigest),
		InputRootDigest: bundleDigest(action.InputRootDigest),
		Action:          rawAction,
		Command:         rawCommand,
	}
	root, err := os.MkdirTemp("", "remex-action-inputs-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	ancestors := map[BundleDigest]bool{}
	var walk func(*re.Digest, string, int) error
	walk = func(d *re.Digest, relative string, depth int) error {
		if depth > 256 {
			return fmt.Errorf("input Directory depth exceeds 256")
		}
		digest := bundleDigest(d)
		if ancestors[digest] {
			return fmt.Errorf("cyclic input Directory graph")
		}
		ancestors[digest] = true
		defer delete(ancestors, digest)
		raw, err := read(d)
		if err != nil {
			return err
		}
		var directory re.Directory
		if err := proto.Unmarshal(raw, &directory); err != nil {
			return err
		}
		source.Directories = append(source.Directories, DirectoryProof{Path: relative, Digest: digest, Data: raw})
		if err := os.MkdirAll(filepath.Join(root, filepath.FromSlash(relative)), 0755); err != nil {
			return err
		}
		used := map[string]bool{}
		name := func(n string) error {
			if !safeRelative(n) || n == "." || strings.Contains(n, "/") || used[n] {
				return fmt.Errorf("invalid or duplicate input node %q", n)
			}
			used[n] = true
			return nil
		}
		for _, file := range directory.Files {
			if err := name(file.Name); err != nil {
				return err
			}
			data, err := read(file.Digest)
			if err != nil {
				return fmt.Errorf("input %s: %w", file.Name, err)
			}
			mode := os.FileMode(0644)
			if file.IsExecutable {
				mode = 0755
			}
			if err := os.WriteFile(
				filepath.Join(root, filepath.FromSlash(relative), file.Name),
				data,
				mode,
			); err != nil {
				return err
			}
		}
		for _, sub := range directory.Directories {
			if err := name(sub.Name); err != nil {
				return err
			}
			if err := walk(sub.Digest, path.Join(relative, sub.Name), depth+1); err != nil {
				return err
			}
		}
		for _, link := range directory.Symlinks {
			if err := name(link.Name); err != nil {
				return err
			}
			if link.Target == "" || path.IsAbs(link.Target) || strings.ContainsAny(link.Target, "\\\x00\n\r") {
				return fmt.Errorf("unsafe input symlink %s", link.Name)
			}
			if err := os.Symlink(
				link.Target,
				filepath.Join(root, filepath.FromSlash(relative), link.Name),
			); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(action.InputRootDigest, ".", 0); err != nil {
		return err
	}
	metadata.Source = source
	return ExportBundle(root, destination, metadata)
}

func bundleDigest(d *re.Digest) BundleDigest {
	if d == nil {
		return BundleDigest{}
	}
	return BundleDigest{Hash: d.Hash, SizeBytes: d.SizeBytes}
}
func validActionDigest(d *re.Digest) error {
	if d == nil || len(d.Hash) != 64 || d.SizeBytes < 0 || strings.ToLower(d.Hash) != d.Hash {
		return fmt.Errorf("invalid SHA256 digest")
	}
	if _, err := hex.DecodeString(d.Hash); err != nil {
		return err
	}
	return nil
}
func verifyActionBlob(d *re.Digest, data []byte) error {
	if err := validActionDigest(d); err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	if int64(len(data)) != d.SizeBytes || hex.EncodeToString(sum[:]) != d.Hash {
		return fmt.Errorf("CAS content does not match requested SHA256/size")
	}
	return nil
}
func jsonEquivalent(a, b any) bool {
	x, err := json.Marshal(a)
	if err != nil {
		return false
	}
	y, err := json.Marshal(b)
	return err == nil && bytes.Equal(x, y)
}
