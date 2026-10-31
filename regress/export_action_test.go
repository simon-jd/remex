package regress

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	re "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"
)

type actionFixtureCAS map[string][]byte

func (c actionFixtureCAS) ReadBlob(_ context.Context, instance string, d *re.Digest) ([]byte, error) {
	if instance != "recorded-instance" {
		return nil, fmt.Errorf("wrong instance")
	}
	b, ok := c[d.Hash]
	if !ok {
		return nil, fmt.Errorf("missing blob")
	}
	return append([]byte{}, b...), nil
}
func actionExportFixture(t *testing.T) ([]byte, actionFixtureCAS, *re.Action) {
	t.Helper()
	cas := actionFixtureCAS{}
	put := func(data []byte) *re.Digest {
		sum := sha256.Sum256(data)
		d := &re.Digest{Hash: fmt.Sprintf("%x", sum), SizeBytes: int64(len(data))}
		cas[d.Hash] = data
		return d
	}
	message := func(p proto.Message) *re.Digest {
		wire, err := proto.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		return put(wire)
	}
	file := put([]byte("#!/bin/sh\nset -e\ntest \"$(readlink link)\" = input\ntest ! -e old-output\ntest -z \"${REMEX_HOST_ONLY:-}\"\ntest -z \"${REMEX_REPLAY_SEED:-}\"\ntest \"$REMEX_SEED\" = 42\ncat link\n"))
	data := put([]byte("initial-input-only"))
	root := message(&re.Directory{Files: []*re.FileNode{{Name: "run", Digest: file, IsExecutable: true}, {Name: "input", Digest: data}}, Symlinks: []*re.SymlinkNode{{Name: "link", Target: "input"}}})
	command := &re.Command{Arguments: []string{"./run"}, EnvironmentVariables: []*re.Command_EnvironmentVariable{{Name: "REMEX_SEED", Value: "42"}}}
	action := &re.Action{CommandDigest: message(command), InputRootDigest: root, Platform: &re.Platform{Properties: []*re.Platform_Property{{Name: "pool", Value: "cpu"}}}}
	ad := message(action)
	cd := action.CommandDigest
	ledger := map[string]any{"schema_version": 1, "executions": []any{map[string]any{"id": "e", "instance_name": "recorded-instance", "action_digest": bundleDigest(ad), "action": cas[ad.Hash], "command": cas[cd.Hash], "platform": map[string]string{"pool": "cpu"}, "winner_attempt_id": "a", "current_epoch": int64(1)}}, "attempts": []any{map[string]any{"id": "a", "execution_id": "e", "epoch": int64(1), "worker_id": "cpu-original", "worker_platform": map[string]string{"pool": "cpu"}}}}
	wire, err := json.Marshal(ledger)
	if err != nil {
		t.Fatal(err)
	}
	return wire, cas, action
}

func TestExportReconstructsExactInitialMerkleInputs(t *testing.T) {
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd unavailable")
	}
	ledger, cas, action := actionExportFixture(t)
	out := filepath.Join(t.TempDir(), "bundle")
	if err := ExportActionBundleFromCAS(context.Background(), bytes.NewReader(ledger), "e", out, Bundle{Revision: "explicit-revision", BazelVersion: "9.3.0"}, cas); err != nil {
		t.Fatal(err)
	}
	var manifest Bundle
	raw, err := os.ReadFile(filepath.Join(out, "bundle.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Inputs) != 3 || manifest.Source == nil || manifest.Source.InputRootDigest != bundleDigest(action.InputRootDigest) || len(manifest.Source.Directories) != 1 || manifest.Seed != 42 {
		t.Fatalf("incorrect input provenance: %+v", manifest)
	}
	// Replay after removing every source CAS byte; no source directory was used.
	clear(cas)
	replay := exec.Command(filepath.Join(out, "run.sh"))
	replay.Env = append(os.Environ(), "REMEX_HOST_ONLY=must-not-leak", "REMEX_REPLAY_SEED=999")
	result, err := replay.CombinedOutput()
	if err != nil || string(result) != "initial-input-only" {
		t.Fatalf("replay: %s %v", result, err)
	}
}

func TestExportFailsClosedOnCorruptionAndMetadataOverrides(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func([]byte, actionFixtureCAS, *re.Action) ([]byte, Bundle)
	}{
		{"corrupt-file", func(l []byte, c actionFixtureCAS, a *re.Action) ([]byte, Bundle) {
			var d re.Directory
			proto.Unmarshal(c[a.InputRootDigest.Hash], &d)
			c[d.Files[0].Digest.Hash] = []byte("altered")
			return l, Bundle{}
		}},
		{"corrupt-directory", func(l []byte, c actionFixtureCAS, a *re.Action) ([]byte, Bundle) {
			c[a.InputRootDigest.Hash] = []byte("altered")
			return l, Bundle{}
		}},
		{"false-command", func(l []byte, c actionFixtureCAS, a *re.Action) ([]byte, Bundle) {
			return l, Bundle{Command: []string{"false"}}
		}},
		{"false-environment", func(l []byte, c actionFixtureCAS, a *re.Action) ([]byte, Bundle) {
			return l, Bundle{Environment: map[string]string{"REMEX_SEED": "999"}}
		}},
		{"false-platform", func(l []byte, c actionFixtureCAS, a *re.Action) ([]byte, Bundle) {
			return l, Bundle{Platform: map[string]string{"pool": "gpu"}}
		}},
		{"false-worker", func(l []byte, c actionFixtureCAS, a *re.Action) ([]byte, Bundle) {
			return l, Bundle{Worker: map[string]any{"worker_id": "invented"}}
		}},
		{"false-source", func(l []byte, c actionFixtureCAS, a *re.Action) ([]byte, Bundle) {
			return l, Bundle{Source: &ActionBundleSource{ExecutionID: "invented"}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ledger, cas, action := actionExportFixture(t)
			ledger, b := tc.mutate(ledger, cas, action)
			out := filepath.Join(t.TempDir(), "bundle")
			if err := ExportActionBundleFromCAS(context.Background(), bytes.NewReader(ledger), "e", out, b, cas); err == nil {
				t.Fatal("unverified content or overridden provenance exported")
			}
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatal("failed export left destination")
			}
		})
	}
}
