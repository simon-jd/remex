package regress

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestBundlePreservesRelativeSymlinkGraph(t *testing.T) {
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd unavailable")
	}
	top := t.TempDir()
	root := filepath.Join(top, "root")
	os.MkdirAll(filepath.Join(root, "runfiles", "pkg"), 0755)
	os.WriteFile(filepath.Join(root, "data"), []byte("real-payload"), 0644)
	os.Symlink("../../data", filepath.Join(root, "runfiles", "pkg", "data"))
	os.Symlink("runfiles/pkg/data", filepath.Join(root, "chain"))
	os.Symlink(".", filepath.Join(root, "same"))
	out := filepath.Join(top, "bundle")
	err := ExportBundle(root, out, Bundle{Command: []string{"sh", "-c", "test \"$(readlink runfiles/pkg/data)\" = ../../data; cat chain; cat same/data"}})
	if err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(root)
	data, err := exec.Command(filepath.Join(out, "run.sh")).CombinedOutput()
	if err != nil || string(data) != "real-payloadreal-payload" {
		t.Fatalf("%s %v", data, err)
	}
}

func TestBundleRejectsChainedEscapeCycleAndDangling(t *testing.T) {
	for _, tc := range []struct {
		name  string
		links map[string]string
	}{{"chain-escape", map[string]string{"a": ".", "b": "a/../outside"}}, {"cycle", map[string]string{"a": "b", "b": "a"}}, {"dangling", map[string]string{"a": "missing"}}, {"absolute", map[string]string{"a": "/etc/passwd"}}} {
		t.Run(tc.name, func(t *testing.T) {
			top := t.TempDir()
			root := filepath.Join(top, "root")
			os.Mkdir(root, 0755)
			os.WriteFile(filepath.Join(top, "outside"), []byte("outside"), 0644)
			for name, target := range tc.links {
				os.Symlink(target, filepath.Join(root, name))
			}
			if err := ExportBundle(root, filepath.Join(top, "bundle"), Bundle{Command: []string{"true"}}); err == nil {
				t.Fatal("unsafe symlink graph exported")
			}
		})
	}
}
