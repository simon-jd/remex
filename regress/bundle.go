package regress

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

type InputFile struct {
	Type       string `json:"type,omitempty"`
	LinkTarget string `json:"link_target,omitempty"`
	Path       string `json:"path"`
	SHA256     string `json:"sha256"`
	Size       int64  `json:"size"`
	Executable bool   `json:"executable"`
}
type Bundle struct {
	SchemaVersion    int                 `json:"schema_version"`
	Revision         string              `json:"revision"`
	BazelVersion     string              `json:"bazel_version"`
	BazelFlags       []string            `json:"bazel_flags"`
	Command          []string            `json:"command"`
	WorkingDirectory string              `json:"working_directory"`
	Environment      map[string]string   `json:"environment"`
	Platform         map[string]string   `json:"platform"`
	Worker           map[string]any      `json:"worker"`
	GPU              map[string]any      `json:"gpu,omitempty"`
	Seed             int64               `json:"seed"`
	Inputs           []InputFile         `json:"inputs"`
	Source           *ActionBundleSource `json:"source,omitempty"`
}

func safeRelative(name string) bool {
	return name != "" && !strings.ContainsAny(
		name,
		"\\\x00\n\r",
	) && !path.IsAbs(name) && path.Clean(name) == name && name != ".." && !strings.HasPrefix(name, "../")
}

// ExportBundle creates a self-contained content bundle, never a list of CAS
// references. Relative symlinks are validated as a complete graph; special files
// and links that escape, cycle or dangle are refused. No destination
// is overwritten, and partial output is removed on errors.
func ExportBundle(inputRoot, destination string, b Bundle) (err error) {
	if len(b.Command) == 0 || b.Command[0] == "" {
		return fmt.Errorf("bundle requires command")
	}
	if b.WorkingDirectory == "" {
		b.WorkingDirectory = "."
	}
	if !safeRelative(b.WorkingDirectory) {
		return fmt.Errorf("unsafe working directory")
	}
	root, err := filepath.Abs(inputRoot)
	if err != nil {
		return err
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("input root must be a directory")
	}
	graph, err := inputGraph(root)
	if err != nil {
		return err
	}
	if err = validateInputGraph(graph); err != nil {
		return err
	}
	dest, err := filepath.Abs(destination)
	if err != nil {
		return err
	}
	// Canonicalize its parent so a symlink cannot place output inside the inputs.
	parent, err := filepath.EvalSymlinks(filepath.Dir(dest))
	if err != nil {
		return err
	}
	dest = filepath.Join(parent, filepath.Base(dest))
	rel, _ := filepath.Rel(root, dest)
	if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
		return fmt.Errorf("bundle destination must be outside input root")
	}
	if err = os.Mkdir(dest, 0755); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			os.RemoveAll(dest)
		}
	}()
	archive, err := os.Create(filepath.Join(dest, "inputs.tar.zst"))
	if err != nil {
		return err
	}
	zstd := exec.Command("zstd", "-q", "-T1", "-c")
	zstd.Stdout = archive
	zstd.Stderr = os.Stderr
	pipe, err := zstd.StdinPipe()
	if err != nil {
		archive.Close()
		return err
	}
	if err = zstd.Start(); err != nil {
		archive.Close()
		return err
	}
	tw := tar.NewWriter(pipe)
	b.Inputs = nil
	walkErr := filepath.Walk(root, func(file string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if file == root {
			return nil
		}
		rel, err := filepath.Rel(root, file)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if !safeRelative(name) {
			return fmt.Errorf("unsafe input path: %q", name)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			target, readErr := os.Readlink(file)
			if readErr != nil {
				return readErr
			}
			if target != graph[name].target {
				return fmt.Errorf("input symlink changed during export: %s", name)
			}
			if err := tw.WriteHeader(&tar.Header{
				Name:     name,
				Mode:     0777,
				Typeflag: tar.TypeSymlink,
				Linkname: target,
			}); err != nil {
				return err
			}
			b.Inputs = append(b.Inputs, InputFile{Path: name, Type: "symlink", LinkTarget: target})
			return nil
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported input type: %s", name)
		}
		h := &tar.Header{Name: name, Mode: int64(info.Mode().Perm()), Typeflag: tar.TypeDir}
		if info.IsDir() {
			return tw.WriteHeader(h)
		}
		h.Typeflag = tar.TypeReg
		h.Size = info.Size()
		h.Mode = 0644
		if info.Mode()&0111 != 0 {
			h.Mode = 0755
		}
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		sum := sha256.New()
		n, copyErr := io.Copy(io.MultiWriter(tw, sum), f)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if n != info.Size() {
			return fmt.Errorf("input changed while reading: %s", name)
		}
		b.Inputs = append(
			b.Inputs,
			InputFile{Path: name, SHA256: hex.EncodeToString(sum.Sum(nil)), Size: n, Executable: info.Mode()&0111 != 0},
		)
		return nil
	})
	closeErr := tw.Close()
	pipeErr := pipe.Close()
	waitErr := zstd.Wait()
	archiveErr := archive.Close()
	for _, e := range []error{walkErr, closeErr, pipeErr, waitErr, archiveErr} {
		if e != nil {
			return e
		}
	}
	b.SchemaVersion = 1
	sort.Slice(b.Inputs, func(i, j int) bool { return b.Inputs[i].Path < b.Inputs[j].Path })
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(dest, "bundle.json"), append(data, '\n'), 0644); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dest, "run.sh"), []byte(replayScript), 0755)
}

type inputNode struct{ kind, target string }

func inputGraph(root string) (map[string]inputNode, error) {
	nodes := map[string]inputNode{".": {kind: "directory"}}
	err := filepath.Walk(root, func(file string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if file == root {
			return nil
		}
		rel, err := filepath.Rel(root, file)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if !safeRelative(name) {
			return fmt.Errorf("unsafe input path: %q", name)
		}
		node := inputNode{kind: "file"}
		if info.Mode()&os.ModeSymlink != 0 {
			node.kind = "symlink"
			node.target, err = os.Readlink(file)
			if err != nil {
				return err
			}
		} else if info.IsDir() {
			node.kind = "directory"
		} else if !info.Mode().IsRegular() {
			return fmt.Errorf("unsupported input type: %s", name)
		}
		nodes[name] = node
		return nil
	})
	return nodes, err
}
func validateInputGraph(nodes map[string]inputNode) error {
	for name, node := range nodes {
		if node.kind != "symlink" {
			continue
		}
		if node.target == "" || path.IsAbs(node.target) || strings.ContainsAny(node.target, "\\\x00\n\r") {
			return fmt.Errorf("unsafe symlink target: %s", name)
		}
		var stack []string
		if parent := path.Dir(name); parent != "." {
			stack = strings.Split(parent, "/")
		}
		queue := strings.Split(node.target, "/")
		links := 0
		for len(queue) > 0 {
			part := queue[0]
			queue = queue[1:]
			switch part {
			case "", ".":
				continue
			case "..":
				if len(stack) == 0 {
					return fmt.Errorf("symlink escapes input root: %s", name)
				}
				stack = stack[:len(stack)-1]
				continue
			}
			key := strings.Join(append(append([]string{}, stack...), part), "/")
			next, ok := nodes[key]
			if !ok {
				return fmt.Errorf("dangling input symlink: %s -> %s", name, key)
			}
			if next.kind == "symlink" {
				links++
				if links > 64 {
					return fmt.Errorf("symlink cycle or excessive depth: %s", name)
				}
				if next.target == "" || path.IsAbs(next.target) || strings.ContainsAny(next.target, "\\\x00\n\r") {
					return fmt.Errorf("unsafe chained symlink: %s", key)
				}
				queue = append(strings.Split(next.target, "/"), queue...)
			} else {
				stack = append(stack, part)
				if next.kind != "directory" && len(queue) > 0 {
					return fmt.Errorf("symlink traverses non-directory: %s", name)
				}
			}
		}
	}
	return nil
}

const replayScript = `#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")"
exec python3 - "$@" <<'REMEX_PY'
import hashlib, json, os, pathlib, shutil, subprocess, sys, tarfile, tempfile
base = pathlib.Path.cwd()
manifest = json.loads((base / 'bundle.json').read_text())
if manifest.get('schema_version') != 1: raise SystemExit('unsupported bundle schema')
def safe(name):
    p = pathlib.PurePosixPath(name)
    return bool(name) and not p.is_absolute() and '..' not in p.parts and str(p) == name and not any(c in name for c in ('\\', '\x00', '\n', '\r'))
cwd = manifest.get('working_directory', '.')
if not safe(cwd): raise SystemExit('unsafe working directory')
expected = {}
for item in manifest['inputs']:
    name = item['path']
    if not safe(name) or name == '.' or name in expected: raise SystemExit('invalid manifest input')
    expected[name] = item
def validate_graph(nodes):
    for name, node in nodes.items():
        parent = pathlib.PurePosixPath(name).parent
        while str(parent) != '.':
            if nodes.get(str(parent), ('directory', ''))[0] != 'directory': raise ValueError('input has non-directory parent')
            parent = parent.parent
        if node[0] != 'symlink': continue
        target = node[1]
        if not target or target.startswith('/') or any(c in target for c in ('\\', '\x00', '\n', '\r')): raise ValueError('unsafe symlink target')
        stack = list(pathlib.PurePosixPath(name).parent.parts)
        queue = target.split('/')
        links = 0
        while queue:
            part = queue.pop(0)
            if part in ('', '.'): continue
            if part == '..':
                if not stack: raise ValueError('symlink escapes input root')
                stack.pop(); continue
            key = '/'.join(stack + [part])
            if key not in nodes: raise ValueError('dangling input symlink')
            kind, target = nodes[key]
            if kind == 'symlink':
                links += 1
                if links > 64: raise ValueError('symlink cycle or excessive depth')
                if not target or target.startswith('/') or any(c in target for c in ('\\', '\x00', '\n', '\r')): raise ValueError('unsafe chained symlink')
                queue = target.split('/') + queue
            else:
                stack.append(part)
                if kind != 'directory' and queue: raise ValueError('symlink traverses non-directory')
work = pathlib.Path(tempfile.mkdtemp(prefix='remex-replay-'))
try:
    z = subprocess.Popen(['zstd', '-q', '-d', '-c', str(base / 'inputs.tar.zst')], stdout=subprocess.PIPE)
    seen = set()
    nodes = {'.': ('directory', '')}
    symlinks = []
    try:
        with tarfile.open(fileobj=z.stdout, mode='r|') as archive:
            for member in archive:
                name = member.name
                if not safe(name) or name == '.' or name in seen: raise ValueError('unsafe or duplicate archive member')
                seen.add(name)
                target = work / name
                parent = pathlib.PurePosixPath(name).parent
                while str(parent) != '.':
                    nodes.setdefault(str(parent), ('directory', '')); parent = parent.parent
                if member.isdir():
                    nodes[name] = ('directory', '')
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                if member.issym():
                    entry = expected.get(name)
                    if not entry or entry.get('type') != 'symlink' or entry.get('link_target') != member.linkname: raise ValueError('symlink manifest mismatch')
                    nodes[name] = ('symlink', member.linkname)
                    symlinks.append((name, member.linkname))
                    continue
                if not member.isfile() or name not in expected: raise ValueError('unexpected archive member')
                entry = expected[name]
                if entry.get('type', 'file') != 'file': raise ValueError('input member type mismatch')
                nodes[name] = ('file', '')
                if member.size != entry['size']: raise ValueError('input size mismatch')
                target.parent.mkdir(parents=True, exist_ok=True)
                digest = hashlib.sha256()
                with archive.extractfile(member) as src, open(target, 'xb') as dst:
                    while True:
                        chunk = src.read(1024 * 1024)
                        if not chunk: break
                        digest.update(chunk); dst.write(chunk)
                if digest.hexdigest() != entry['sha256']: raise ValueError('input hash mismatch: ' + name)
                target.chmod(0o755 if entry['executable'] else 0o644)
        if z.wait() != 0: raise ValueError('zstd decode failed')
    finally:
        z.stdout.close()
        if z.poll() is None: z.kill(); z.wait()
    if not expected.keys() <= seen: raise ValueError('missing archive inputs')
    validate_graph(nodes)
    for name, target in symlinks:
        link = work / name
        link.parent.mkdir(parents=True, exist_ok=True)
        os.symlink(target, link)
    if manifest.get('source'):
        env = dict(manifest.get('environment') or {})
    else:
        env = os.environ.copy()
        env.update(manifest.get('environment') or {})
        env['REMEX_REPLAY_SEED'] = str(manifest.get('seed', 0))
    command = manifest['command']
    if not isinstance(command, list) or not command or not all(isinstance(s,str) for s in command): raise ValueError('invalid command')
    result = subprocess.run(command + sys.argv[1:], cwd=work/cwd, env=env)
    raise SystemExit(result.returncode)
finally:
    shutil.rmtree(work)
REMEX_PY
`
