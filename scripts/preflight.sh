#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
python3 - "$root" <<'PY'
import json, os, pathlib, shutil, subprocess, sys
root = pathlib.Path(sys.argv[1])
def probe(args):
    try:
        p = subprocess.run(args, capture_output=True, text=True, timeout=30)
        return {"exit_code": p.returncode, "output": (p.stdout+p.stderr).strip()[:3000]}
    except (OSError, subprocess.TimeoutExpired) as e:
        return {"exit_code": None, "output": str(e)}
cgroup = pathlib.Path(os.environ.get("REMEX_CGROUP_ROOT", f"/sys/fs/cgroup/user.slice/user-{os.getuid()}.slice/user@{os.getuid()}.service"))
report = {
    "go": probe(["go", "version"]),
    "cxx": probe(["g++", "--version"]),
    "docker": probe(["docker", "version", "--format", "{{.Server.Version}}"]),
    "cuda": probe(["nvcc", "--version"]),
    "gpu": probe(["nvidia-smi", "--query-gpu=name,uuid,driver_version", "--format=csv,noheader"]),
    "cgroup_parent": str(cgroup),
    "cgroup_parent_writable": os.access(cgroup, os.W_OK),
    "cgroup_controllers_present": (cgroup / "cgroup.controllers").exists(),
}
print(json.dumps(report, indent=2))
PY
