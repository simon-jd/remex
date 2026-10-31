#!/usr/bin/env python3
"""Run target-determinator on detached Git worktrees and apply remex fallbacks."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import xml.etree.ElementTree as ET

p = argparse.ArgumentParser()
p.add_argument("--repository", type=Path, required=True)
p.add_argument("--before", required=True)
p.add_argument("--after", default="HEAD")
p.add_argument("--workspace", default=".")
p.add_argument("--bazel", default="bazel")
p.add_argument("--selector", default="target-determinator")
p.add_argument("--remex", default="remex")
p.add_argument("--evidence", type=Path, required=True)
p.add_argument("--baseline-environment", type=Path)
p.add_argument("--current-environment", type=Path)
args = p.parse_args()
repo = args.repository.resolve()
evidence = args.evidence.resolve()
evidence.mkdir(parents=True, exist_ok=True)
workspace = Path(args.workspace)
if workspace.is_absolute() or ".." in workspace.parts:
    p.error("workspace must stay inside the Git repository")
def run(argv, cwd=repo, timeout=600):
    return subprocess.run(argv, cwd=cwd, text=True, capture_output=True, timeout=timeout)
def revision(ref):
    if ref.startswith("-"):
        p.error("invalid Git revision")
    result = run(["git", "rev-parse", "--verify", ref+"^{commit}"])
    if result.returncode:
        raise SystemExit(result.stderr)
    return result.stdout.strip()
before, after = revision(args.before), revision(args.after)
diff = run(["git", "diff", "--name-only", "-z", before, after, "--"])
if diff.returncode:
    raise SystemExit(diff.stderr)
changed = [f for f in diff.stdout.split("\0") if f]
request = {"targets":[], "affected":[], "changed_files":changed}
environment_error = ""
if not args.baseline_environment or not args.current_environment:
    environment_error = "no baseline/current environment pair; hardware/flags equality is unproven"
else:
    old = json.loads(args.baseline_environment.read_text())
    new = json.loads(args.current_environment.read_text())
    request.update(image_changed=old.get("image") != new.get("image"),
                   driver_changed=old.get("driver") != new.get("driver"),
                   platform_changed=old.get("platform") != new.get("platform") or old.get("bazel_flags") != new.get("bazel_flags") or old.get("bazelrc_sha256") != new.get("bazelrc_sha256"))
with tempfile.TemporaryDirectory(prefix="remex-select-") as tmp:
    work = Path(tmp) / "worktree"
    add = run(["git", "worktree", "add", "--detach", str(work), after])
    if add.returncode:
        raise SystemExit(add.stderr)
    try:
        query = run([args.bazel, "query", "tests(//...)", "--output=xml", "--lockfile_mode=off"], work / workspace)
        (evidence / "inventory.xml").write_text(query.stdout)
        (evidence / "inventory.log").write_text(query.stderr)
        if query.returncode:
            raise SystemExit("Cannot enumerate current test universe; refuse to return an empty or partial selection. See inventory.log")
        for rule in ET.fromstring(query.stdout).findall("rule"):
            tags = [x.attrib["value"] for x in rule.findall("./list[@name='tags']/string")]
            request["targets"].append({"label":rule.attrib["name"],"tags":tags})
        command = [args.selector, "-bazel", args.bazel, "-working-directory", str(work / workspace), "-bazel-opts=--lockfile_mode=off", "-before-query-error-behavior", "fatal", "-nocache_results", "-delete-cached-worktree", before]
        try:
            selected = run(command, work / workspace)
            (evidence / "selector.stdout").write_text(selected.stdout)
            (evidence / "selector.stderr").write_text(selected.stderr)
            if selected.returncode:
                request["selector_error"] = f"target-determinator exited {selected.returncode}"
            else:
                labels = selected.stdout.splitlines()
                if any(not line.startswith("//") and not line.startswith("@") for line in labels):
                    request["selector_error"] = "malformed selector output"
                else:
                    request["affected"] = labels
        except (OSError, subprocess.TimeoutExpired) as error:
            request["selector_error"] = str(error)
        if environment_error:
            request["selector_error"] = "; ".join(filter(None, [request.get("selector_error"), environment_error]))
        inp = evidence / "selection-input.json"
        inp.write_text(json.dumps(request, indent=2)+"\n")
        result = run([args.remex, "select", "--input", str(inp)])
        if result.returncode:
            raise SystemExit(result.stderr)
        (evidence / "selection.json").write_text(result.stdout)
        print(result.stdout, end="")
    finally:
        # This detached worktree is private to the invocation. Release its
        # persistent Bazel server before deleting it; TD's config queries need
        # a persistent server, so --batch is not a valid substitute.
        run([args.bazel, "shutdown"], work / workspace, timeout=60)
        run(["git", "worktree", "remove", "--force", str(work)])
