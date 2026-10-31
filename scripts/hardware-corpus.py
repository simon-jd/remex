#!/usr/bin/env python3
"""Evaluate planted hardware history using real Bazel/remex executions.

The full oracle and selected arm run independently with identical REMEX_SEED.
Skipped selected targets carry a previous *selected-arm* observation for that
same seed; they are never counted as newly executed tests. Missing evidence or
cross-arm replay disagreement is inconclusive, not a zero-miss success.
"""
import argparse
import ctypes
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import stat
import selectors
import subprocess
import sys
import time
import urllib.request
import urllib.parse
import uuid
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
MASK = (1 << 64) - 1

def seed_gate(seed):
    x = (seed + 0x9E3779B97F4A7C15) & MASK
    x = ((x ^ (x >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    x = ((x ^ (x >> 27)) * 0x94D049BB133111EB) & MASK
    return (x ^ (x >> 31)) % 4 == 0

def parse_indices(text, count):
    if text == "all":
        return list(range(count))
    indices = set()
    for part in text.split(","):
        ends = part.split("-")
        if len(ends) == 1:
            indices.add(int(part))
        elif len(ends) == 2:
            start, end = map(int, ends)
            if start > end:
                raise ValueError("reversed index range")
            indices.update(range(start, end + 1))
        else:
            raise ValueError("invalid index range")
    if not indices or min(indices) < 0 or max(indices) >= count:
        raise ValueError("revision index outside corpus")
    return sorted(indices)

def expected_category(scenario, index, seed):
    planted = index >= scenario["culprit_index"]
    return "TEST_FAILURE" if planted and (not scenario.get("seed_gate") or seed_gate(seed)) else "PASS"

def scope_ledger(ledger, invocation):
    executions = [e for e in ledger.get("executions", []) if invocation in e.get("invocation_ids", [])]
    ids = {e["id"] for e in executions}
    attempts = [a for a in ledger.get("attempts", []) if a.get("execution_id") in ids]
    return dict(schema_version=ledger.get("schema_version", 1), executions=executions,
                attempts=attempts, events=[e for e in ledger.get("events", []) if e.get("execution_id") in ids])

def hardware_test_attempts(ledger, result, pool):
    # Join the BEP test.log digest to the winning ActionResult's output digests.
    # A compiler happening to use the right pool is not test-execution evidence.
    digests = set()
    for artifact in result.get("testActionOutput", []):
        if artifact.get("name") != "test.log":
            continue
        uri = urllib.parse.urlparse(artifact.get("uri", ""))
        match = re.search(r"/blobs/([0-9a-f]{64})/(\d+)$", uri.path)
        if uri.scheme == "bytestream" and match:
            digests.add((match[1], int(match[2])))
        elif uri.scheme == "file" and uri.netloc in ("", "localhost"):
            data = Path(urllib.request.url2pathname(uri.path)).read_bytes()
            digests.add((hashlib.sha256(data).hexdigest(), len(data)))
    executions = {e["id"]: e for e in ledger["executions"]}
    matched = []
    for attempt in ledger["attempts"]:
        execution = executions[attempt["execution_id"]]
        if execution.get("winner_attempt_id") != attempt["id"]:
            continue
        if not execution.get("do_not_cache") or execution.get("cached_result"):
            continue
        if execution.get("platform", {}).get("pool") != pool or attempt.get("worker_platform", {}).get("pool") != pool:
            continue
        if any((d.get("hash"), d.get("size_bytes")) in digests for d in attempt.get("result_digests", [])):
            matched.append(attempt)
    return matched

def retry_evidence(ledger, events, scenario, testlogs, evidence):
    """Preserve each retry and prove a bijection to distinct hardware winners.

    Identical retry logs can have the same digest. In that case the evidence
    records all candidate winner IDs instead of inventing a unique pairing.
    """
    local = {}
    for path in testlogs.rglob("*.log"):
        if path.is_file():
            data = path.read_bytes()
            local[(hashlib.sha256(data).hexdigest(), len(data))] = data
    records = []
    winners = {}
    for number, event in enumerate(events, 1):
        result = event["testResult"]
        if result.get("cachedLocally") or result.get("executionInfo", {}).get("cachedRemotely") or result.get("executionInfo", {}).get("strategy") != "remote":
            raise ValueError("every retry must be a fresh remote execution")
        attempts = hardware_test_attempts(ledger, result, scenario["hardware"])
        if not attempts or any(not attempt.get("cgroup_empty") for attempt in attempts):
            raise ValueError("retry lacks uncached winning hardware attempts with empty cgroups")
        if any(attempt.get("oom_kill") or attempt.get("command_timed_out") or attempt.get("failure_category") not in (None, "", "SUCCESS", "TEST_FAILURE") or attempt.get("status") not in (None, "OK") for attempt in attempts):
            raise ValueError("retry hardware attempt has infrastructure failure evidence")
        if scenario["hardware"] == "gpu" and any(not (attempt.get("device_verified") and attempt.get("device_snapshot_json")) for attempt in attempts):
            raise ValueError("GPU retry lacks NVML identity evidence")
        contents = []
        for artifact in result.get("testActionOutput", []):
            if artifact.get("name") != "test.log":
                continue
            uri = urllib.parse.urlparse(artifact.get("uri", ""))
            match = re.search(r"/blobs/([0-9a-f]{64})/(\d+)$", uri.path)
            if uri.scheme == "bytestream" and match:
                data = local.get((match[1], int(match[2])))
            elif uri.scheme == "file" and uri.netloc in ("", "localhost"):
                data = Path(urllib.request.url2pathname(uri.path)).read_bytes()
            else:
                data = None
            if data is not None:
                contents.append(data)
        if len(contents) != 1:
            raise ValueError("each retry needs a downloaded test.log matching its BEP digest")
        log = contents[0]
        directory = evidence / ("retry-%d" % number)
        directory.mkdir()
        (directory / "test.log").write_bytes(log)
        save(directory / "bep-event.json", event)
        text = log.decode("utf-8", errors="replace")
        signatures = sorted(set(re.findall(r"REMEX_SIGNATURE:\s*([A-Za-z0-9_.-]+)", text)))
        if "CUDA infrastructure:" in text or "RTL infrastructure:" in text:
            raise ValueError("hardware runtime reported infrastructure failure")
        records.append(dict(bep_id=event["id"]["testResult"], status=result.get("status"),
                            signatures=signatures, test_log_sha256=hashlib.sha256(log).hexdigest(),
                            test_log_bytes=len(log), evidence=str(directory),
                            candidate_winner_attempt_ids=sorted(attempt["id"] for attempt in attempts)))
        winners.update({attempt["id"]: attempt for attempt in attempts})
    if len(winners) != len(records):
        raise ValueError("retry count must equal the distinct matching hardware winner count")
    # A perfect matching establishes set-level provenance even for repeated logs.
    matched = {}
    def assign(index, seen):
        for attempt in records[index]["candidate_winner_attempt_ids"]:
            if attempt in seen:
                continue
            seen.add(attempt)
            if attempt not in matched or assign(matched[attempt], seen):
                matched[attempt] = index
                return True
        return False
    if not all(assign(index, set()) for index in range(len(records))):
        raise ValueError("retry logs cannot be matched to distinct hardware winners")
    proof = dict(retries=records, hardware_attempts=list(winners.values()),
                 distinct_winner_count=len(winners),
                 unique_pairing=all(len(row["candidate_winner_attempt_ids"]) == 1 for row in records),
                 note="Repeated identical log digests prove a set of distinct winners; candidate lists do not claim unique per-retry pairing.")
    save(evidence / "hardware-retries.json", proof)
    return proof

def retry_classification(proof, bazel_exit, signature):
    statuses = [row["status"] for row in proof["retries"]]
    if not statuses or bazel_exit not in (0, 3):
        raise ValueError("Bazel did not complete a normal retry invocation")
    for row in proof["retries"]:
        if row["status"] == "FAILED" and row["signatures"] != [signature]:
            return dict(category="INCONCLUSIVE", signature="", detail="retry failed with a different or absent signature")
        if row["status"] not in ("PASSED", "FAILED"):
            return dict(category="INFRASTRUCTURE", signature="", detail="retry did not finish as pass or test failure")
    if bazel_exit == 0 and statuses[-1] == "PASSED":
        return dict(category="PASS", signature="", flaky_observed="FAILED" in statuses,
                    detail="Bazel retry baseline passed; individual failures remain in hardware-retries.json")
    if bazel_exit == 3 and all(status == "FAILED" for status in statuses):
        return dict(category="TEST_FAILURE", signature=signature, detail="all recorded Bazel retries failed with the expected signature")
    raise ValueError("Bazel retry exit and per-attempt statuses disagree")

def test_inventory(xml, required):
    targets = []
    for rule in ET.fromstring(xml).findall("rule"):
        if rule.attrib["name"] in required:
            tags = [x.attrib["value"] for x in rule.findall("./list[@name='tags']/string")]
            if not {"remex-hw", "no-remote-cache"}.issubset(tags):
                raise ValueError("hardware target is missing remex-hw/no-remote-cache tags")
            targets.append(dict(label=rule.attrib["name"], tags=tags))
    if {target["label"] for target in targets} != set(required):
        raise ValueError("Bazel query did not enumerate every requested hardware test")
    return targets

def compare_observations(oracle, selected, executed):
    if not oracle or not selected or oracle["category"] not in ("PASS", "TEST_FAILURE") or selected["category"] not in ("PASS", "TEST_FAILURE"):
        return "INCONCLUSIVE"
    if (oracle["category"], oracle.get("signature", "")) == (selected["category"], selected.get("signature", "")):
        return "AGREE"
    if executed:
        return "REPLAY_DISAGREEMENT"
    if oracle["category"] == "TEST_FAILURE" and selected["category"] == "PASS":
        return "MISSED_FAILURE"
    return "STALE_OBSERVATION"

def save(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2) + "\n")

def command_supervisor(parent, stderr_path, argv):
    """Linux subreaper keeps escaped sessions owned until output is complete.

    A process-group kill alone misses probe commands and daemons which call
    setsid(). On cancellation, stop parents before enumerating their children;
    orphaned grandchildren are adopted here and included in the next sweep.
    """
    cancelled = False
    def cancel(signum, frame):
        nonlocal cancelled
        cancelled = True
    signal.signal(signal.SIGTERM, cancel)
    signal.signal(signal.SIGINT, cancel)
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) or libc.prctl(1, signal.SIGTERM, 0, 0, 0):
        raise OSError(ctypes.get_errno(), "cannot establish command subreaper")
    if os.getppid() != parent:
        return 125
    stderr = open(stderr_path, "wb") if stderr_path != "-" else subprocess.STDOUT
    try:
        child = subprocess.Popen(argv, stdout=subprocess.PIPE, stderr=stderr)
    finally:
        if stderr != subprocess.STDOUT:
            stderr.close()
    reader = selectors.DefaultSelector()
    reader.register(child.stdout, selectors.EVENT_READ)
    status = None
    eof = False
    output_closed = False
    def children(pid):
        result = set()
        for task in Path("/proc/%d/task" % pid).glob("*/children"):
            try:
                result.update(int(item) for item in task.read_text().split())
            except FileNotFoundError:
                pass
        return result
    def stop_tree(pid, owned):
        if pid in owned:
            return
        try:
            os.kill(pid, signal.SIGSTOP)
        except ProcessLookupError:
            return
        owned.add(pid)
        for descendant in children(pid):
            stop_tree(descendant, owned)
    def reap():
        nonlocal status
        while True:
            try:
                pid, raw = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return
            if not pid:
                return
            if pid == child.pid:
                status = os.waitstatus_to_exitcode(raw)
    while True:
        if cancelled:
            # Repeated sweeps cover children adopted when their parents exit.
            owned = set()
            for pid in children(os.getpid()):
                stop_tree(pid, owned)
            for pid in owned:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        for key, _ in reader.select(.02):
            data = os.read(key.fd, 65536)
            if data and not output_closed:
                try:
                    offset = 0
                    while offset < len(data):
                        offset += os.write(sys.stdout.fileno(), data[offset:])
                except BrokenPipeError:
                    # The owning driver may die while a child is producing
                    # output. A closed pipe must trigger cleanup, not bypass it.
                    cancelled = True
                    output_closed = True
            elif not data:
                reader.unregister(key.fileobj)
                eof = True
        reap()
        if status is not None and eof and (not cancelled or not children(os.getpid())):
            child.returncode = status
            reader.close()
            return 125 if cancelled else (status if status >= 0 else 128 - status)

def command(argv, cwd, log=None, timeout=1800, env=None, stderr_log=None):
    started = time.monotonic()
    if stderr_log:
        stderr_log.parent.mkdir(parents=True, exist_ok=True)
    try:
        process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "--command-supervisor",
                                    str(os.getpid()), str(stderr_log) if stderr_log else "-",
                                    *[str(x) for x in argv]], cwd=cwd, env=env, text=True,
                                   start_new_session=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        try:
            output, _ = process.communicate(timeout=timeout)
            code = process.returncode
        except subprocess.TimeoutExpired:
            # Let the subreaper kill/reap all descendants, including new sessions.
            process.send_signal(signal.SIGTERM)
            output, _ = process.communicate()
            code = 124
            output += "\nremex corpus driver: command timed out\n"
        except BaseException:
            process.send_signal(signal.SIGTERM)
            process.communicate()
            raise
    except OSError as error:
        code, output = 127, str(error)
    if log:
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text(output)
    return code, output, time.monotonic() - started

def git(repository, *args):
    code, output, _ = command(["git", "-C", repository, *args], ROOT)
    if code:
        raise RuntimeError(output)
    return output.strip()

def executable(value):
    return str(Path(shutil.which(value) or value).resolve())

def workspace_paths(args, evidence):
    if not getattr(args, "reuse_resume_workspace", False):
        return evidence / "worktree", evidence / "bazel-state"
    source = args.resume_from.resolve()
    allowed, cursor = set(), source
    while cursor not in allowed:
        allowed.add(cursor)
        lineage = cursor / "resume-provenance.json"
        if not lineage.exists():
            break
        cursor = Path(json.loads(lineage.read_text())["source"]).resolve()
    else:
        raise ValueError("cyclic workspace continuation ancestry")
    provenance = source / "workspace-provenance.json"
    recorded = (json.loads(provenance.read_text()) if provenance.exists() else
                {"worktree":str(source / "worktree"), "bazel_state":str(source / "bazel-state")})
    work, state = Path(recorded["worktree"]).resolve(), Path(recorded["bazel_state"]).resolve()
    if work.name != "worktree" or state.name != "bazel-state" or work.parent != state.parent or work.parent not in allowed:
        raise ValueError("workspace is outside recorded continuation ancestry")
    return work, state

def make_wrapper(args, evidence):
    bazel = executable(args.bazel)
    if not Path(bazel).is_file():
        raise ValueError("Bazel executable does not exist")
    args.remex, args.selector = executable(args.remex), executable(args.selector)
    wrapper = evidence / "bazel-wrapper.sh"
    _, state_root = workspace_paths(args, evidence)
    wrapper.write_text("#!/bin/sh\ncase \"$PWD\" in " + shlex.quote(str(evidence / "target-determinator-cache")) +
                       "/*) set -- --batch \"$@\" ;; esac\nexec " + shlex.quote(bazel) +
                       " --output_user_root=" + shlex.quote(str(state_root)) +
                       " --max_idle_secs=600 \"$@\"\n")
    wrapper.chmod(0o755)
    return wrapper

def selector_environment(evidence):
    # Some Bazel info calls inside TD use default lockfile mode. Ignore those
    # generated files without changing commits or the user's Git configuration.
    exclude = evidence / "git-excludes"
    exclude.write_text("**/MODULE.bazel.lock\n")
    env = dict(os.environ)
    count = int(env.get("GIT_CONFIG_COUNT", "0"))
    env.update({"GIT_CONFIG_COUNT": str(count + 1), "GIT_CONFIG_KEY_" + str(count): "core.excludesFile",
                "GIT_CONFIG_VALUE_" + str(count): str(exclude)})
    return env

def test_command(args, wrapper, scenario, seed, invocation, evidence, retry_limit=1):
    flags = list(args.bazel_flag) + (args.cuda_flag if scenario["workspace"] == "cuda" else args.rtl_flag)
    return [wrapper, "test", scenario["target"], *flags,
            "--lockfile_mode=off", "--remote_executor=" + args.executor,
            "--remote_cache=" + args.cache, "--remote_instance_name=" + args.instance,
            "--spawn_strategy=remote", "--strategy=TestRunner=remote", "--remote_local_fallback=false",
            "--nocache_test_results", "--remote_accept_cached=true",
            "--flaky_test_attempts=" + str(retry_limit), "--runs_per_test=1", "--test_output=all",
            "--remote_download_outputs=all", "--test_env=REMEX_SEED=" + str(seed),
            "--invocation_id=" + invocation, "--build_event_json_file=" + str(evidence / "bep.json"),
            "--remote_grpc_log=" + str(evidence / "grpc.log"),
            "--execution_log_compact_file=" + str(evidence / "execution.log")]

def test_observation(args, wrapper, work, scenario, name, index, revision, seed, arm, evidence):
    evidence.mkdir(parents=True, exist_ok=False)
    invocation = str(uuid.uuid4())
    seen = getattr(args, "_seen_invocations", set())
    if invocation in seen:
        raise ValueError("fresh oracle/selected invocations must be distinct")
    seen.add(invocation)
    args._seen_invocations = seen
    retry_limit = getattr(args, "baseline_retry_attempts", 1) if arm == "git-baseline" else 1
    argv = test_command(args, wrapper, scenario, seed, invocation, evidence, retry_limit)
    save(evidence / "probe.json", dict(revision=revision, index=index, seed=seed, scenario=name, arm=arm,
                                      invocation=invocation, command=[str(x) for x in argv]))
    code, output, elapsed = command(argv, work / scenario["workspace"], evidence / "bazel.log", args.timeout)
    observation = dict(category="INFRASTRUCTURE", signature="", detail="missing required evidence")
    test_seconds = None
    try:
        with urllib.request.urlopen(args.admin.rstrip("/") + "/export", timeout=60) as response:
            ledger = scope_ledger(json.load(response), invocation)
        save(evidence / "ledger.json", ledger)
        if not ledger["executions"] or not ledger["attempts"]:
            raise ValueError("no invocation-scoped remote attempts; hardware execution is unproven")
        bep = [json.loads(line) for line in (evidence / "bep.json").read_text().splitlines() if line]
        events = [e for e in bep if e.get("id", {}).get("testResult", {}).get("label") == scenario["target"]]
        events.sort(key=lambda event: event["id"]["testResult"].get("attempt", 1))
        results = [event["testResult"] for event in events]
        if not 1 <= len(results) <= retry_limit:
            raise ValueError("fresh test-result count is outside the configured retry limit")
        if arm == "git-baseline":
            package, target = scenario["target"][2:].split(":")
            proof = retry_evidence(ledger, events, scenario,
                                   work / scenario["workspace"] / "bazel-testlogs" / package / target, evidence)
            observation = retry_classification(proof, code, scenario["signature"])
            observation["retry_limit"] = retry_limit
            observation["retry_count"] = len(results)
            observation["hardware_retry_evidence"] = str(evidence / "hardware-retries.json")
            durations = [float(result["testAttemptDuration"][:-1]) if result.get("testAttemptDuration", "").endswith("s")
                         else float(result["testAttemptDurationMillis"]) / 1000 if "testAttemptDurationMillis" in result else None
                         for result in results]
            test_seconds = sum(durations) if all(value is not None for value in durations) else None
        else:
            if len(results) != 1 or results[0].get("cachedLocally") or results[0].get("executionInfo", {}).get("cachedRemotely"):
                raise ValueError("one fresh uncached test result is required")
            if results[0].get("executionInfo", {}).get("strategy") != "remote":
                raise ValueError("test did not execute remotely")
            hardware = hardware_test_attempts(ledger, results[0], scenario["hardware"])
            if len(hardware) != 1 or not hardware[0].get("cgroup_empty"):
                raise ValueError("BEP test.log must identify one uncached winning hardware attempt with empty cgroup")
            if scenario["hardware"] == "gpu" and not (hardware[0].get("device_verified") and hardware[0].get("device_snapshot_json")):
                raise ValueError("GPU execution lacks NVML identity evidence")
            save(evidence / "hardware-attempt.json", hardware[0])
            raw_duration = results[0].get("testAttemptDuration", "")
            if raw_duration.endswith("s"):
                test_seconds = float(raw_duration[:-1])
            elif "testAttemptDurationMillis" in results[0]:
                test_seconds = float(results[0]["testAttemptDurationMillis"]) / 1000
            signatures = set(re.findall(r"REMEX_SIGNATURE:\s*([A-Za-z0-9_.-]+)", output))
            if "CUDA infrastructure:" in output or "RTL infrastructure:" in output:
                raise ValueError("hardware runtime reported infrastructure failure")
            classify = [args.remex, "classify", "--bep", evidence / "bep.json", "--ledger", evidence / "ledger.json"]
            if len(signatures) == 1:
                classify += ["--signature", next(iter(signatures))]
            classify_code, classified, _ = command(classify, work, evidence / "classify.log", 60)
            if classify_code:
                raise ValueError("remex classify failed")
            observation = json.loads(classified)
            if code not in (0, 3) and observation.get("category") in ("PASS", "TEST_FAILURE"):
                raise ValueError("Bazel did not complete a normal test invocation")
            if observation.get("category") == "TEST_FAILURE" and observation.get("signature") != scenario["signature"]:
                observation = dict(category="INCONCLUSIVE", signature=observation.get("signature", ""), detail="different failure signature")
    except (OSError, ValueError, KeyError) as error:
        observation = dict(category="INFRASTRUCTURE", signature="", detail=str(error))
    observation.update(index=index, revision=revision, seed=seed, scenario=name, arm=arm,
                       invocation=invocation, bazel_exit=code, elapsed_seconds=elapsed,
                       test_seconds=test_seconds, evidence=str(evidence))
    observation["expected_category"] = expected_category(scenario, index, seed)
    observation["matches_planted_oracle"] = observation["category"] == observation["expected_category"]
    save(evidence / "observation.json", observation)
    return observation

def json_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

class FileFingerprint:
    def __init__(self):
        self.files = {}
    def snapshot(self, roots):
        entries = {}
        def visit(path):
            path = Path(os.path.abspath(path))
            key = str(path)
            if key in entries:
                return
            info = path.lstat()
            mode = stat.S_IMODE(info.st_mode)
            if path.is_symlink():
                target = os.readlink(path)
                entries[key] = ["symlink", mode, target]
                try:
                    visit(path.resolve(strict=True))
                except FileNotFoundError:
                    entries[key].append("dangling")
            elif path.is_dir():
                entries[key] = ["directory", mode]
                for child in sorted(path.iterdir()):
                    visit(child)
            elif path.is_file():
                token = (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, mode)
                old = self.files.get(key)
                if not old or old[0] != token:
                    with path.open("rb") as stream:
                        digest = hashlib.file_digest(stream, "sha256").hexdigest()
                    after = path.stat()
                    if token != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns, stat.S_IMODE(after.st_mode)):
                        raise ValueError("selector input changed during fingerprint: " + key)
                    self.files[key] = (token, digest)
                entries[key] = ["file", mode, info.st_size, self.files[key][1]]
            else:
                raise ValueError("unsupported special selector input: " + key)
        for root in roots:
            visit(root)
        return dict(roots=sorted(str(Path(root).resolve()) for root in roots), entries=entries)

def external_selector_inputs(external, workspace):
    # Bazel external/_main points back to the workspace, whose tracked contents
    # already have an exact Git-tree key. Following it would also traverse
    # bazel-* output symlinks and make every test result invalidate selection.
    main = external / "_main"
    if not main.is_symlink() or main.resolve() != workspace.resolve():
        raise ValueError("unexpected Bazel external/_main workspace mapping")
    return sorted(path for path in external.iterdir() if path.name != "_main")

def checked_workspace_inputs(args, work, family):
    code, output, _ = command(["git", "-C", work, "status", "--porcelain", "--untracked-files=all", "--", family], ROOT, timeout=60)
    if code or output.strip():
        raise ValueError("selector cache requires a clean tracked/untracked workspace")
    flags = git(work, "ls-files", "-v", "-z", "--", family).split("\0")
    if any(entry and entry[0] != "H" for entry in flags):
        raise ValueError("selector cache rejects assume-unchanged/skip-worktree index flags")
    inputs = []
    for entry in git(work, "ls-files", "--stage", "-z", "--", family).split("\0"):
        if not entry:
            continue
        header, relative = entry.split("\t", 1)
        mode, object_id, stage = header.split()
        path = work / relative
        if stage != "0" or mode not in ("100644", "100755") or path.is_symlink() or not path.is_file():
            raise ValueError("selector cache requires ordinary tracked files, without symlinks/gitlinks")
        data = path.read_bytes()
        blob = b"blob " + str(len(data)).encode() + b"\0" + data
        digest = hashlib.sha1(blob).hexdigest() if len(object_id) == 40 else hashlib.sha256(blob).hexdigest()
        if digest != object_id or bool(path.stat().st_mode & 0o111) != (mode == "100755"):
            raise ValueError("tracked source content/mode differs from the Git index")
        inputs.append(path)
    ignored = git(work, "ls-files", "--others", "--ignored", "--exclude-standard", "--", family).splitlines()
    for relative in ignored:
        path = work / relative
        if relative == family + "/MODULE.bazel.lock" and path.is_file():
            inputs.append(path)
        elif path.parent == work / family and path.name in ("bazel-bin", "bazel-out", "bazel-testlogs", "bazel-" + family) and path.is_symlink():
            pass
        else:
            raise ValueError("unexpected ignored workspace input: " + relative)
    return inputs

def selector_key(args, wrapper, work, family, before, after, environment, evidence):
    if not getattr(args, "selector_cache", False) or not environment or not args.selector_input:
        return None
    for revision in (before, after):
        marker = git(args.repository, "show", revision + ":revision.txt")
        if marker != str(args._commits.index(revision)):
            raise ValueError("selector cache requires the generator's numeric revision marker")
        tracked = git(args.repository, "ls-tree", "--name-only", revision).splitlines()
        if any(name in tracked for name in ("MODULE.bazel", "WORKSPACE", "WORKSPACE.bazel", "BUILD", "BUILD.bazel")):
            raise ValueError("selector cache does not support a repository-root Bazel workspace")
        code, _, _ = command(["git", "-C", args.repository, "grep", "-F", "revision.txt", revision, "--", ".", ":(exclude)revision.txt"], ROOT, timeout=60)
        if code != 1:
            raise ValueError("revision marker is referenced by tracked inputs or cannot be checked")
    info_code, output, _ = command([wrapper, "info", "output_base", "--lockfile_mode=off"], work / family,
                                  evidence / (family + "-output-base.log"), args.timeout,
                                  stderr_log=evidence / (family + "-output-base.stderr"))
    if info_code:
        return None
    external = Path(output.strip()) / "external"
    roots = [*args.selector_input, executable(args.bazel), args.selector, args.remex, wrapper,
             *external_selector_inputs(external, work / family), *checked_workspace_inputs(args, work, family)]
    snapshot = args._fingerprinter.snapshot(roots)
    fingerprint = json_digest(snapshot)
    snapshot_path = evidence.parent.parent / "selector-snapshots" / (fingerprint + ".json")
    if not snapshot_path.exists():
        save(snapshot_path, snapshot)
    # All repository content other than the generator's numeric revision marker
    # participates too, so sibling workspace/global-file changes invalidate reuse.
    def repository_tree(revision):
        rows = git(args.repository, "ls-tree", "-r", revision).splitlines()
        return json_digest([row for row in rows if row.split("\t", 1)[-1] != "revision.txt"])
    key = dict(schema_version=1, family=family,
               before_tree=git(args.repository, "rev-parse", before + ":" + family),
               after_tree=git(args.repository, "rev-parse", after + ":" + family),
               before_repository=repository_tree(before), after_repository=repository_tree(after),
               environment=environment, process_environment_sha256=json_digest(dict(os.environ)),
               toolchain_snapshot_sha256=fingerprint, toolchain_snapshot=str(snapshot_path),
               bazel_flags=args.bazel_flag, family_flags=args.cuda_flag if family == "cuda" else args.rtl_flag,
               selector_flags=["-before-query-error-behavior=fatal", "-nocache_results", "-delete-cached-worktree"],
               implementation_sha256=args._implementation_sha256,
               revision_marker_policy="schema2 numeric marker; no root Bazel workspace; no tracked reference; all other tracked repository files included")
    return key

def normalized_test_command(command):
    ignored = ("--invocation_id=", "--build_event_json_file=", "--remote_grpc_log=", "--execution_log_compact_file=")
    return [str(value) for value in command[1:] if not str(value).startswith(ignored)]

def resume_checkpoint(args, oracle, indices, names, seeds, evidence):
    source = args.resume_from.resolve()
    raw_path = source / "comparisons.jsonl"
    raw = raw_path.read_bytes()
    rows = [json.loads(line) for line in raw.splitlines() if line]
    required = {(name, seed) for name in names for seed in seeds}
    imported, selection_records, observations = [], [], []
    retained = {}
    environment = None
    previous = None
    artifact_hashes = {}
    roots = [source]
    seen_roots = {source}
    while (roots[-1] / "resume-provenance.json").exists():
        link = json.loads((roots[-1] / "resume-provenance.json").read_text())
        older = Path(link["source"]).resolve()
        if older in seen_roots or len(roots) >= 32:
            raise ValueError("checkpoint provenance cycle or excessive depth")
        if hashlib.sha256((older / "comparisons.jsonl").read_bytes()).hexdigest() != link["comparisons_sha256"]:
            raise ValueError("ancestor checkpoint changed after continuation")
        for artifact, expected_hash in link["artifact_sha256"].items():
            if hashlib.sha256(Path(artifact).read_bytes()).hexdigest() != expected_hash:
                raise ValueError("ancestor checkpoint raw evidence changed: " + artifact)
        roots.append(older)
        seen_roots.add(older)
    completed = []
    source_manifest = source / "run-manifest.json"
    if source_manifest.exists():
        old = json.loads(source_manifest.read_text())
        if old["contract"] != args._run_contract:
            raise ValueError("resume contract changed: versions, flags, scenarios, seeds or repository differ")
    elif not args.resume_legacy:
        raise ValueError("legacy evidence has no run manifest; explicit --resume-legacy is required")
    for index in indices:
        current = [row for row in rows if row["index"] == index]
        if len(current) != len(required) or {(row["scenario"], row["seed"]) for row in current} != required:
            break
        revision = oracle["commits"][index]
        selected_names = set()
        family_records = []
        directories = [root / ("revision-%02d" % index) / "selection" for root in roots]
        directory = next((path for path in directories if (path / "environment.json").exists()), directories[0])
        for artifact in directory.iterdir():
            if artifact.is_file():
                artifact_hashes[str(artifact)] = hashlib.sha256(artifact.read_bytes()).hexdigest()
        for family in sorted({oracle["scenarios"][name]["workspace"] for name in names}):
            selection_path = directory / (family + "-selection.json")
            result = json.loads(selection_path.read_text())
            family_records.append(dict(workspace=family, result=result))
            selected_names.update(name for name in names if oracle["scenarios"][name]["workspace"] == family and oracle["scenarios"][name]["target"] in result["targets"])
        for row in current:
            name, seed = row["scenario"], row["seed"]
            if bool(row["selected_executed"]) != (name in selected_names):
                raise ValueError("checkpoint selection and executed arm disagree")
            for arm in ("oracle", "selected") if row["selected_executed"] else ("oracle",):
                observation = row[arm]
                if (observation["index"], observation["revision"], observation["seed"], observation["scenario"], observation["arm"]) != (index, revision, seed, name, arm):
                    raise ValueError("checkpoint observation identity differs from requested experiment")
                path = Path(observation["evidence"]).resolve()
                if not any(path.is_relative_to(root) for root in roots):
                    raise ValueError("checkpoint observation is outside its evidence directory")
                if json.loads((path / "observation.json").read_text()) != observation:
                    raise ValueError("checkpoint embedded observation differs from its raw file")
                probe = json.loads((path / "probe.json").read_text())
                expected = test_command(args, "wrapper", oracle["scenarios"][name], seed, "invocation", path)
                if normalized_test_command(probe["command"]) != normalized_test_command(expected):
                    raise ValueError("checkpoint execution flags differ from resumed experiment")
                if any(probe.get(key) != observation[key] for key in ("revision", "index", "seed", "scenario", "arm", "invocation")):
                    raise ValueError("checkpoint raw probe identity differs")
                seen = getattr(args, "_seen_invocations", set())
                if observation["invocation"] in seen:
                    raise ValueError("checkpoint fresh arms reuse an invocation")
                seen.add(observation["invocation"])
                args._seen_invocations = seen
                if observation["category"] in ("PASS", "TEST_FAILURE"):
                    ledger = json.loads((path / "ledger.json").read_text())
                    if ledger != scope_ledger(ledger, observation["invocation"]):
                        raise ValueError("checkpoint ledger is not scoped to its exact invocation")
                    bep = [json.loads(line) for line in (path / "bep.json").read_text().splitlines() if line]
                    versions = [event["started"]["buildToolVersion"] for event in bep if "started" in event]
                    if versions != [args._run_contract["bazel_version"].removeprefix("bazel ")]:
                        raise ValueError("checkpoint Bazel version differs")
                    results = [event["testResult"] for event in bep if event.get("id", {}).get("testResult", {}).get("label") == oracle["scenarios"][name]["target"]]
                    if len(results) != 1 or results[0].get("cachedLocally") or results[0].get("executionInfo", {}).get("cachedRemotely") or results[0].get("executionInfo", {}).get("strategy") != "remote":
                        raise ValueError("checkpoint lacks a fresh remote hardware test result")
                    hardware = hardware_test_attempts(ledger, results[0], oracle["scenarios"][name]["hardware"])
                    if len(hardware) != 1 or not hardware[0].get("cgroup_empty"):
                        raise ValueError("checkpoint test.log does not prove one empty winning hardware attempt")
                    if oracle["scenarios"][name]["hardware"] == "gpu" and not (hardware[0].get("device_verified") and hardware[0].get("device_snapshot_json")):
                        raise ValueError("checkpoint GPU evidence is absent")
                    expected_status = "PASSED" if observation["category"] == "PASS" else "FAILED"
                    if results[0].get("status") != expected_status:
                        raise ValueError("checkpoint category disagrees with BEP test status")
                    raw_output = (path / "bazel.log").read_text()
                    if "CUDA infrastructure:" in raw_output or "RTL infrastructure:" in raw_output:
                        raise ValueError("checkpoint has an infrastructure diagnostic")
                    signatures = set(re.findall(r"REMEX_SIGNATURE:\s*([A-Za-z0-9_.-]+)", raw_output))
                    classify = [args.remex, "classify", "--bep", path / "bep.json", "--ledger", path / "ledger.json"]
                    if len(signatures) == 1:
                        classify += ["--signature", next(iter(signatures))]
                    code, classified, _ = command(classify, ROOT, evidence / "resume-classification" / (probe["invocation"] + ".json"), 60)
                    fresh = json.loads(classified) if code == 0 else {}
                    if (fresh.get("category"), fresh.get("signature", "")) != (observation["category"], observation.get("signature", "")):
                        raise ValueError("checkpoint category/signature differs from fresh classification of raw evidence")
                expected_category_now = expected_category(oracle["scenarios"][name], index, seed)
                if observation["expected_category"] != expected_category_now or observation["matches_planted_oracle"] != (observation["category"] == expected_category_now):
                    raise ValueError("checkpoint planted-oracle comparison is inconsistent")
                for artifact in path.iterdir():
                    if artifact.is_file():
                        artifact_hashes[str(artifact)] = hashlib.sha256(artifact.read_bytes()).hexdigest()
                observations.append(observation)
                if arm == "selected":
                    retained[(name, seed)] = observation
            if row["selected"] != retained.get((name, seed)):
                raise ValueError("checkpoint retained selected-arm provenance differs")
            if row["verdict"] != compare_observations(row["oracle"], row["selected"], row["selected_executed"]):
                raise ValueError("checkpoint comparison verdict is inconsistent")
            imported.append(row)
        completed.append(index)
        selection_records.append(dict(index=index, chosen=sorted(selected_names), workspaces=family_records))
        environment = json.loads((directory / "environment.json").read_text())
        previous = revision
    if not completed:
        raise ValueError("checkpoint has no complete requested revision")
    proof = dict(source=str(source), comparisons_sha256=hashlib.sha256(raw).hexdigest(),
                 completed_indices=completed, imported_comparisons=len(imported),
                 incomplete_or_out_of_scope_rows=len(rows)-len(imported), artifact_sha256=artifact_hashes,
                 legacy_without_binary_manifest=not source_manifest.exists(),
                 note="Only complete revisions are imported. Raw source evidence is preserved; partial revisions are rerun. Legacy records prove their recorded commands, not historical executable content hashes.")
    save(evidence / "resume-provenance.json", proof)
    (evidence / "comparisons.jsonl").write_text("".join(json.dumps(row)+"\n" for row in imported))
    return imported, observations, selection_records, retained, environment, previous, completed

def selection(args, wrapper, work, before, after, names, scenarios, previous_environment, evidence):
    evidence.mkdir(parents=True, exist_ok=True)
    changed = git(args.repository, "diff", "--name-only", before, after, "--").splitlines() if before else []
    environment = json.loads(args.environment.read_text()) if args.environment else None
    selections = set()
    records = []
    for family in sorted({scenarios[name]["workspace"] for name in names}):
        family_names = [name for name in names if scenarios[name]["workspace"] == family]
        query_code, query_output, _ = command([wrapper, "query", "tests(//...)", "--output=xml", "--lockfile_mode=off"],
                                              work / family, evidence / (family + "-inventory.xml"), args.timeout,
                                              stderr_log=evidence / (family + "-inventory.log"))
        if query_code:
            raise RuntimeError("Cannot enumerate current test universe; refusing partial hardware selection")
        request = dict(targets=test_inventory(query_output, [scenarios[name]["target"] for name in family_names]),
                       affected=[], changed_files=changed)
        if not before:
            request["selector_error"] = "first evaluated revision requires a full selected-arm baseline"
        else:
            family_flags = args.cuda_flag if family == "cuda" else args.rtl_flag
            argv = [args.selector, "-bazel", str(wrapper), "-working-directory", str(work / family),
                    "-cache-dir", str(evidence.parent.parent / "target-determinator-cache"),
                    "-bazel-opts=--lockfile_mode=off", "-before-query-error-behavior", "fatal",
                    "-nocache_results", "-delete-cached-worktree",
                    *["-bazel-opts=" + flag for flag in args.bazel_flag + family_flags], before]
            try:
                key = None
                cache_diagnostic = None
                try:
                    key = selector_key(args, wrapper, work, family, before, after, environment, evidence)
                except (OSError, ValueError) as error:
                    cache_diagnostic = str(error)
                cache_path = evidence.parent.parent / "selector-cache" / (json_digest(key) + ".json") if key else None
                provenance = dict(cache_hit=False, key=key, cache_disabled_reason=cache_diagnostic)
                if cache_path and cache_path.exists():
                    cached = json.loads(cache_path.read_text())
                    source = Path(cached["source_stdout"])
                    if cached["key"] != key or hashlib.sha256(source.read_bytes()).hexdigest() != cached["source_stdout_sha256"]:
                        raise ValueError("selector cache provenance changed")
                    selector_code, labels = 0, cached["labels"]
                    if labels != source.read_text().splitlines():
                        raise ValueError("selector cache labels differ from actual source output")
                    provenance.update(cache_hit=True, source_stdout=str(source), cache_entry=str(cache_path))
                else:
                    selector_code, selector_output, _ = command(argv, work / family,
                                                               evidence / (family + "-selector.stdout"), args.timeout,
                                                               env=selector_environment(evidence),
                                                               stderr_log=evidence / (family + "-selector.stderr"))
                    labels = selector_output.splitlines()
                    provenance["actual_command"] = [str(value) for value in argv]
                    provenance["exit"] = selector_code
                    if selector_code == 0 and all(line.startswith(("//", "@")) for line in labels) and cache_path:
                        # Never cache results if an input changed during the call.
                        after_key = selector_key(args, wrapper, work, family, before, after, environment, evidence)
                        if after_key == key:
                            source = evidence / (family + "-selector.stdout")
                            save(cache_path, dict(key=key, labels=labels, exit=0, source_stdout=str(source),
                                                  source_stdout_sha256=hashlib.sha256(source.read_bytes()).hexdigest()))
                        else:
                            provenance["cache_disabled_reason"] = "selector inputs changed during actual call"
                save(evidence / (family + "-selector-provenance.json"), provenance)
                if selector_code or any(not line.startswith(("//", "@")) for line in labels):
                    request["selector_error"] = "target-determinator failed or returned malformed labels"
                else:
                    request["affected"] = labels
            except (OSError, ValueError, subprocess.TimeoutExpired) as error:
                request["selector_error"] = str(error)
        if not environment or not environment.get("image") or not environment.get("driver"):
            request["selector_error"] = request.get("selector_error", "") + "; stable image/driver environment evidence absent"
        elif previous_environment and environment != previous_environment:
            request["platform_changed"] = True
        inp = evidence / (family + "-input.json")
        save(inp, request)
        code, output, _ = command([args.remex, "select", "--input", inp], work, evidence / (family + "-selection.log"), 60)
        if code:
            raise RuntimeError("remex select failed; refusing a partial hardware selection")
        chosen = json.loads(output)
        save(evidence / (family + "-selection.json"), chosen)
        for name in family_names:
            if scenarios[name]["target"] in chosen["targets"]:
                selections.add(name)
        records.append(dict(workspace=family, result=chosen))
    save(evidence / "environment.json", environment)
    return selections, environment, records

def evaluate(args):
    args.repository = args.repository.resolve()
    oracle = json.loads((args.repository / "oracle.json").read_text())
    if oracle.get("schema_version") != 2:
        raise ValueError("generate a new schema-2 hardware corpus; existing histories are never rewritten")
    if oracle.get("count") != 96 or len(oracle.get("commits", [])) != 96 or any(not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", commit) for commit in oracle["commits"]):
        raise ValueError("corpus must contain 96 explicit commit object IDs")
    indices = parse_indices(args.indices, oracle["count"])
    seeds = sorted(set(int(seed) for seed in args.seeds.split(",")))
    if not seeds or min(seeds) < 0 or max(seeds) > MASK:
        raise ValueError("seeds must be uint64")
    names = list(dict.fromkeys(args.scenarios.split(",")))
    scenarios = oracle["scenarios"]
    if any(name not in scenarios or not scenarios[name].get("hardware") or scenarios[name].get("workspace") not in ("cuda", "rtl") or not scenarios[name].get("target", "").startswith("//") for name in names):
        raise ValueError("all scenarios must name actual GPU/RTL tests")
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    # Keep the two evaluated workspaces warm across revisions and seeds. The
    # selector's disposable before-worktrees use batch mode to avoid leaving
    # one server per revision behind.
    if args.reuse_resume_workspace and not args.resume_from:
        raise ValueError("reusing a workspace requires --resume-from")
    wrapper = make_wrapper(args, evidence)
    args._commits = oracle["commits"]
    driver_source = Path(__file__).read_bytes()
    args._implementation_sha256 = hashlib.sha256(driver_source).hexdigest()
    (evidence / "driver-source.py").write_bytes(driver_source)
    args._fingerprinter = FileFingerprint()
    version_code, version, _ = command([executable(args.bazel), "--version"], ROOT, evidence / "bazel-version.txt", 60)
    if version_code:
        raise ValueError("cannot establish Bazel version")
    args._run_contract = dict(schema_version=1, oracle_sha256=hashlib.sha256((args.repository / "oracle.json").read_bytes()).hexdigest(),
                              repository=str(args.repository), indices=indices, scenarios=names, seeds=seeds,
                              bazel_flags=args.bazel_flag, cuda_flags=args.cuda_flag, rtl_flags=args.rtl_flag,
                              executor=args.executor, cache=args.cache, admin=args.admin, instance=args.instance,
                              bazel_version=version.strip(),
                              executables={name:hashlib.sha256(Path(executable(value)).read_bytes()).hexdigest()
                                           for name,value in (("bazel",args.bazel),("remex",args.remex),("selector",args.selector))})
    save(evidence / "run-manifest.json", dict(contract=args._run_contract,
                                             implementation_sha256=args._implementation_sha256,
                                             selector_cache=args.selector_cache,
                                             selector_inputs=[str(path.resolve()) for path in args.selector_input]))
    previous, environment = None, None
    retained, comparisons, observations, selection_records, completed = {}, [], [], [], []
    if args.resume_from:
        comparisons, observations, selection_records, retained, environment, previous, completed = resume_checkpoint(args, oracle, indices, names, seeds, evidence)
    remaining = [index for index in indices if index not in completed]
    work, state_root = workspace_paths(args, evidence)
    if work.exists():
        raise ValueError("previous worktree still exists; its experiment must finish cleanup before reuse")
    save(evidence / "workspace-provenance.json", dict(worktree=str(work), bazel_state=str(state_root),
                                                     reused_only_ephemeral_workspace=args.reuse_resume_workspace))
    git(args.repository, "worktree", "add", "--detach", str(work), oracle["commits"][remaining[0] if remaining else indices[-1]])
    try:
        for index in remaining:
            revision = oracle["commits"][index]
            git(work, "checkout", "--detach", "--force", revision)
            directory = evidence / ("revision-%02d" % index)
            chosen, environment, choices = selection(args, wrapper, work, previous, revision, names, scenarios, environment, directory / "selection")
            selection_records.append(dict(index=index, chosen=sorted(chosen), workspaces=choices))
            for name in names:
                for seed in seeds:
                    key = (name, seed)
                    full = test_observation(args, wrapper, work, scenarios[name], name, index, revision, seed, "oracle", directory / "oracle" / name / str(seed))
                    observations.append(full)
                    executed = name in chosen
                    if executed:
                        selected = test_observation(args, wrapper, work, scenarios[name], name, index, revision, seed, "selected", directory / "selected" / name / str(seed))
                        observations.append(selected)
                        retained[key] = selected
                    selected = retained.get(key)
                    comparison = dict(index=index, scenario=name, seed=seed, selected_executed=executed,
                                      selected_observation_revision=selected.get("revision") if selected else None,
                                      verdict=compare_observations(full, selected, executed), oracle=full,
                                      selected=selected)
                    comparisons.append(comparison)
                    with (evidence / "comparisons.jsonl").open("a") as stream:
                        stream.write(json.dumps(comparison) + "\n")
                    print(json.dumps({k: comparison[k] for k in ("index", "scenario", "seed", "selected_executed", "verdict")}), flush=True)
            previous = revision
            save(evidence / "checkpoint.json", dict(completed_indices=[*completed, *[value for value in remaining if value <= index]],
                                                    comparison_count=len(comparisons),
                                                    comparisons_sha256=hashlib.sha256((evidence / "comparisons.jsonl").read_bytes()).hexdigest()))
    finally:
        for family in sorted({scenarios[name]["workspace"] for name in names}):
            command([wrapper, "shutdown"], work / family, evidence / (family + "-shutdown.log"), 60)
        git(args.repository, "worktree", "remove", "--force", str(work))
    counts = {verdict: sum(row["verdict"] == verdict for row in comparisons) for verdict in ("AGREE", "MISSED_FAILURE", "REPLAY_DISAGREEMENT", "STALE_OBSERVATION", "INCONCLUSIVE")}
    mismatches = [o for o in observations if not o["matches_planted_oracle"]]
    summary = dict(schema_version=1, indices=indices, seeds=seeds, scenarios=names,
                   full_96_revisions=indices == list(range(96)), comparisons=counts,
                   resumed_from=str(args.resume_from.resolve()) if args.resume_from else None,
                   imported_complete_indices=completed,
                   planted_oracle_mismatches=len(mismatches), observed_invocations=len(observations),
                   selected_invocations=sum(o["arm"] == "selected" for o in observations),
                   oracle_invocations=sum(o["arm"] == "oracle" for o in observations),
                   selections=selection_records,
                   note="Only listed revisions/seeds/scenarios were executed. Seed-gate probability is not an intrinsic race failure probability. Reused selected observations are provenance-labelled; they are not fresh hardware executions.")
    for arm in ("oracle", "selected"):
        runs = [o for o in observations if o["arm"] == arm and scenarios[o["scenario"]]["hardware"] == "gpu"]
        summary[arm + "_gpu_test_seconds"] = sum(o["test_seconds"] or 0 for o in runs)
        summary[arm + "_gpu_duration_missing"] = sum(o["test_seconds"] is None for o in runs)
    save(evidence / "summary.json", summary)
    save(evidence / "oracle-mismatches.json", mismatches)
    print(json.dumps(summary, indent=2))
    return 0 if all(row["verdict"] == "AGREE" for row in comparisons) and not mismatches else 2

def hardware_probe(state_path, revision, seed, evidence):
    state = json.loads(state_path.read_text())
    if revision not in state["commits"] or not 0 <= seed <= MASK:
        raise ValueError("probe revision/seed is outside the declared corpus")
    args = argparse.Namespace(**state["arguments"])
    work = Path(state["worktree"])
    with state_path.with_suffix(".lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        git(work, "checkout", "--detach", "--force", revision)
        observation = test_observation(args, Path(state["wrapper"]), work, state["scenario"],
                                       args.scenario, state["commits"].index(revision), revision, seed,
                                       state["mode"], evidence / "hardware-run")
    if observation["category"] == "INCONCLUSIVE":
        observation["category"] = "UNKNOWN"
    return {key: observation[key] for key in ("category", "signature", "detail") if key in observation} | {
        "evidence": [str(evidence / "hardware-run")]}

def git_probe(state_path):
    state = json.loads(state_path.read_text())
    directory = Path(state["evidence"])
    counter = directory / "baseline-sequence.json"
    sequence = json.loads(counter.read_text()) if counter.exists() else 0
    save(counter, sequence + 1)
    seed = state["arguments"]["seed"] + sequence
    revision = git(state["worktree"], "rev-parse", "HEAD")
    observation = hardware_probe(state_path, revision, seed, directory / "probes" / ("trial-%03d" % sequence))
    save(directory / "probes" / ("trial-%03d" % sequence) / "observation.json", observation)
    print(json.dumps(dict(revision=revision, seed=seed, **observation)), flush=True)
    if observation["category"] == "PASS":
        return 0
    if observation["category"] == "TEST_FAILURE" and observation.get("signature") == state["scenario"]["signature"]:
        return 1
    return 125

def bisect_hardware(args):
    if args.seed < 0 or args.seed > sys.maxsize - max(getattr(args, "budget", 96), 96):
        raise ValueError("bisection seed range must fit the remex signed integer sequence")
    args.repository = args.repository.resolve()
    oracle = json.loads((args.repository / "oracle.json").read_text())
    if oracle.get("schema_version") != 2 or args.scenario not in ("cuda", "rtl", "cuda_race", "rtl_flaky"):
        raise ValueError("hardware bisection requires a generated schema-2 flaky scenario")
    scenario = oracle["scenarios"][args.scenario]
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    wrapper = make_wrapper(args, evidence)
    work = evidence / "worktree"
    git(args.repository, "worktree", "add", "--detach", str(work), oracle["commits"][-1])
    state_path = evidence / "probe-state.json"
    arguments = {key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()}
    save(state_path, dict(arguments=arguments, commits=oracle["commits"], scenario=scenario,
                          worktree=str(work), wrapper=str(wrapper), evidence=str(evidence), mode=args.action))
    try:
        if args.action == "bisect":
            config = dict(good=oracle["commits"][0], bad=oracle["commits"][-1],
                          bisect=dict(revisions=oracle["commits"], signature=scenario["signature"],
                                      alpha=args.alpha, calibration_runs=args.calibration_runs,
                                      budget=args.budget, seed=args.seed),
                          runner=dict(repository=str(args.repository), evidence_directory=str(evidence / "probes"),
                                      probe_command=[sys.executable, str(Path(__file__).resolve()), "probe", "--config", str(state_path)]))
            save(evidence / "bisect-config.json", config)
            code, output, elapsed = command([args.remex, "bisect", "--config", evidence / "bisect-config.json"],
                                             args.repository, evidence / "remex-result.json", args.experiment_timeout)
            result = json.loads(output) if code == 0 else dict(conclusion="INCONCLUSIVE", reason="remex process did not finish normally")
            summary = dict(scenario=args.scenario, mode=args.action, exit=code, elapsed_seconds=elapsed,
                           result=result, expected_culprit=scenario["culprit"],
                           culprit_matches_oracle=result.get("conclusion") == "CULPRIT" and result.get("culprit") == scenario["culprit"])
        else:
            git(work, "bisect", "start", oracle["commits"][-1], oracle["commits"][0])
            code, output, elapsed = command(["git", "bisect", "run", sys.executable, Path(__file__).resolve(),
                                             "git-probe", "--config", state_path], work,
                                             evidence / "git-bisect.log", args.experiment_timeout)
            (evidence / "git-bisect-replay.log").write_text(git(work, "bisect", "log") + "\n")
            candidate = git(work, "rev-parse", "refs/bisect/bad") if code == 0 else None
            summary = dict(scenario=args.scenario, mode=args.action, exit=code, elapsed_seconds=elapsed,
                           culprit=candidate, expected_culprit=scenario["culprit"],
                           culprit_matches_oracle=candidate == scenario["culprit"],
                           baseline_retry_attempts=args.baseline_retry_attempts,
                           note="Actual git bisect baseline, increasing seed per probe. Bazel retries share the fixed probe seed, so they repeat identical gated inputs; this does not measure general retry effectiveness.")
            git(work, "bisect", "reset")
        save(evidence / "summary.json", summary)
        print(json.dumps(summary, indent=2))
        return 0 if summary["culprit_matches_oracle"] else 2
    finally:
        command([wrapper, "shutdown"], work / scenario["workspace"], evidence / "shutdown.log", 60)
        git(args.repository, "worktree", "remove", "--force", str(work))

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    generate = sub.add_parser("generate", help="create a NEW 96-commit history, never modify an existing one")
    generate.add_argument("--output", type=Path, required=True)
    run = sub.add_parser("run", help="execute independent full and selected hardware arms")
    run.add_argument("--resume-from", type=Path, help="import only complete verified revisions from a prior evidence directory")
    run.add_argument("--reuse-resume-workspace", action="store_true", help="reuse a stopped predecessor's cleaned ephemeral worktree location and Bazel state")
    run.add_argument("--resume-legacy", action="store_true", help="allow old raw evidence without historical executable hashes")
    run.add_argument("--selector-cache", action="store_true", help="reuse only successful exact-input selector results")
    run.add_argument("--selector-input", type=Path, action="append", default=[], help="toolchain/system input root to fingerprint for selector caching (repeatable)")
    run.add_argument("--indices", default="0,46-47,62-63,71-72,79-80,95", help="comma-separated indices/ranges, or all")
    run.add_argument("--seeds", default="0,1,2,6", help="uint64 seeds; default includes trigger seed 6 and three quiet seeds")
    run.add_argument("--scenarios", default="cuda,rtl,cuda_race,rtl_flaky")
    run.add_argument("--environment", type=Path, help="current verified image/driver/platform/flags JSON; absent evidence conservatively selects all")
    bisect = sub.add_parser("bisect", help="run real remex flaky bisection with a persistent hardware workspace")
    baseline = sub.add_parser("git-baseline", help="run actual git bisect with increasing seeds and optional Bazel retries")
    baseline.add_argument("--baseline-retry-attempts", type=int, choices=(1, 3), default=1)
    for experiment in (bisect, baseline):
        experiment.add_argument("--scenario", choices=("cuda", "rtl", "cuda_race", "rtl_flaky"), required=True)
        experiment.add_argument("--seed", type=int, default=0)
        experiment.add_argument("--experiment-timeout", type=int, default=10800)
    bisect.add_argument("--alpha", type=float, default=0.05)
    bisect.add_argument("--calibration-runs", type=int, default=64)
    bisect.add_argument("--budget", type=int, default=1000)
    for experiment in (run, bisect, baseline):
        experiment.add_argument("--repository", type=Path, required=True)
        experiment.add_argument("--evidence", type=Path, required=True)
        experiment.add_argument("--bazel", default=str(ROOT / ".tools/bazel.official"))
        experiment.add_argument("--remex", default=str(ROOT / "build/bin/remex"))
        experiment.add_argument("--selector", default=str(ROOT / ".tools/target-determinator"))
        experiment.add_argument("--executor", default="grpc://127.0.0.1:18980")
        experiment.add_argument("--cache", default="grpc://127.0.0.1:19092")
        experiment.add_argument("--admin", default="http://127.0.0.1:18081")
        experiment.add_argument("--instance", default="remex-hardware-corpus")
        experiment.add_argument("--bazel-flag", action="append", default=[])
        experiment.add_argument("--cuda-flag", action="append", default=[])
        experiment.add_argument("--rtl-flag", action="append", default=[])
        experiment.add_argument("--timeout", type=int, default=1800)
    probe = sub.add_parser("probe", help=argparse.SUPPRESS)
    git_adapter = sub.add_parser("git-probe", help=argparse.SUPPRESS)
    for adapter in (probe, git_adapter):
        adapter.add_argument("--config", type=Path, required=True)
    probe.add_argument("revision")
    probe.add_argument("seed", type=int)
    probe.add_argument("probe_evidence", type=Path)
    args = parser.parse_args()
    if args.action == "generate":
        return subprocess.call([sys.executable, str(ROOT / "scripts/generate-history.py"), "--output", str(args.output)])
    if args.action == "probe":
        print(json.dumps(hardware_probe(args.config, args.revision, args.seed, args.probe_evidence)))
        return 0
    if args.action == "git-probe":
        return git_probe(args.config)
    if args.action in ("bisect", "git-baseline"):
        return bisect_hardware(args)
    return evaluate(args)

if __name__ == "__main__":
    try:
        if len(sys.argv) >= 5 and sys.argv[1] == "--command-supervisor":
            raise SystemExit(command_supervisor(int(sys.argv[2]), sys.argv[3], sys.argv[4:]))
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError, ET.ParseError, subprocess.TimeoutExpired) as error:
        print("hardware-corpus:", error, file=sys.stderr)
        raise SystemExit(2)
