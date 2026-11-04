#!/usr/bin/env python3
"""End-to-end build time of samples/cpp-history, local Bazel against remex.

Each trial builds and tests //:math_test and //:abseil_test four times, in
this order: local cold, local warm, remote cold, remote warm. Every build
starts from `bazel clean`. Cold means an empty cache: a new local disk cache,
or a new bazel-remote and PostgreSQL for the remote builds. Warm reuses the
cache the cold build filled. Dependency download and analysis happen once,
before any timed build. PostgreSQL, bazel-remote, the scheduler, the workers
and Bazel all run on --cores, and local builds use as many jobs as there are
remote workers, so both sides get the same CPUs.

    scripts/benchmark-cpu.py --output build/cpu-bench --trials 3 > results/cpu-build.jsonl

Prints one JSON line per build; remote builds also summarize the scheduler's
ledger (see ledger_summary). The output directory keeps every log, BEP
file, execution log and ledger; --summarize OUTPUT prints the Bazel timing,
gRPC and ledger summaries of an earlier run again, given as many --jobs as
it had.
"""
import argparse
import base64
import datetime
import hashlib
import json
import os
import pathlib
import platform
import statistics
import subprocess
import sys
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
WORKSPACE = ROOT / "samples/cpp-history"
TARGETS = ["//:math_test", "//:abseil_test"]
ORDER = ["local-cold", "local-warm", "remote-cold", "remote-warm"]
PORTS = dict(postgres="35432", cas="39092", http="38080", grpc="39980", admin="39081")


def run(argv, log, cwd=ROOT, env=None, check=True):
    with open(log, "w") as f:
        code = subprocess.run(argv, cwd=cwd, env=env, stdout=f, stderr=subprocess.STDOUT).returncode
    if check and code:
        raise RuntimeError(f"{argv[0]} failed; see {log}")
    return code


def runner_counts(bep):
    """Bazel's per-strategy action counts, e.g. remote, remote cache hit, linux-sandbox."""
    for line in bep.read_text().splitlines():
        event = json.loads(line)
        if "buildMetrics" in event:
            summary = event["buildMetrics"].get("actionSummary", {})
            counts = {r["name"]: int(r.get("count", 0)) for r in summary.get("runnerCount", [])}
            return int(summary.get("actionsExecuted", 0)), counts
    return None, {}


def bazel_timing(bep):
    """Bazel's own split of the build's wall time, in seconds: analysis, when
    the first action started, and the execution phase."""
    for line in bep.read_text().splitlines():
        timing = json.loads(line).get("buildMetrics", {}).get("timingMetrics")
        if timing:
            return dict(wall_s=int(timing["wallTimeInMs"]) / 1000, analysis_s=int(timing["analysisPhaseTimeInMs"]) / 1000,
                        actions_start_s=int(timing["actionsExecutionStartInMs"]) / 1000,
                        execution_s=int(timing["executionPhaseTimeInMs"]) / 1000)
    return None


def grpc_calls(log):
    """Bazel's remote calls by method: how many, and their summed duration
    (calls overlap, so the sum can exceed the wall time)."""
    report = json.loads(subprocess.run([sys.executable, str(ROOT / "scripts/summarize-grpc-log.py"), str(log)],
                                       capture_output=True, text=True, check=True).stdout)[0]
    return {method.rsplit("/", 1)[1]: dict(calls=d["count"], total_s=round(d["mean"] * d["count"], 2))
            for method, d in report["rpc_duration_seconds"].items()}


# Ledger events that bound each phase of a remote action.
PHASES = {
    "queued": ("execution_created", "lease_granted"),  # waiting for a worker to lease it
    "materialize": ("lease_granted", "command_started"),  # inputs from the CAS, attempt setup
    "run": ("command_started", "uploading"),  # the command itself
    "upload": ("uploading", "attempt_committed"),  # outputs to the CAS, their check, the commit
}


# Timestamps a worker writes into ActionResult.execution_metadata
# (REAPI ExecutedActionMetadata field numbers), and the phases between them.
WORKER_PHASES = {
    "input_fetch": (5, 6),  # inputs from the CAS
    "setup": (6, 7),  # output directories, cgroup, process, RUNNING acknowledged
    "command": (7, 8),
    "output_upload": (9, 10),  # outputs to the CAS
}


def protobuf_fields(data):
    """(field number, value) for each varint or length-delimited field."""
    def varint(at):
        n = shift = 0
        while True:
            byte = data[at]
            n, shift, at = n | (byte & 127) << shift, shift + 7, at + 1
            if not byte & 128:
                return n, at
    at = 0
    while at < len(data):
        key, at = varint(at)
        if key & 7 == 0:
            value, at = varint(at)
        elif key & 7 == 2:
            size, at = varint(at)
            value, at = data[at:at + size], at + size
        else:
            at += {1: 8, 5: 4}[key & 7]
            continue
        yield key >> 3, value


def worker_timestamps(result):
    """The execution_metadata timestamps of a base64 ActionResult, in seconds."""
    for number, metadata in protobuf_fields(base64.b64decode(result)):
        if number == 9:
            return {n: sum(v * scale for f, v in protobuf_fields(stamp) for scale in [{1: 1, 2: 1e-9}.get(f, 0)])
                    for n, stamp in protobuf_fields(metadata) if isinstance(stamp, bytes) and n in range(3, 11)}
    return {}


def ledger_summary(path, seen, workers):
    """Where a remote build's actions spent their time, from the scheduler's
    ledger: executions this build created (not in seen), their median wait
    from creation to lease, the span from the first creation to the last
    commit, the summed lease-to-commit time over all workers, and for each
    phase in PHASES its total over the executions and its median.
    queued_with_idle_worker_s is the queued time during which at least as
    many of the workers held no lease as there were executions queued: time
    the work waited although a worker was free to run it. worker_phases
    splits each committed result's worker time by WORKER_PHASES."""
    def at(event):
        return datetime.datetime.fromisoformat(event["time"].replace("Z", "+00:00")).timestamp()
    times = {}
    ledger = json.loads(path.read_text())
    for event in ledger["events"]:
        if event["kind"] in {kind for phase in PHASES.values() for kind in phase} and event["execution_id"] not in seen:
            times.setdefault(event["execution_id"], {}).setdefault(event["kind"], at(event))
    created = {x: t["execution_created"] for x, t in times.items() if "execution_created" in t}
    seen.update(created)
    if not created or any("attempt_committed" not in times[x] for x in created):
        return dict(executions=len(created), complete=False)
    leased = {x: times[x]["lease_granted"] for x in created}
    committed = {x: times[x]["attempt_committed"] for x in created}
    phases = {}
    for name, (start, end) in PHASES.items():
        # An execution without both events, such as one whose every attempt
        # failed before its command started, is left out of that phase.
        spans = [times[x][end] - times[x][start] for x in created if start in times[x] and end in times[x]]
        phases[name] = dict(total_s=round(sum(spans), 2),
                            median_ms=round(statistics.median(spans) * 1000, 1) if spans else None)
    changes = sorted([(t, 1, 0) for t in created.values()] + [(t, -1, 1) for t in leased.values()] +
                     [(t, 0, -1) for t in committed.values()])
    queued = busy = 0
    idle_wait, last = 0.0, changes[0][0]
    for t, queue_change, busy_change in changes:
        idle_wait += (t - last) * min(queued, max(workers - busy, 0))
        queued, busy, last = queued + queue_change, busy + busy_change, t
    stamps = [worker_timestamps(e["result"]) for e in ledger["executions"] if e["id"] in created and e.get("result")]
    worker_phases = {}
    for name, (start, end) in WORKER_PHASES.items():
        spans = [t[end] - t[start] for t in stamps if start in t and end in t]
        worker_phases[name] = dict(total_s=round(sum(spans), 2),
                                   median_ms=round(statistics.median(spans) * 1000, 1) if spans else None)
    return dict(executions=len(created), complete=True,
                queued_ms_median=round(statistics.median(leased[x] - created[x] for x in created) * 1000, 1),
                scheduler_span_s=round(max(committed.values()) - min(created.values()), 2),
                worker_busy_s=round(sum(committed[x] - leased[x] for x in created), 2),
                phases=phases, queued_with_idle_worker_s=round(idle_wait, 2), worker_phases=worker_phases)


def environment(cores, jobs):
    cpu = next((line.split(":", 1)[1].strip() for line in open("/proc/cpuinfo") if line.startswith("model name")), "")
    bazel = subprocess.run([str(ROOT / ".tools/bazel.official"), "--version"], capture_output=True, text=True).stdout
    commit = subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", str(ROOT), "diff", "--quiet", "HEAD", "--", ".", ":!results"]).returncode
    return dict(commit=commit + ("-dirty" if dirty else ""), cpu=cpu, kernel=platform.release(),
                bazel=bazel.strip(), cores=cores, jobs=jobs, workers=jobs)


class RemoteStack:
    """PostgreSQL and bazel-remote from deploy/compose.yaml under a private
    project name, the scheduler and the workers, all pinned to the cores."""

    def __init__(self, out, cores, workers, cgroup):
        self.out, self.cores, self.workers, self.cgroup = out, cores, workers, cgroup
        self.project = "remexbench" + str(os.getpid())
        override = out / "compose-override.json"
        override.write_text(json.dumps({"services": {"postgres": {"cpuset": cores}, "bazel-remote": {"cpuset": cores}}}))
        self.compose = ["docker", "compose", "-p", self.project, "-f", str(ROOT / "deploy/compose.yaml"), "-f", str(override)]
        self.env = dict(os.environ, REMEX_POSTGRES_PORT=PORTS["postgres"], REMEX_CAS_PORT=PORTS["cas"],
                        REMEX_CACHE_HTTP_PORT=PORTS["http"], REMEX_CACHE_GB="4")
        self.procs, self.logs = [], []

    def start(self, tag):
        run(self.compose + ["up", "-d", "--wait", "postgres", "bazel-remote"], self.out / f"{tag}-compose-up.log", env=self.env)
        self.spawn(tag + "-scheduler", [ROOT / "build/bin/remex-scheduler", "--listen", "127.0.0.1:" + PORTS["grpc"],
                                         "--admin", "127.0.0.1:" + PORTS["admin"], "--cas", "127.0.0.1:" + PORTS["cas"],
                                         "--postgres", f"postgres://remex:remex-local-only@127.0.0.1:{PORTS['postgres']}/remex?sslmode=disable"])
        for _ in range(100):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{PORTS['admin']}/healthz", timeout=1)
                break
            except OSError:
                time.sleep(.1)
        else:
            raise RuntimeError("scheduler did not become healthy")
        for i in range(self.workers):
            self.spawn(f"{tag}-worker-{i}", [ROOT / "build/worker/remex-worker", "--scheduler", "127.0.0.1:" + PORTS["grpc"],
                                              "--cas", "127.0.0.1:" + PORTS["cas"], "--cgroup-root", self.cgroup,
                                              "--root", self.out / f"{tag}-worker-{i}", "--id", f"{self.project}-{tag}-{i}",
                                              "--property", "pool=cpu"])
        time.sleep(1)

    def spawn(self, name, argv):
        log = open(self.out / (name + ".log"), "w")
        self.logs.append(log)
        self.procs.append(subprocess.Popen(["taskset", "-c", self.cores, *map(str, argv)], cwd=ROOT, env=self.env,
                                           stdout=log, stderr=subprocess.STDOUT))

    def ledger(self, path):
        path.write_bytes(urllib.request.urlopen(f"http://127.0.0.1:{PORTS['admin']}/export").read())

    def stop(self, tag):
        for proc in self.procs:
            proc.terminate()
        for proc in self.procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for log in self.logs:
            log.close()
        self.procs, self.logs = [], []
        run(self.compose + ["logs", "--no-color"], self.out / f"{tag}-compose.log", env=self.env, check=False)
        # -v removes this project's volumes, so the next trial starts empty.
        run(self.compose + ["down", "-v"], self.out / f"{tag}-compose-down.log", env=self.env, check=False)


def summarize(out, workers):
    trials = sorted(int(folder.name[len("trial"):-len("-local-cold")]) for folder in out.glob("trial*-local-cold"))
    for trial in trials:
        seen = set()
        for name in ORDER:
            folder = out / f"trial{trial}-{name}"
            if not (folder / "bep.json").is_file():
                continue
            row = dict(trial=trial, build=name, bazel=bazel_timing(folder / "bep.json"))
            if name.startswith("remote"):
                row["grpc"] = grpc_calls(folder / "grpc.bin")
                row["ledger"] = ledger_summary(folder / "ledger.json", seen, workers)
            print(json.dumps(row), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--cores", default="8,9,10")
    parser.add_argument("--jobs", type=int, default=3, help="local jobs and remote workers")
    parser.add_argument("--trials", type=int, default=1)
    parser.add_argument("--output", type=pathlib.Path)
    parser.add_argument("--summarize", type=pathlib.Path, metavar="OUTPUT",
                        help="print the summaries of every build in an earlier --output directory and exit")
    args = parser.parse_args()
    if args.summarize:
        summarize(args.summarize, args.jobs)
        return
    if not args.output:
        parser.error("--output is required")
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    uid = os.getuid()
    cgroup = pathlib.Path(f"/sys/fs/cgroup/user.slice/user-{uid}.slice/user@{uid}.service") / ("remexbench" + str(os.getpid()))
    cgroup.mkdir()
    (cgroup / "cgroup.subtree_control").write_text("+cpu +memory +pids\n")
    stack = RemoteStack(out, args.cores, args.jobs, cgroup)
    env = environment(args.cores, args.jobs)
    bazel = ["taskset", "-c", args.cores, str(ROOT / ".tools/bazel.official"),
             "--output_user_root=" + str(out / "bazel-user"), "--output_base=" + str(out / "bazel-output")]
    try:
        run(bazel + ["build", "--nobuild", *TARGETS, "--lockfile_mode=off"], out / "dependency-setup.log", WORKSPACE)
        for trial in range(1, args.trials + 1):
            for name in ORDER:
                tag = f"trial{trial}-{name}"
                folder = out / tag
                folder.mkdir()
                if name == "remote-cold":
                    stack.start(f"trial{trial}")
                run(bazel + ["clean"], folder / "clean.log", WORKSPACE)
                flags = [f"--jobs={args.jobs}", "--lockfile_mode=off", "--test_output=errors", "--remote_download_outputs=all",
                         "--nocache_test_results", f"--build_event_json_file={folder}/bep.json",
                         f"--execution_log_compact_file={folder}/execution.zstd"]
                if name.startswith("local"):
                    flags += ["--spawn_strategy=local", "--strategy=TestRunner=local",
                              "--disk_cache=" + str(out / f"trial{trial}-disk-cache")]
                else:
                    flags += ["--spawn_strategy=remote", "--strategy=TestRunner=remote", "--noremote_local_fallback",
                              "--remote_executor=grpc://127.0.0.1:" + PORTS["grpc"], "--remote_cache=grpc://127.0.0.1:" + PORTS["cas"],
                              "--remote_instance_name=benchmark", f"--remote_grpc_log={folder}/grpc.bin"]
                command = bazel + ["test", *TARGETS] + flags
                (folder / "command.json").write_text(json.dumps(command, indent=2) + "\n")
                started = time.monotonic()
                code = run(command, folder / "bazel.log", WORKSPACE, check=False)
                elapsed = time.monotonic() - started
                digests = {}
                for target in ("math_test", "abseil_test"):
                    binary = out / "bazel-output/execroot/_main/bazel-out/k8-fastbuild/bin" / target
                    if binary.is_file():
                        digests[target] = hashlib.sha256(binary.read_bytes()).hexdigest()
                actions, runners = runner_counts(folder / "bep.json")
                row = dict(trial=trial, build=name, exit_code=code, seconds=round(elapsed, 2), actions_executed=actions,
                           runners=runners, output_sha256=digests, bazel=bazel_timing(folder / "bep.json"))
                if name == "remote-cold":
                    seen = set()
                if name.startswith("remote"):
                    stack.ledger(folder / "ledger.json")
                    row["grpc"] = grpc_calls(folder / "grpc.bin")
                    row["ledger"] = ledger_summary(folder / "ledger.json", seen, args.jobs)
                row["environment"] = env
                print(json.dumps(row), flush=True)
                if name == "remote-warm":
                    stack.stop(f"trial{trial}")
                if code:
                    raise RuntimeError(f"{tag} failed; see {folder}/bazel.log")
    finally:
        run(bazel + ["shutdown"], out / "bazel-shutdown.log", WORKSPACE, check=False)
        stack.stop("final")
        try:
            cgroup.rmdir()
        except OSError:
            pass


if __name__ == "__main__":
    main()
