#!/usr/bin/env python3
"""Build time of a large C++ build over several machines, and SIGKILLs across them.

Local Bazel on the client machine against remex with 1, 2 and 3 worker
machines, by the method of scripts/benchmark-cpu.py: every build starts from
`bazel clean`; cold means an empty cache (a new disk cache, or a new
bazel-remote and PostgreSQL), warm reuses the cache the cold build filled;
dependency setup and analysis run once before any timed build; each row
carries Bazel's own timing and, for remex, benchmark-cpu.py's ledger summary
(per action: queued, input fetch, command and upload times). Each trial runs
local-cold, local-warm, then remote-N-cold and remote-N-warm for each N.

The client machine runs Bazel, PostgreSQL, bazel-remote and the scheduler.
Each worker machine runs --workers-per-node remex-worker processes with
--isolation process-group. Local builds use --local-jobs jobs (all of the
client's CPUs) and the process-wrapper sandbox: the workload does not build
unsandboxed (an undeclared inclusion in gRPC). remex builds use as many jobs as
there are workers. The workload is a Bazel workspace with its external
repositories vendored (--vendor_dir), so nothing is fetched.

--fault runs --fault-runs cold builds on all machines with each fault named,
in order, instead of the scaling builds. worker-node SIGKILLs every
remex-worker process of the last worker machine --fault-after seconds into the
build, and does not restart them; scheduler SIGKILLs the scheduler and starts
it again --restart-delay seconds later. Each fault row counts the build's executions,
commits, executions committed more than once, executions never committed,
attempts, expired leases and the action processes still alive on the killed
machine a few seconds after the kill.

On a Snowflake ML job (--snowflake; this file is the job's entry point on
instance 0) the worker machines are the job's other instances, reached through
Ray actors pinned to them, and the binaries and workspace come from the asset
stage, checked against SHA256SUMS. With --local-nodes the worker machines are
directories on this host (the dry run).

Every row is printed as `RESULT <json>` and appended to OUTPUT/rows.jsonl;
OUTPUT keeps every Bazel log, BEP file, execution log, gRPC log, ledger export
and process log.
"""
import argparse
import hashlib
import importlib.util
import json
import os
import pathlib
import platform
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import threading
import time
import urllib.request

ASSET_STAGE = "@PORTFOLIO_LAB.GPU.ASSETS"
RESULT_STAGE = "@PORTFOLIO_LAB.GPU.JOBS/remex-results"
PG_USER = "remexpg"


def result(row):
    print("RESULT " + json.dumps(row), flush=True)


def log(message):
    print(time.strftime("%H:%M:%S ") + message, flush=True)


def sh(command):
    try:
        return subprocess.run(command, shell=True, capture_output=True, text=True, timeout=60).stdout.strip()
    except Exception as e:  # noqa: BLE001 - reported, not raised
        return f"error: {e!r}"


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def machine():
    cpu = next((line.split(":", 1)[1].strip() for line in open("/proc/cpuinfo") if line.startswith("model name")), "")
    mem = next((line.split()[1] for line in open("/proc/meminfo") if line.startswith("MemTotal")), "0")
    return dict(host=socket.gethostname(), cpus=os.cpu_count(), cpu=cpu, mem_gib=round(int(mem) / 2**20, 1),
                kernel=platform.release(), gcc=sh("gcc --version | head -1"), glibc=sh("ldd --version | head -1"),
                user=sh("id -un"))


class Node:
    """One worker machine: its remex-worker processes and their directories. A
    janitor deletes the input tree of every finished attempt (one whose
    resources.json the worker has written, after its cleanup and before its
    Complete), because the worker keeps attempt directories and this workload
    would fill the disk."""

    def __init__(self, base):
        self.base = pathlib.Path(base)
        self.procs, self.logs, self.tag = [], [], None
        self.stop_janitor, self.janitor, self.removed = threading.Event(), None, 0

    def info(self):
        info = machine()
        info["ip"] = self.ip()
        return info

    def ip(self):
        try:
            import ray

            if ray.is_initialized():
                return ray.util.get_node_ip_address()
        except ImportError:
            pass
        return "127.0.0.1"

    def install(self, name, data):
        path = self.base / "bin" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        path.chmod(0o755)
        return sha256(path)

    def start(self, tag, count, argv):
        self.tag = tag
        folder = self.base / tag
        folder.mkdir(parents=True)
        for i in range(count):
            log_file = open(folder / f"worker-{i}.log", "w")  # noqa: SIM115 - closed in stop()
            self.logs.append(log_file)
            self.procs.append(subprocess.Popen(
                [str(self.base / "bin/remex-worker"), *argv, "--root", str(folder / f"w{i}"),
                 "--id", f"{tag}-{socket.gethostname()}-{self.base.name}-{i}"],
                stdout=log_file, stderr=subprocess.STDOUT, start_new_session=True))
        self.stop_janitor.clear()
        self.janitor = threading.Thread(target=self.clean_attempts, daemon=True)
        self.janitor.start()
        return [p.pid for p in self.procs]

    def clean_attempts(self):
        while not self.stop_janitor.wait(2):
            for done in (self.base / self.tag).glob("w*/attempts/*/resources.json"):
                root = done.parent / "root"
                if root.exists():
                    shutil.rmtree(root, ignore_errors=True)
                    self.removed += 1

    def alive(self):
        return sum(p.poll() is None for p in self.procs)

    def kill(self, sig):
        """Signal every remex-worker process (not its children: they are what the
        isolation mode must clean up)."""
        at = time.time()
        for p in self.procs:
            if p.poll() is None:
                os.kill(p.pid, sig)
        return at

    def strays(self):
        """Processes, other than the workers, that run in this tag's directories."""
        mine = {p.pid for p in self.procs} | {os.getpid()}
        found = []
        for entry in pathlib.Path("/proc").iterdir():
            if not entry.name.isdigit() or int(entry.name) in mine:
                continue
            try:
                cwd = os.readlink(entry / "cwd")
                if cwd.startswith(str(self.base / self.tag)):
                    found.append(dict(pid=int(entry.name), comm=(entry / "comm").read_text().strip()))
            except OSError:
                continue
        return found

    def stop(self):
        self.stop_janitor.set()
        for p in self.procs:
            if p.poll() is None:
                p.terminate()
        codes = []
        for p in self.procs:
            try:
                codes.append(p.wait(timeout=15))
            except subprocess.TimeoutExpired:
                p.kill()
                codes.append(p.wait())
        for f in self.logs:
            f.close()
        self.procs, self.logs = [], []
        return codes

    def collect(self):
        """The worker logs of the current tag; then its directories are removed."""
        folder = self.base / self.tag
        files = {p.name: p.read_bytes()[-(4 << 20):] for p in sorted(folder.glob("worker-*.log"))}
        files["janitor.json"] = json.dumps(dict(attempt_roots_removed=self.removed)).encode()
        shutil.rmtree(folder, ignore_errors=True)
        return files


class Local:
    """Calls a Node in this process (the dry run)."""

    def __init__(self, node):
        self.node = node

    def __getattr__(self, name):
        return getattr(self.node, name)


class Remote:
    """Calls a Node actor on another machine and waits for the answer."""

    def __init__(self, actor):
        self.actor = actor

    def __getattr__(self, name):
        import ray

        method = getattr(self.actor, name)
        return lambda *args: ray.get(method.remote(*args), timeout=900)


def ray_nodes(count):
    import ray
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    ray.init(address="auto", ignore_reinit_error=True, log_to_driver=False)
    head = ray.get_runtime_context().get_node_id()
    deadline = time.time() + 600
    while True:
        others = sorted((n for n in ray.nodes() if n["Alive"] and n["NodeID"] != head),
                        key=lambda n: n["NodeManagerAddress"])
        if len(others) >= count or time.time() > deadline:
            break
        time.sleep(5)
    if len(others) < count:
        raise RuntimeError(f"{len(others)} other nodes in the Ray cluster, need {count}")
    actor = ray.remote(num_cpus=1)(Node)
    return [Remote(actor.options(scheduling_strategy=NodeAffinitySchedulingStrategy(n["NodeID"], soft=False))
                   .remote("/tmp/remex-node")) for n in others[:count]], ray.util.get_node_ip_address()


class Stack:
    """PostgreSQL, bazel-remote and the scheduler on the client machine."""

    def __init__(self, bin_dir, work, out, ports, cache_gb):
        self.bin, self.work, self.out, self.ports, self.cache_gb = bin_dir, work, out, ports, cache_gb
        self.procs, self.logs, self.scheduler = {}, [], None
        self.env = dict(os.environ, LD_LIBRARY_PATH=str(bin_dir / "pg/lib"))

    def as_pg(self, argv):
        """PostgreSQL refuses to run as root: as root, run it as PG_USER."""
        if os.geteuid():
            return argv
        return ["runuser", "-u", PG_USER, "--", "env", f"LD_LIBRARY_PATH={self.bin / 'pg/lib'}", *argv]

    def spawn(self, name, argv):
        log_file = open(self.out / f"{self.tag}-{name}.log", "a")  # noqa: SIM115 - closed in stop()
        self.logs.append(log_file)
        self.procs[name] = subprocess.Popen(argv, stdout=log_file, stderr=subprocess.STDOUT, env=self.env,
                                            start_new_session=True)

    def start(self, tag):
        self.tag = tag
        pg = self.bin / "pg/bin"
        data, cas = self.work / f"{tag}-pgdata", self.work / f"{tag}-cas"
        for d in (data, cas):
            shutil.rmtree(d, ignore_errors=True)
        data.mkdir(mode=0o700)
        cas.mkdir()
        if os.geteuid() == 0:
            subprocess.run(["sh", "-c", f"id {PG_USER} || useradd -M -s /bin/sh {PG_USER}"], capture_output=True)
            shutil.chown(data, PG_USER, PG_USER)
        with open(self.out / f"{tag}-initdb.log", "w") as f:
            subprocess.run(self.as_pg([str(pg / "initdb"), "-D", str(data), "-U", "remex", "--auth=trust",
                                       "-E", "UTF8", "--locale=C"]), stdout=f, stderr=subprocess.STDOUT,
                           env=self.env, check=True)
        self.spawn("postgres", self.as_pg([str(pg / "postgres"), "-D", str(data), "-p", self.ports["postgres"],
                                           "-c", "listen_addresses=127.0.0.1", "-c", "unix_socket_directories="]))
        for _ in range(100):
            if subprocess.run(self.as_pg([str(pg / "createdb"), "-h", "127.0.0.1", "-p", self.ports["postgres"],
                                          "-U", "remex", "remex"]), capture_output=True, env=self.env).returncode == 0:
                break
            time.sleep(0.2)
        else:
            raise RuntimeError("PostgreSQL did not start")
        self.spawn("bazel-remote", [str(self.bin / "bazel-remote"), f"--dir={cas}", f"--max_size={self.cache_gb}",
                                    f"--http_address=0.0.0.0:{self.ports['http']}",
                                    f"--grpc_address=0.0.0.0:{self.ports['cas']}", "--enable_ac_key_instance_mangling"])
        self.start_scheduler()

    def start_scheduler(self):
        self.spawn("scheduler", [str(self.bin / "remex-scheduler"), "--listen", "0.0.0.0:" + self.ports["grpc"],
                                 "--admin", "127.0.0.1:" + self.ports["admin"], "--cas", "127.0.0.1:" + self.ports["cas"],
                                 "--postgres", f"postgres://remex:remex-local-only@127.0.0.1:{self.ports['postgres']}"
                                               "/remex?sslmode=disable"])
        for _ in range(300):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{self.ports['admin']}/healthz", timeout=1)
                return time.time()
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("scheduler did not become healthy")

    def kill_scheduler(self):
        at = time.time()
        self.procs["scheduler"].send_signal(signal.SIGKILL)
        self.procs["scheduler"].wait()
        return at

    def ledger(self, path):
        path.write_bytes(urllib.request.urlopen(f"http://127.0.0.1:{self.ports['admin']}/export", timeout=300).read())

    def stop(self):
        for name in ("scheduler", "bazel-remote", "postgres"):
            proc = self.procs.get(name)
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
        for f in self.logs:
            f.close()
        self.procs, self.logs = {}, []
        for d in (self.work / f"{self.tag}-pgdata", self.work / f"{self.tag}-cas"):
            shutil.rmtree(d, ignore_errors=True)


def fault_summary(ledger_path, seen):
    """Executions this build created (not in seen), their commits, attempts and expired leases."""
    ledger = json.loads(ledger_path.read_text())
    created = {e["execution_id"] for e in ledger["events"] if e["kind"] == "execution_created"} - seen
    commits, kinds = {}, {}
    for e in ledger["events"]:
        if e.get("execution_id") in created:
            kinds[e["kind"]] = kinds.get(e["kind"], 0) + 1
            if e["kind"] == "attempt_committed":
                commits[e["execution_id"]] = commits.get(e["execution_id"], 0) + 1
    attempts = [a for a in ledger["attempts"] if a["execution_id"] in created]
    states = {}
    for x in ledger["executions"]:
        if x["id"] in created:
            states[x["state"]] = states.get(x["state"], 0) + 1
    return dict(executions=len(created), committed=sum(1 for x in created if commits.get(x)),
                committed_twice=sum(1 for x in created if commits.get(x, 0) > 1),
                never_committed=sum(1 for x in created if not commits.get(x)),
                attempts=len(attempts), rerun_attempts=len(attempts) - len(created), states=states, events=kinds)


class Bench:
    def __init__(self, a, cpu, nodes, client_ip):
        self.a, self.cpu, self.nodes, self.client_ip = a, cpu, nodes, client_ip
        self.out = a.output
        self.work = a.work
        ports = range(a.port_base, a.port_base + 5)
        self.ports = dict(zip(("grpc", "admin", "cas", "http", "postgres"), map(str, ports)))
        self.stack = Stack(a.bin, self.work, self.out, self.ports, a.cache_gb)
        self.bazel = [str(a.bin / "bazel"), f"--output_user_root={self.work}/bazel-user",
                      f"--output_base={self.work}/bazel-output"]
        self.deadline = time.time() + a.wall_minutes * 60
        self.rows = open(self.out / "rows.jsonl", "a")  # noqa: SIM115

    def emit(self, row):
        self.rows.write(json.dumps(row) + "\n")
        self.rows.flush()
        result(row)

    def run(self, argv, log_path, check=True):
        with open(log_path, "w") as f:
            code = subprocess.run(argv, cwd=self.a.workspace, stdout=f, stderr=subprocess.STDOUT).returncode
        if check and code:
            raise RuntimeError(f"{argv[:4]} failed; see {log_path}")
        return code

    def outputs(self):
        binaries = {}
        for target in self.a.targets:
            package, name = target.lstrip("/").split(":")
            path = self.work / "bazel-output/execroot/_main/bazel-out/k8-fastbuild/bin" / package / name
            if path.is_file():
                binaries[target] = sha256(path)
        return binaries

    def command(self, folder, remote_workers=0, disk_cache=None):
        flags = [f"--vendor_dir={self.a.workspace}/vendor", "--remote_download_outputs=all",
                 f"--build_event_json_file={folder}/bep.json", f"--execution_log_compact_file={folder}/execution.zstd"]
        if remote_workers:
            flags += [f"--jobs={remote_workers}", "--spawn_strategy=remote", "--noremote_local_fallback",
                      f"--remote_executor=grpc://127.0.0.1:{self.ports['grpc']}",
                      f"--remote_cache=grpc://127.0.0.1:{self.ports['cas']}", "--remote_instance_name=benchmark",
                      f"--remote_grpc_log={folder}/grpc.bin"]
        else:
            flags += [f"--jobs={self.a.local_jobs}", "--spawn_strategy=processwrapper-sandbox",
                      f"--disk_cache={disk_cache}"]
        return self.bazel + ["build", *self.a.targets] + flags

    def build(self, tag, name, remote_workers=0, disk_cache=None, fault=None):
        """One timed build; fault(started) runs in a thread while Bazel builds and returns a dict."""
        folder = self.out / tag
        folder.mkdir()
        self.run(self.bazel + ["clean"], folder / "clean.log")
        command = self.command(folder, remote_workers, disk_cache)
        (folder / "command.json").write_text(json.dumps(command, indent=2) + "\n")
        fault_result, thread = {}, None
        started = time.monotonic()
        if fault:
            thread = threading.Thread(target=lambda: fault_result.update(fault(started)), daemon=True)
            thread.start()
        code = self.run(command, folder / "bazel.log", check=False)
        elapsed = time.monotonic() - started
        if thread:
            thread.join(timeout=120)
        actions, runners = self.cpu.runner_counts(folder / "bep.json") if (folder / "bep.json").is_file() else (None, {})
        row = dict(build=name, exit_code=code, seconds=round(elapsed, 2), actions_executed=actions, runners=runners,
                   output_sha256=self.outputs(),
                   bazel=self.cpu.bazel_timing(folder / "bep.json") if (folder / "bep.json").is_file() else None)
        if fault_result:
            row["fault"] = fault_result
            if "at_s" in fault_result:
                row["fault"]["to_done_s"] = round(elapsed - fault_result["at_s"], 2)
        if remote_workers:
            self.stack.ledger(folder / "ledger.json")
            row["grpc"] = self.cpu.grpc_calls(folder / "grpc.bin")
        return row, folder

    def start_remote(self, tag, count):
        self.stack.start(tag)
        argv = ["--scheduler", f"{self.client_ip}:{self.ports['grpc']}", "--cas", f"{self.client_ip}:{self.ports['cas']}",
                "--isolation", "process-group", "--rlimit", "core=0", "--property", "pool=cpu"]
        for node in self.nodes[:count]:
            node.start(tag, self.a.workers_per_node, argv)
        time.sleep(2)
        return [node.alive() for node in self.nodes[:count]]

    def stop_remote(self, tag, count):
        codes = []
        for i, node in enumerate(self.nodes[:count]):
            codes.append(node.stop())
            folder = self.out / f"{tag}-node{i + 1}"
            folder.mkdir(exist_ok=True)
            for name, data in node.collect().items():
                (folder / name).write_bytes(data)
        self.stack.stop()
        return codes

    def left(self):
        return self.deadline - time.time()

    def scaling(self, environment):
        for trial in range(1, self.a.trials + 1):
            base = dict(trial=trial, environment=environment)
            if not self.a.skip_local:
                if self.left() < 0:
                    self.emit(dict(base, skipped="wall time"))
                    return
                cache = self.work / f"trial{trial}-disk-cache"
                for name in ("local-cold", "local-warm"):
                    row, _ = self.build(f"trial{trial}-{name}", name, disk_cache=cache)
                    self.emit(dict(base, **row, jobs=self.a.local_jobs))
                shutil.rmtree(cache, ignore_errors=True)
            for count in self.a.nodes:
                if self.left() < 0:
                    self.emit(dict(base, skipped="wall time"))
                    return
                tag = f"trial{trial}-remote{count}"
                workers = count * self.a.workers_per_node
                alive = self.start_remote(tag, count)
                seen = set()
                try:
                    for kind in ("cold", "warm"):
                        name = f"remote-{count}-{kind}"
                        row, folder = self.build(f"{tag}-{kind}", name, remote_workers=workers)
                        row["ledger"] = self.cpu.ledger_summary(folder / "ledger.json", seen, workers)
                        self.emit(dict(base, **row, worker_nodes=count, workers=workers, workers_alive=alive))
                        if row["exit_code"]:
                            break
                finally:
                    self.stop_remote(tag, count)

    def faults(self, environment):
        for kind in self.a.fault:
            for run in range(1, self.a.fault_runs + 1):
                if self.left() < 0:
                    self.emit(dict(fault=kind, run=run, skipped="wall time"))
                    return
                self.fault(kind, run, environment)

    def fault(self, kind, run, environment):
        count = max(self.a.nodes)
        workers = count * self.a.workers_per_node
        tag = f"fault-{kind}-{run}"
        alive = self.start_remote(tag, count)

        def inject(started):
            time.sleep(max(0.0, self.a.fault_after - (time.monotonic() - started)))
            if kind == "worker-node":
                node = self.nodes[count - 1]
                node.kill(signal.SIGKILL)
                at = time.monotonic() - started
                time.sleep(3)
                strays = node.strays()
                return dict(kind="worker-node", node=count, at_s=round(at, 2), workers_killed=self.a.workers_per_node,
                            strays_after_3s=len(strays), stray_commands=sorted({s["comm"] for s in strays}))
            self.stack.kill_scheduler()
            at = time.monotonic() - started
            time.sleep(self.a.restart_delay)
            self.stack.start_scheduler()
            return dict(kind="scheduler", at_s=round(at, 2), restart_delay_s=self.a.restart_delay,
                        back_s=round(time.monotonic() - started - at, 2))

        try:
            row, folder = self.build(tag, f"remote-{count}-cold", remote_workers=workers, fault=inject)
            row["ledger"] = self.cpu.ledger_summary(folder / "ledger.json", set(), workers)
            row["outcome"] = fault_summary(folder / "ledger.json", set())
            self.emit(dict(row, run=run, worker_nodes=count, workers=workers, workers_alive=alive,
                           environment=environment))
        finally:
            self.stop_remote(tag, count)


def fetch_assets(names, work):
    """The named assets and SHA256SUMS into work, from PF_LOCAL_ASSETS or the stage, checked."""
    local = os.environ.get("PF_LOCAL_ASSETS")
    started = time.time()
    if local:
        for name in names + ["SHA256SUMS"]:
            shutil.copy(os.path.join(local, name), work / name)
    else:
        from snowflake.snowpark import Session

        session = Session.builder.getOrCreate()
        for name in names + ["SHA256SUMS"]:
            session.file.get(f"{ASSET_STAGE}/{name}", str(work))
    want = dict(reversed(line.split()) for line in open(work / "SHA256SUMS") if line.strip())
    bad = [n for n in names if sha256(work / n) != want.get(n)]
    if bad:
        raise RuntimeError(f"sha256 mismatch: {bad}")
    for name in names:
        with tarfile.open(work / name) as tar:
            tar.extractall(work, filter="fully_trusted")
        (work / name).unlink()
    return dict(fetch_s=round(time.time() - started, 1), checked=names)


def upload(out, label):
    path = out.parent / f"{label}.tar"
    with tarfile.open(path, "w") as tar:
        tar.add(out, arcname=label)
    if os.environ.get("PF_LOCAL_ASSETS"):
        return dict(tar=str(path), bytes=path.stat().st_size)
    from snowflake.snowpark import Session

    session = Session.builder.getOrCreate()
    put = session.file.put(str(path), f"{RESULT_STAGE}/", auto_compress=False, overwrite=True)
    return dict(tar=f"{RESULT_STAGE}/{path.name}", bytes=path.stat().st_size, status=[str(r.status) for r in put])


def job_argv(argv):
    """The job runner passes only arguments that do not start with '-': there
    NAME=VALUE stands for --NAME VALUE, with a list value separated by commas,
    and NAME alone for the flag --NAME."""
    if not argv or any(arg.startswith("-") for arg in argv):
        return argv
    expanded = []
    for arg in argv:
        name, _, value = arg.partition("=")
        expanded += ["--" + name.replace("_", "-"), *(value.split(",") if value else [])]
    return expanded


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--label", required=True)
    parser.add_argument("--snowflake", action="store_true", help="fetch --assets, worker machines through Ray")
    parser.add_argument("--assets", nargs="*", default=[], help="tars holding bin/ and ws/ (with --snowflake)")
    parser.add_argument("--work", type=pathlib.Path, default=pathlib.Path(os.environ.get("PF_WORK", "/tmp/remex-sf")))
    parser.add_argument("--bin", type=pathlib.Path, help="remex-scheduler, remex-worker, bazel-remote, bazel, pg/")
    parser.add_argument("--workspace", type=pathlib.Path, help="the workload, with its vendor/ directory")
    parser.add_argument("--targets", nargs="+", default=["//worker:remex-worker"])
    parser.add_argument("--nodes", nargs="+", type=int, default=[1, 2, 3], help="worker machine counts to measure")
    parser.add_argument("--local-nodes", type=int, default=0, help="dry run: this many worker 'machines' on this host")
    parser.add_argument("--workers-per-node", type=int, default=8)
    parser.add_argument("--local-jobs", type=int, default=os.cpu_count())
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--skip-local", action="store_true")
    parser.add_argument("--fault", nargs="+", choices=["worker-node", "scheduler"],
                        help="run --fault-runs cold builds with each fault, in this order")
    parser.add_argument("--fault-runs", type=int, default=3)
    parser.add_argument("--fault-after", type=float, default=60)
    parser.add_argument("--restart-delay", type=float, default=1.0)
    parser.add_argument("--port-base", type=int, default=12960)
    parser.add_argument("--cache-gb", type=int, default=32)
    parser.add_argument("--wall-minutes", type=float, default=150, help="start no build after this")
    a = parser.parse_args(job_argv(sys.argv[1:]))
    a.work.mkdir(parents=True, exist_ok=True)
    a.work = a.work.resolve()
    a.output = a.work / "out" / a.label
    a.output.mkdir(parents=True)
    environment = dict(client=machine(), label=a.label, argv=sys.argv[1:], driver_sha256=sha256(__file__))
    if a.assets:
        environment["assets"] = fetch_assets(a.assets, a.work)
    a.bin = (a.bin or a.work / "bin").resolve()
    a.workspace = (a.workspace or a.work / "ws").resolve()
    for name in ("remex-scheduler", "remex-worker", "bazel-remote", "bazel"):
        environment.setdefault("binaries", {})[name] = sha256(a.bin / name)
    environment["workspace"] = (a.workspace / "SOURCE").read_text().strip() if (a.workspace / "SOURCE").exists() else None
    spec = importlib.util.spec_from_file_location("benchmark_cpu", a.workspace / "scripts/benchmark-cpu.py")
    cpu = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cpu)
    count = max(a.nodes)
    if a.snowflake:
        nodes, client_ip = ray_nodes(count)
    else:
        nodes = [Local(Node(a.work / f"node{i + 1}")) for i in range(a.local_nodes or count)]
        client_ip = "127.0.0.1"
    worker_binary = (a.bin / "remex-worker").read_bytes()
    environment["nodes"] = []
    for node in nodes:
        info = node.info()
        info["remex_worker_sha256"] = node.install("remex-worker", worker_binary)
        environment["nodes"].append(info)
    result(dict(environment=environment))
    (a.output / "environment.json").write_text(json.dumps(environment, indent=1) + "\n")
    bench = Bench(a, cpu, nodes, client_ip)
    try:
        bench.run(bench.bazel + ["build", "--nobuild", *a.targets, f"--vendor_dir={a.workspace}/vendor"],
                  a.output / "dependency-setup.log")
        if a.fault:
            bench.faults(environment)
        else:
            bench.scaling(environment)
    finally:
        bench.run(bench.bazel + ["shutdown"], a.output / "bazel-shutdown.log", check=False)
        bench.rows.close()
        try:
            result(dict(upload=upload(a.output, a.label)))
        except Exception as e:  # noqa: BLE001 - the RESULT lines in the log still hold every row
            result(dict(upload_error=repr(e)[:1000]))


if __name__ == "__main__":
    main()
