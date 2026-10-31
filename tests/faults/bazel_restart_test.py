#!/usr/bin/env python3
"""Real Bazel + PostgreSQL scheduler crash/restart test in an isolated deployment.

Only processes, a database and a cgroup subtree created by this test are killed
or removed. Existing project services and the shared CAS are never restarted.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import time
import urllib.parse
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
# The worker that make worker-build produces; REMEX_TEST_WORKER_BINARY overrides it.
WORKER = Path(os.environ.get("REMEX_TEST_WORKER_BINARY", ROOT / "build/worker/remex-worker"))
LEASE = 2.0  # seconds; also the scheduler's recovery grace


def save(path, data):
    path.write_text(json.dumps(data, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cgroup-root", required=True, type=Path)
    parser.add_argument("--artifacts", type=Path)
    parser.add_argument("--worker", type=Path, default=WORKER)
    parser.add_argument("--scheduler", type=Path, default=ROOT / "build/bin/remex-scheduler")
    parser.add_argument("--bazel", type=Path, default=ROOT / ".tools/bazel.official")
    parser.add_argument("--cas", default="127.0.0.1:19092")
    parser.add_argument(
        "--postgres", default="postgres://remex:remex-local-only@127.0.0.1:15432/postgres?sslmode=disable"
    )
    parser.add_argument("--grpc-port", type=int, default=21980)
    parser.add_argument("--admin-port", type=int, default=21081)
    parser.add_argument(
        "--capture", action="store_true", help="record original cache/worker RPCs, adapt and independently check"
    )
    parser.add_argument(
        "--fault",
        choices=(
            "scheduler-queued",
            "scheduler-leased",
            "scheduler-running",
            "scheduler-uploading",
            "scheduler-before-ac",
            "scheduler-after-ac",
            "worker-materializing",
            "worker-running",
            "worker-uploading",
            "worker-stopped",
        ),
        default="scheduler-running",
    )
    parser.add_argument(
        "--restart-delay",
        type=float,
        default=0.8,
        help="seconds a killed scheduler stays down; from one lease up, no running attempt can survive",
    )
    args = parser.parse_args()
    token = uuid.uuid4().hex[:12]
    output = (args.artifacts or ROOT / "evidence/runs" / ("scheduler-restart-" + token)).resolve()
    output.mkdir(parents=True, exist_ok=False)
    work = ROOT / "build" / ("independent-restart-" + token)
    work.mkdir()
    fixture = work / "fixture"
    fixture.mkdir()
    dbname = "remex_restart_" + token
    parsed = urllib.parse.urlsplit(args.postgres)
    dsn = urllib.parse.urlunsplit(parsed._replace(path="/" + dbname))
    cgroup = args.cgroup_root / ("independent-bazel-restart-" + token)
    address = f"127.0.0.1:{args.grpc_port}"
    admin = f"http://127.0.0.1:{args.admin_port}"
    capture_cache = f"127.0.0.1:{args.grpc_port + 10}"
    capture_worker = f"127.0.0.1:{args.grpc_port + 11}"
    publication_fault = args.fault in ("scheduler-before-ac", "scheduler-after-ac")
    gate = work / "pause-ac"
    gate_observed = output / "gate-observed.json"
    lease_gate = work / "hold-leases"
    after_lease_gate = work / "hold-first-lease-response"
    processes = []
    logs = []
    database_created = False
    observations = []

    def observe(event, **details):
        row = {"event": event, "time": datetime.now(timezone.utc).isoformat(), "monotonic": time.monotonic(), **details}
        observations.append(row)
        with (output / "observations.jsonl").open("a") as f:
            f.write(json.dumps(row) + "\n")

    def launch(command, logname, cwd=ROOT):
        log = (output / logname).open("wb")
        logs.append(log)
        process = subprocess.Popen([str(x) for x in command], cwd=cwd, stdout=log, stderr=subprocess.STDOUT)
        processes.append(process)
        return process

    def ledger():
        with urllib.request.urlopen(admin + "/audit/durable-export", timeout=1) as response:
            return json.load(response)

    def stop(process):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)

    def wait_for(predicate, seconds, description):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            time.sleep(0.01)
        raise AssertionError("timeout waiting for " + description)

    def healthy():
        try:
            with urllib.request.urlopen(admin + "/healthz", timeout=0.5) as response:
                return response.status == 200
        except OSError:
            return False

    try:
        for port in (
            args.grpc_port,
            args.admin_port,
            args.grpc_port + 10,
            args.grpc_port + 11,
            args.grpc_port + 12,
            args.grpc_port + 100,
        ):
            with socket.socket() as check:
                check.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                check.bind(("127.0.0.1", port))
        subprocess.run(
            ["psql", args.postgres, "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE " + dbname],
            check=True,
            capture_output=True,
        )
        database_created = True
        cgroup.mkdir()
        (cgroup / "cgroup.subtree_control").write_text("+cpu +memory +pids")
        for name in ("MODULE.bazel", "MODULE.bazel.lock"):
            shutil.copy2(ROOT / "samples/cpp-history" / name, fixture / name)
        marker = "independent-restart-" + token
        if args.fault == "worker-materializing":
            (fixture / "payload.bin").write_bytes(os.urandom(64 << 20))
            rule = (
                'genrule(name="restart_probe", srcs=["payload.bin"], outs=["restart.out"], cmd="cat $(location payload.bin) >/dev/null; printf '
                + marker
                + ' > $@", exec_properties={"pool":"cpu", "OSFamily":"Linux", "ISA":"x86-64"})\n'
            )
        elif args.fault in ("worker-uploading", "scheduler-uploading"):
            rule = (
                'genrule(name="restart_probe", outs=["restart.out", "payload.out"], cmd="dd if=/dev/urandom of=$(RULEDIR)/payload.out bs=1048576 count=64 status=none; printf '
                + marker
                + ' > $(RULEDIR)/restart.out", exec_properties={"pool":"cpu", "OSFamily":"Linux", "ISA":"x86-64"})\n'
            )
        else:
            rule = (
                'genrule(name="restart_probe", outs=["restart.out"], cmd="sleep 8; printf '
                + marker
                + ' > $@", exec_properties={"pool":"cpu", "OSFamily":"Linux", "ISA":"x86-64"})\n'
            )
        (fixture / "BUILD.bazel").write_text(rule)
        all_cas = capture_cache if args.capture else args.cas
        worker_scheduler = capture_worker if args.capture else address
        if args.capture:
            recorder = launch(
                [
                    ROOT / "build/bin/remex-record",
                    "--cache-listen",
                    capture_cache,
                    "--cache-target",
                    args.cas,
                    "--worker-listen",
                    capture_worker,
                    "--worker-target",
                    address,
                    "--cgroup-root",
                    cgroup,
                    "--ledger-url",
                    admin + "/audit/durable-export",
                    "--out",
                    output / "raw-capture.jsonl",
                ],
                "recorder.log",
            )
            wait_for(
                lambda: (output / "recorder.log").read_text().count("->") == 2, 10, "both original recorder listeners"
            )
        scheduler_cas = all_cas
        if publication_fault:
            scheduler_cas = f"127.0.0.1:{args.grpc_port + 100}"
            gate.write_text("pause")
            gate_binary = work / "cachegate"
            subprocess.run(
                ["go", "build", "-o", str(gate_binary), "./tests/faults/cachegate"],
                cwd=ROOT,
                check=True,
                capture_output=True,
            )
            launch(
                [
                    gate_binary,
                    "--listen",
                    scheduler_cas,
                    "--upstream",
                    all_cas,
                    "--gate",
                    gate,
                    "--observed",
                    gate_observed,
                    "--position",
                    "after" if args.fault == "scheduler-after-ac" else "before",
                    "--log",
                    output / "ac-proxy.jsonl",
                ],
                "ac-proxy.log",
            )
        scheduler_command = [
            args.scheduler,
            "--listen",
            address,
            "--admin",
            f"127.0.0.1:{args.admin_port}",
            "--cas",
            scheduler_cas,
            "--postgres",
            dsn,
            "--lease-duration",
            f"{LEASE:g}s",
            "--recovery-grace",
            f"{LEASE:g}s",
            "--max-attempts",
            "3",
        ]
        scheduler = launch(scheduler_command, "scheduler-before.log")
        wait_for(healthy, 10, "initial scheduler health")
        if args.fault in ("scheduler-queued", "scheduler-leased", "worker-stopped"):
            lease_target = worker_scheduler
            worker_scheduler = f"127.0.0.1:{args.grpc_port + 12}"
            if args.fault == "scheduler-queued":
                lease_gate.write_text("hold queued execution")
            extra = []
            if args.fault == "scheduler-leased":
                after_lease_gate.write_text("hold persisted Lease response")
                extra = ["--after-lease-gate", after_lease_gate, "--observed", output / "lease-response.json"]
            launch(
                [
                    ROOT / "build/bin/remex-test-leasegate",
                    "--listen",
                    worker_scheduler,
                    "--target",
                    lease_target,
                    "--gate",
                    lease_gate,
                    *extra,
                ],
                "leasegate.log",
            )
        worker_command = [
            args.worker,
            "--scheduler",
            worker_scheduler,
            "--cas",
            all_cas,
            "--cgroup-root",
            cgroup,
            "--root",
            work / "worker",
            "--id",
            "restart-worker-" + token,
            "--property",
            "pool=cpu",
        ]
        worker = launch(worker_command, "worker.log")
        wait_for(lambda: "registered worker=" in (output / "worker.log").read_text(), 10, "worker registration")
        command = [
            str(args.bazel),
            "--batch",
            "--ignore_all_rc_files",
            "--output_user_root=" + str(work / "bazel-state"),
            "build",
            "//:restart_probe",
            "--repository_cache=" + str(ROOT / ".cache/bazel/cache/repos/v1"),
            "--remote_executor=grpc://" + address,
            "--remote_cache=grpc://" + all_cas,
            "--remote_instance_name=independent-restart-" + token,
            "--noremote_accept_cached",
            "--noremote_local_fallback",
            "--spawn_strategy=remote",
            "--jobs=1",
            "--remote_retries=6",
            "--remote_timeout=60s",
            "--remote_grpc_log=" + str(output / "bazel-grpc.bin"),
            "--execution_log_compact_file=" + str(output / "execution.zstd"),
            "--build_event_json_file=" + str(output / "bep.json"),
        ]
        save(
            output / "commands.json",
            {
                "bazel": command,
                "scheduler": [x if x != dsn else "<isolated-test-database-dsn>" for x in map(str, scheduler_command)],
                "database": dbname,
                "cgroup": str(cgroup),
                "worker_sha256": hashlib.sha256(args.worker.read_bytes()).hexdigest(),
                "scheduler_sha256": hashlib.sha256(args.scheduler.read_bytes()).hexdigest(),
            },
        )
        bazel = launch(command, "bazel.log", fixture)
        target_phase = {
            "scheduler-queued": "Queued",
            "scheduler-leased": "Leased",
            "scheduler-running": "Running",
            "scheduler-uploading": "Uploading",
            "scheduler-before-ac": "Committed",
            "scheduler-after-ac": "Committed",
            "worker-materializing": "Materializing",
            "worker-running": "Running",
            "worker-uploading": "Uploading",
            "worker-stopped": "Running",
        }[args.fault]

        def running():
            if bazel.poll() is not None:
                raise AssertionError("Bazel exited before Running: " + (output / "bazel.log").read_text()[-6000:])
            current = ledger()
            if publication_fault and not gate_observed.exists():
                return None
            if args.fault in ("scheduler-queued", "scheduler-leased"):
                if args.fault == "scheduler-leased" and not (output / "lease-response.json").exists():
                    return None
                return current if any(e["state"] == target_phase for e in current["executions"]) else None
            return current if any(a["phase"] == target_phase for a in current["attempts"]) else None

        before = wait_for(running, 90, "actual remote attempt " + target_phase)
        save(output / "ledger-before-kill.json", before)
        old = (
            None
            if args.fault == "scheduler-queued"
            else next(
                a
                for a in before["attempts"]
                if a["phase"] == ("Materializing" if args.fault == "scheduler-leased" else target_phase)
            )
        )
        if publication_fault:
            if (
                any(e["kind"] == "ac_published" for e in before["events"])
                or before["executions"][0]["winner_attempt_id"] != old["id"]
            ):
                raise AssertionError("selected publication boundary lacks a committed, unpublished winner")
        if args.fault.startswith("scheduler-"):
            observe(
                "scheduler_sigkill",
                pid=scheduler.pid,
                attempt=old["id"] if old else None,
                epoch=old["epoch"] if old else 1,
            )
            scheduler.kill()
            scheduler.wait(timeout=5)
            if publication_fault:
                wait_for(
                    lambda: any(
                        json.loads(line)["kind"] == "gate_cancelled"
                        for line in (output / "ac-proxy.jsonl").read_text().splitlines()
                    ),
                    5,
                    "killed scheduler's gated AC RPC cancellation",
                )
                gate.unlink()
            time.sleep(args.restart_delay)
            recovered = launch(scheduler_command, "scheduler-after.log")
            wait_for(healthy, 10, "restarted scheduler health")
            observe("scheduler_restarted", pid=recovered.pid)
            if args.fault == "scheduler-queued":
                lease_gate.unlink()
            elif args.fault == "scheduler-leased":
                time.sleep(2.2)
                after_lease_gate.unlink()
        elif args.fault == "worker-stopped":
            lease_gate.write_text("hold next lease until old subtree is independently empty")
            worker.send_signal(signal.SIGSTOP)
            observe(
                "worker_sigstop",
                pid=worker.pid,
                attempt=old["id"],
                epoch=old["epoch"],
                cgroup_events=(cgroup / "cgroup.events").read_text(),
            )
            time.sleep(3)
            worker.send_signal(signal.SIGCONT)
            observe("worker_sigcont", pid=worker.pid)
            wait_for(
                lambda: "populated 0" in (cgroup / "cgroup.events").read_text(),
                3,
                "resumed expired worker cleared old subtree before next Lease",
            )
            observe(
                "expired_worker_cgroup_empty_before_next_lease", cgroup_events=(cgroup / "cgroup.events").read_text()
            )
            lease_gate.unlink()
        else:
            observe(
                "worker_sigkill", pid=worker.pid, attempt=old["id"], epoch=old["epoch"], observed_phase=target_phase
            )
            worker.kill()
            worker.wait(timeout=5)
            wait_for(
                lambda: "populated 0" in (cgroup / "cgroup.events").read_text(),
                3,
                "killed worker's action subtree empty",
            )
            observe("worker_cgroup_empty_before_replacement")
            time.sleep(0.8)
            replacement = launch(worker_command, "worker-after.log")
            observe("worker_restarted", pid=replacement.pid)
        status = bazel.wait(timeout=90)
        observe("bazel_finished", exit_code=status)
        if status != 0:
            raise AssertionError("Bazel failed after scheduler restart: " + (output / "bazel.log").read_text()[-6000:])
        if (fixture / "bazel-bin/restart.out").read_text() != marker:
            raise AssertionError("Bazel output differs from actual command output")
        built = ledger()
        save(output / "ledger-after-build.json", built)
        # A worker retries heartbeats that fail in transport until its own lease
        # runs out, so a running attempt outlives a scheduler that is back
        # sooner: the restarted scheduler recovers the lease and the attempt can
        # still win. An attempt that has finished only has to deliver Complete,
        # which waits up to one RPC deadline for the scheduler, so it can win
        # after a longer outage too. A lease response held past the lease, or a
        # killed or stopped worker, leaves the action to a retry.
        won = old is not None and built["executions"][0]["winner_attempt_id"] == old["id"]
        could_survive = args.fault == "scheduler-uploading" or (
            args.fault == "scheduler-running" and args.restart_delay < LEASE
        )
        if won and not publication_fault and not could_survive:
            raise AssertionError("an attempt won after its lease had run out")
        probe = work / "restartprobe"
        subprocess.run(
            ["go", "build", "-o", str(probe), "./tests/faults/restartprobe"], cwd=ROOT, check=True, capture_output=True
        )
        replay_data = None
        if old:
            replay = subprocess.run(
                [
                    str(probe),
                    "--scheduler",
                    address,
                    "--worker",
                    old["worker_id"],
                    "--attempt",
                    old["id"],
                    "--epoch",
                    str(old["epoch"]),
                    "--expected-complete",
                    "ACCEPTED" if won else "FENCED",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            replay_data = json.loads(replay.stdout)
            save(output / "stale-replay.json", replay_data)
        wait_for(
            lambda: any(e["kind"] == "ac_published" for e in ledger()["events"]),
            10,
            "durable AC publication after recovery",
        )
        final = ledger()
        save(output / "ledger-final.json", final)
        commits = [e for e in final["events"] if e["kind"] == "attempt_committed"]
        published = [e for e in final["events"] if e["kind"] == "ac_published"]
        if len(final["executions"]) != 1 or len(commits) != 1 or len(published) != 1:
            raise AssertionError("restart did not preserve a unique execution/commit/publication")
        execution = final["executions"][0]
        if won:
            # Committed before the kill (publication faults) or after recovery;
            # either way the stale replay must not change what was committed.
            reference = before if publication_fault else built
            if (
                execution["current_epoch"] != old["epoch"]
                or execution["winner_attempt_id"] != old["id"]
                or len(final["attempts"]) != 1
            ):
                raise AssertionError("recovery changed the committed winner")
            if (
                execution["result"] != reference["executions"][0]["result"]
                or final["attempts"][0]["result"] != reference["attempts"][0]["result"]
            ):
                raise AssertionError("recovery or duplicate Complete changed the committed payload")
            if not publication_fault and not any(
                e["kind"] == "lease_recovered" and e["attempt_id"] == old["id"] for e in final["events"]
            ):
                raise AssertionError("old attempt won without the restarted scheduler recovering its lease")
        elif publication_fault:
            raise AssertionError("publication recovery changed the already committed winner")
        elif old and (execution["current_epoch"] <= old["epoch"] or execution["winner_attempt_id"] == old["id"]):
            raise AssertionError("old failed attempt unexpectedly won")
        elif old is None and (len(final["attempts"]) != 1 or execution["current_epoch"] != 1):
            raise AssertionError("queued recovery unnecessarily duplicated an unleased execution")
        summary = subprocess.run(
            ["python3", str(ROOT / "scripts/summarize-grpc-log.py"), str(output / "bazel-grpc.bin")],
            check=True,
            capture_output=True,
            text=True,
        )
        rpc = json.loads(summary.stdout)
        save(output / "bazel-rpc-summary.json", rpc)
        waits = sum(count for method, count in rpc[0]["methods"].items() if method.endswith("Execution/WaitExecution"))
        # A stream reports the result once it is committed, before the action
        # cache write, so at a publication fault Bazel may be done already.
        if args.fault.startswith("scheduler-") and not publication_fault and waits < 1:
            raise AssertionError("no actual Bazel WaitExecution RPC was captured")
        populated = (cgroup / "cgroup.events").read_text()
        if "populated 0" not in populated:
            raise AssertionError("completed test left a live action process")
        result = {
            "ok": True,
            "fault": args.fault,
            "observed_phase_before_fault": target_phase,
            "bazel_exit": status,
            "wait_execution_calls": waits,
            "executions": len(final["executions"]),
            "attempts": len(final["attempts"]),
            "current_epoch": execution["current_epoch"],
            "commits": len(commits),
            "ac_publications": len(published),
            "old_attempt": "none"
            if old is None
            else "committed before the fault"
            if publication_fault
            else "recovered and won"
            if won
            else "retried",
            "old_epoch_replay": replay_data,
            "cgroup_events_before_cleanup": populated,
            "note": "Actual process signal and recovery; stale-token replay is a separate explicit protocol probe.",
        }
        if publication_fault:
            records = [json.loads(line) for line in (output / "ac-proxy.jsonl").read_text().splitlines()]
            writes = [row for row in records if row["kind"] == "upstream_write_succeeded"]
            expected_writes = 2 if args.fault == "scheduler-after-ac" else 1
            if len(writes) != expected_writes or any(
                row["action_digest"] != writes[0]["action_digest"] or row["result"] != writes[0]["result"]
                for row in writes
            ):
                raise AssertionError("AC recovery writes were not the expected identical committed result")
            result["successful_upstream_ac_writes"] = len(writes)
            result["note"] = (
                "Real scheduler SIGKILL at a gated AC boundary. Repeated AC writes, when present, have identical key/result; one durable commit and logical publication."
            )
        if args.capture:
            for process in reversed(processes):
                if process != recorder:
                    stop(process)
            time.sleep(0.2)
            stop(recorder)
            with (output / "normalized.jsonl").open("wb") as normalized, (output / "adapter.log").open("wb") as errors:
                adapted = subprocess.run(
                    [ROOT / "build/bin/remex-adapt", "--capture", output / "raw-capture.jsonl"],
                    stdout=normalized,
                    stderr=errors,
                )
            with (output / "checker.json").open("wb") as checked:
                checker = subprocess.run(
                    [
                        ROOT / "build/bin/remex-checker",
                        "--ledger",
                        output / "ledger-final.json",
                        "--evidence",
                        output / "normalized.jsonl",
                    ],
                    stdout=checked,
                    stderr=subprocess.PIPE,
                )
            result["checker"] = json.loads((output / "checker.json").read_text())
            save(output / "process-result.json", result)
            if adapted.returncode != 0 or checker.returncode != 0:
                raise AssertionError(
                    "original-capture adapter/checker did not pass; actual process result saved separately"
                )
        save(output / "result.json", result)
        print(json.dumps({"artifacts": str(output), **result}, indent=2))
    except BaseException as error:
        save(output / "failure.json", {"type": type(error).__name__, "message": str(error)})
        raise
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
        for process in reversed(processes):
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        for log in logs:
            log.close()
        if cgroup.exists():
            (cgroup / "cgroup.kill").write_text("1")
            time.sleep(0.05)
            for child in cgroup.iterdir():
                if child.is_dir():
                    child.rmdir()
            cgroup.rmdir()
        if database_created:
            subprocess.run(
                ["psql", args.postgres, "-v", "ON_ERROR_STOP=1", "-c", "DROP DATABASE " + dbname],
                check=True,
                capture_output=True,
            )


if __name__ == "__main__":
    main()
