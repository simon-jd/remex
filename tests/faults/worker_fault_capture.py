#!/usr/bin/env python3
"""Precise actual REAPI/worker fault scenarios with original RPC evidence.

The standalone official-REAPI client sets the actual Action timeout. These are
not described as Bazel invocations. All processes/DB/cgroup belong to this run.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fault",
        choices=(
            "oom",
            "timeout-setm",
            "success-setm",
            "cancel-before-deadline",
            "cancel-at-deadline",
            "cancel-after-deadline",
        ),
        required=True,
    )
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--cgroup-root", type=Path, required=True)
    parser.add_argument("--base-port", type=int, default=23980)
    parser.add_argument("--cas", default="127.0.0.1:19092")
    parser.add_argument(
        "--postgres", default="postgres://remex:remex-local-only@127.0.0.1:15432/postgres?sslmode=disable"
    )
    args = parser.parse_args()
    output = args.artifacts.resolve()
    output.mkdir(parents=True, exist_ok=False)
    token = uuid.uuid4().hex[:12]
    work = ROOT / "build" / ("independent-worker-capture-" + token)
    work.mkdir()
    database = "remex_worker_" + token
    dsn = urllib.parse.urlunsplit(urllib.parse.urlsplit(args.postgres)._replace(path="/" + database))
    cgroup = args.cgroup_root / ("independent-worker-capture-" + token)
    address, cache, worker_address = [f"127.0.0.1:{args.base_port + n}" for n in (0, 2, 3)]
    admin = f"http://127.0.0.1:{args.base_port + 1}"
    processes, logs = [], []
    database_created = False

    def save(name, value):
        (output / name).write_text(json.dumps(value, indent=2) + "\n")

    def observe(kind, **details):
        with (output / "observations.jsonl").open("a") as file:
            file.write(
                json.dumps(
                    {
                        "kind": kind,
                        "time": datetime.now(timezone.utc).isoformat(),
                        "monotonic": time.monotonic(),
                        **details,
                    }
                )
                + "\n"
            )

    def launch(command, name):
        log = (output / name).open("wb")
        logs.append(log)
        p = subprocess.Popen(list(map(str, command)), cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        processes.append(p)
        return p

    def stop(p):
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=3)

    def wait_for(predicate, timeout, what):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(0.005)
        raise AssertionError("timeout waiting for " + what)

    def healthy():
        try:
            return urllib.request.urlopen(admin + "/healthz", timeout=1).status == 200
        except OSError:
            return False

    def ledger():
        with urllib.request.urlopen(admin + "/audit/durable-export", timeout=2) as response:
            return json.load(response)

    try:
        for port in range(args.base_port, args.base_port + 4):
            with socket.socket() as check:
                check.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                check.bind(("127.0.0.1", port))
        subprocess.run(
            ["psql", args.postgres, "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE " + database],
            check=True,
            capture_output=True,
        )
        database_created = True
        cgroup.mkdir()
        (cgroup / "cgroup.subtree_control").write_text("+cpu +memory +pids")
        if args.fault == "oom":
            (cgroup / "memory.max").write_text("67108864")
            (cgroup / "memory.swap.max").write_text("0")
        scheduler = launch(
            [
                ROOT / "build/bin/remex-scheduler",
                "--listen",
                address,
                "--admin",
                admin.removeprefix("http://"),
                "--cas",
                cache,
                "--postgres",
                dsn,
                "--lease-duration",
                "2s",
                "--max-attempts",
                "1",
            ],
            "scheduler.log",
        )
        wait_for(healthy, 10, "own scheduler health")
        recorder = launch(
            [
                ROOT / "build/bin/remex-record",
                "--cache-listen",
                cache,
                "--cache-target",
                args.cas,
                "--worker-listen",
                worker_address,
                "--worker-target",
                address,
                "--ledger-url",
                admin + "/audit/durable-export",
                "--cgroup-root",
                cgroup,
                "--out",
                output / "raw-capture.jsonl",
            ],
            "recorder.log",
        )
        wait_for(lambda: (output / "recorder.log").read_text().count("->") == 2, 10, "both recorder listeners")
        worker = launch(
            [
                WORKER,
                "--scheduler",
                worker_address,
                "--cas",
                cache,
                "--root",
                work / "worker",
                "--cgroup-root",
                cgroup,
                "--id",
                "independent-" + token,
                "--property",
                "pool=cpu",
                "--once",
            ],
            "worker.log",
        )
        wait_for(lambda: "registered worker=" in (output / "worker.log").read_text(), 10, "actual worker registration")
        timeout = "300ms"
        argv = ["/bin/bash", "-c", "set -m; sleep 60 & echo REMEX_CHILD:$! >&2; wait"]
        if args.fault == "oom":
            argv = ["/usr/bin/python3", "-c", "x=bytearray(256*1024*1024)"]
            timeout = "5s"
        elif args.fault == "success-setm":
            argv = ["/bin/bash", "-c", "set -m; sleep 60 & echo REMEX_CHILD:$! >&2; printf escaped-job-launched"]
            timeout = "5s"
        command = [
            ROOT / "build/bin/remex-test-actionprobe",
            "--scheduler",
            address,
            "--cas",
            cache,
            "--instance",
            "independent-worker-" + token,
            "--argv",
            json.dumps(argv),
            "--timeout",
            timeout,
            "--out",
            output,
        ]
        save("command.json", {"client": list(map(str, command)), "fault": args.fault})
        client = launch(command, "client.json")
        if args.fault.startswith("cancel-"):
            running = wait_for(
                lambda: (lambda value: value if any(a["phase"] == "Running" for a in value["attempts"]) else None)(
                    ledger()
                ),
                10,
                "actual command Running",
            )
            attempt = running["attempts"][0]
            started = datetime.fromisoformat(attempt["command_started_at"].replace("Z", "+00:00"))
            offset = {"cancel-before-deadline": 0.1, "cancel-at-deadline": 0.3, "cancel-after-deadline": 0.5}[
                args.fault
            ]
            remaining = offset - (datetime.now(timezone.utc) - started).total_seconds()
            if remaining > 0:
                time.sleep(remaining)
            observe(
                "cancel_requested",
                execution_id=attempt["execution_id"],
                scheduled_seconds_after_start=offset,
                observed_seconds_after_start=(datetime.now(timezone.utc) - started).total_seconds(),
            )
            request = urllib.request.Request(
                admin + "/cancel?id=" + urllib.parse.quote(attempt["execution_id"], safe=""), method="POST"
            )
            with urllib.request.urlopen(request, timeout=3) as response:
                if response.status != 204:
                    raise AssertionError("cancel rejected")
        if client.wait(timeout=30) != 0:
            raise AssertionError("actual REAPI client transport failed: " + (output / "client.json").read_text())
        response = json.loads((output / "client.json").read_text())
        expected = {
            "oom": {8},
            "timeout-setm": {4},
            "success-setm": {0},
            "cancel-before-deadline": {1},
            "cancel-at-deadline": {1, 4},
            "cancel-after-deadline": {4},
        }[args.fault]
        if response["status_code"] not in expected:
            raise AssertionError("incorrect terminal status: " + str(response))
        wait_for(
            lambda: "populated 0" in (cgroup / "cgroup.events").read_text(),
            5,
            "all actual command descendants empty before cleanup",
        )
        observe(
            "cgroup_empty_before_cleanup",
            cgroup_events=(cgroup / "cgroup.events").read_text(),
            memory_events=(cgroup / "memory.events").read_text(),
        )
        if worker.wait(timeout=5) != 0:
            raise AssertionError("actual worker did not finish cleanly")
        final = ledger()
        save("ledger-final.json", final)
        if len(final["executions"]) != 1 or len(final["attempts"]) != 1:
            raise AssertionError("one-shot fault duplicated executions or attempts")
        published = [e for e in final["events"] if e["kind"] == "ac_published"]
        if len(published) != (1 if args.fault == "success-setm" else 0):
            raise AssertionError("incorrect AC publication for failed/cancelled action")
        if args.fault == "oom" and final["attempts"][0]["oom_kill"] < 1:
            raise AssertionError("OOM has no kernel oom_kill evidence")
        for pid_text in re.findall(r"REMEX_CHILD:(\d+)", response.get("logs", {}).get("stderr", "")):
            path = Path("/proc") / pid_text / "stat"
            if path.exists() and path.read_text().split(") ", 1)[1][0] != "Z":
                raise AssertionError("set -m descendant remains alive")
            observe("setm_child_not_running", pid=int(pid_text))
        stop(scheduler)
        time.sleep(0.2)
        stop(recorder)
        with (output / "normalized.jsonl").open("wb") as normalized, (output / "adapter.log").open("wb") as errors:
            subprocess.run(
                [ROOT / "build/bin/remex-adapt", "--capture", output / "raw-capture.jsonl"],
                stdout=normalized,
                stderr=errors,
                check=True,
            )
        with (output / "checker.json").open("wb") as checked:
            subprocess.run(
                [
                    ROOT / "build/bin/remex-checker",
                    "--ledger",
                    output / "ledger-final.json",
                    "--evidence",
                    output / "normalized.jsonl",
                ],
                stdout=checked,
                check=True,
            )
        report = json.loads((output / "checker.json").read_text())
        save(
            "result.json",
            {
                "ok": True,
                "fault": args.fault,
                "actual_client": "standalone official REAPI; not Bazel",
                "status_code": response["status_code"],
                "oom_kill": final["attempts"][0]["oom_kill"],
                "ac_publications": len(published),
                "checker": report,
            },
        )
        print((output / "result.json").read_text())
    except BaseException as error:
        save("failure.json", {"type": type(error).__name__, "message": str(error)})
        raise
    finally:
        for p in reversed(processes):
            stop(p)
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
                ["psql", args.postgres, "-v", "ON_ERROR_STOP=1", "-c", "DROP DATABASE " + database],
                check=True,
                capture_output=True,
            )
        save(
            "environment.json",
            {
                "binary_sha256": {
                    name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                    for name in (
                        "build/bin/remex-scheduler",
                        os.path.relpath(WORKER, ROOT),
                        "build/bin/remex-record",
                        "build/bin/remex-adapt",
                        "build/bin/remex-checker",
                        "build/bin/remex-test-actionprobe",
                    )
                }
            },
        )


if __name__ == "__main__":
    main()
