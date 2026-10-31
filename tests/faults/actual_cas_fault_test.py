#!/usr/bin/env python3
"""Actual Bazel/C++/Postgres/cache faults with an independent raw RPC capture.

Every run owns its DB, container, directory, processes and cgroup. It never edits
a ledger. Corruption repair deletes only the deliberately corrupted CAS file and
restarts this disposable cache so Bazel can re-upload the original input.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from verify_capture_fault import verify as verify_original_fault_frames

ROOT = Path(__file__).resolve().parents[2]
# The worker that make worker-build produces; REMEX_TEST_WORKER_BINARY overrides it.
WORKER = Path(os.environ.get("REMEX_TEST_WORKER_BINARY", ROOT / "build/worker/remex-worker"))
IMAGE = "buchgr/bazel-remote-cache:v2.6.3@sha256:adea2ff6dcd59890a1e9c3470e19cf7f2c031502dc0e2d13e12b1dd505762c05"


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fault", choices=("cas-corruption", "cas-eviction", "worker-before-complete"), required=True)
    parser.add_argument("--cgroup-root", required=True, type=Path)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--base-port", default=21980, type=int)
    parser.add_argument("--cas-port", default=39092, type=int)
    parser.add_argument("--http-port", default=38080, type=int)
    parser.add_argument("--postgres", default="postgres://remex:remex-local-only@127.0.0.1:15432/postgres?sslmode=disable")
    args = parser.parse_args()
    token = uuid.uuid4().hex[:12]
    output = args.artifacts.resolve()
    output.mkdir(parents=True, exist_ok=False)
    work = ROOT / "build" / ("independent-cas-" + token)
    work.mkdir()
    fixture = work / "fixture"
    fixture.mkdir()
    cache = work / "cache"
    cache.mkdir()
    container = "remex-independent-cas-" + token
    database = "remex_cas_" + token
    cgroup = args.cgroup_root / ("independent-cas-" + token)
    address = f"127.0.0.1:{args.base_port}"
    worker_proxy = f"127.0.0.1:{args.base_port + 1}"
    cache_proxy = f"127.0.0.1:{args.base_port + 2}"
    gate_proxy = f"127.0.0.1:{args.base_port + 3}"
    lease_proxy = f"127.0.0.1:{args.base_port + 5}"
    lease_gate = work / "hold-leases"
    admin = f"http://127.0.0.1:{args.base_port + 4}"
    cas = f"127.0.0.1:{args.cas_port}"
    http = f"http://127.0.0.1:{args.http_port}"
    dsn = urllib.parse.urlunsplit(urllib.parse.urlsplit(args.postgres)._replace(path="/" + database))
    processes, logs = [], []
    database_created = container_created = False
    def observe(kind, **fields):
        with (output / "observations.jsonl").open("a") as file:
            file.write(json.dumps({"kind": kind, "time": datetime.now(timezone.utc).isoformat(), **fields}) + "\n")
    def launch(command, name, cwd=ROOT):
        log = (output / name).open("wb")
        logs.append(log)
        process = subprocess.Popen(list(map(str, command)), cwd=cwd, stdout=log, stderr=subprocess.STDOUT)
        processes.append(process)
        return process
    def stop(process):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
    def wait_for(predicate, seconds, description):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            answer = predicate()
            if answer:
                return answer
            time.sleep(0.02)
        raise AssertionError("timeout waiting for " + description)
    def healthy(url):
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                return response.status == 200
        except OSError:
            return False
    def ledger():
        with urllib.request.urlopen(admin + "/audit/durable-export", timeout=2) as response:
            return json.load(response)
    def blob_path(digest):
        return next((file for file in cache.rglob("*") if file.is_file() and digest in file.name), None)
    def http_missing(digest):
        try:
            urllib.request.urlopen(http + "/cas/" + digest, timeout=2).close()
            return False
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            return True
    try:
        for port in [args.base_port + i for i in range(6)] + [args.cas_port, args.http_port]:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", port))
        subprocess.run(["psql", args.postgres, "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE " + database], check=True, capture_output=True)
        database_created = True
        cgroup.mkdir()
        (cgroup / "cgroup.subtree_control").write_text("+cpu +memory +pids")
        docker = ["docker", "run", "-d", "--name", container, "--user", str(os.getuid()) + ":" + str(os.getgid()), "--cap-drop=ALL", "--security-opt=no-new-privileges", "-p", f"127.0.0.1:{args.cas_port}:9092", "-p", f"127.0.0.1:{args.http_port}:8080", "-v", str(cache) + ":/data", IMAGE, "--dir", "/data", "--max_size", "1", "--storage_mode", "uncompressed", "--enable_ac_key_instance_mangling"]
        subprocess.run(docker, check=True, capture_output=True)
        container_created = True
        wait_for(lambda: healthy(http + "/status"), 10, "isolated cache health")
        scheduler_command = [ROOT / "build/bin/remex-scheduler", "--listen", address, "--admin", admin.removeprefix("http://"), "--cas", cache_proxy, "--postgres", dsn, "--lease-duration", "2s", "--recovery-grace", "2s", "--max-attempts", "3"]
        scheduler = launch(scheduler_command, "scheduler.log")
        wait_for(lambda: healthy(admin + "/healthz"), 10, "scheduler health")
        recorder = launch([ROOT / "build/bin/remex-record", "--cache-listen", cache_proxy, "--cache-target", cas, "--worker-listen", worker_proxy, "--worker-target", address, "--cgroup-root", cgroup, "--ledger-url", admin + "/audit/durable-export", "--out", output / "raw-capture.jsonl"], "recorder.log")
        wait_for(lambda: (output / "recorder.log").read_text().count("->") == 2, 10, "both recorder listeners")
        for name in ("MODULE.bazel", "MODULE.bazel.lock"):
            shutil.copy2(ROOT / "samples/cpp-history" / name, fixture / name)
        payload = ("original-input-" + token + "\n").encode() * 32
        digest = hashlib.sha256(payload).hexdigest()
        (fixture / "payload.txt").write_bytes(payload)
        stderr = "final-stderr-" + token
        (fixture / "BUILD.bazel").write_text('genrule(name="probe",srcs=["payload.txt"],outs=["probe.out"],cmd="cat $(location payload.txt) > $@; printf ' + stderr + ' >&2",exec_properties={"pool":"cpu","OSFamily":"Linux","ISA":"x86-64"})\n')
        lease_gate.write_text("hold leases while fault is prepared")
        launch([ROOT / "build/bin/remex-test-leasegate", "--listen", lease_proxy, "--target", worker_proxy, "--gate", lease_gate], "leasegate.log")
        worker_cas = cache_proxy
        if args.fault == "worker-before-complete":
            worker_cas = gate_proxy
            launch([ROOT / "build/bin/remex-test-uploadgate", "--listen", gate_proxy, "--upstream", cache_proxy, "--digest", hashlib.sha256(stderr.encode()).hexdigest(), "--observed", output / "gate-observed.json"], "uploadgate.log")
        worker_command = [WORKER, "--scheduler", lease_proxy, "--cas", worker_cas, "--cgroup-root", cgroup, "--root", work / "worker", "--id", "independent-" + token, "--property", "pool=cpu"]
        worker = launch(worker_command, "worker-fault.log")
        wait_for(lambda: "registered worker=" in (output / "worker-fault.log").read_text(), 10, "real worker registration before Execute")
        command = [ROOT / ".tools/bazel.official", "--batch", "--ignore_all_rc_files", "--output_user_root=" + str(work / "bazel-state"), "build", "//:probe", "--repository_cache=" + str(ROOT / ".cache/bazel/cache/repos/v1"), "--remote_executor=grpc://" + address, "--remote_cache=grpc://" + cache_proxy, "--remote_instance_name=" + container, "--noremote_accept_cached", "--noremote_local_fallback", "--spawn_strategy=remote", "--jobs=1", "--remote_retries=0", "--remote_timeout=60s"]
        def bazel_run(name):
            return launch(command + ["--remote_grpc_log=" + str(output / (name + "-grpc.bin")), "--build_event_json_file=" + str(output / (name + "-bep.json")), "--execution_log_compact_file=" + str(output / (name + "-execution.zstd"))], name + ".log", fixture)
        bazel = bazel_run("fault")
        def queued():
            if bazel.poll() is not None:
                raise AssertionError("Bazel exited before queue: " + (output / "fault.log").read_text()[-4000:])
            current = ledger()
            if any(e["state"] == "Unschedulable" for e in current["executions"]):
                raise AssertionError("test setup failed to register a live matching worker")
            return current if any(e["state"] == "Queued" for e in current["executions"]) else None
        before = wait_for(queued, 90, "Bazel queued execution before worker start")
        save(output / "ledger-queued.json", before)
        target = blob_path(digest)
        if target is None or target.read_bytes() != payload:
            raise AssertionError("original payload not found verbatim in uncompressed cache")
        observe("input_uploaded", digest=digest, size=len(payload), cache_path=str(target))
        if args.fault == "cas-corruption":
            damaged = bytes([payload[0] ^ 1]) + payload[1:]
            target.write_bytes(damaged)
            observe("same_size_disk_corruption", expected_sha256=digest, actual_sha256=hashlib.sha256(target.read_bytes()).hexdigest(), size=target.stat().st_size)
        elif args.fault == "cas-eviction":
            filler = bytearray(64 << 20)
            total = 0
            for index in range(1, 21):
                filler[:8] = index.to_bytes(8, "big")
                fill_digest = hashlib.sha256(filler).hexdigest()
                request = urllib.request.Request(http + "/cas/" + fill_digest, data=filler, method="PUT")
                with urllib.request.urlopen(request, timeout=30) as response:
                    if response.status != 200:
                        raise AssertionError("filler upload rejected")
                total += len(filler)
                observe("lru_pressure_upload", index=index, digest=fill_digest, size=len(filler), total=total)
                if not target.exists():
                    break
            wait_for(lambda: not target.exists() and http_missing(digest), 10, "actual target LRU eviction")
            observe("lru_eviction_observed", uploaded_bytes=total, target_absent=True, http_status=404)
        lease_gate.unlink()
        observe("lease_hold_released")
        if args.fault == "worker-before-complete":
            wait_for(lambda: (output / "gate-observed.json").exists(), 30, "actual final stderr upload persisted before Complete")
            current = wait_for(lambda: (lambda value: value if any(a["phase"] == "Uploading" for a in value["attempts"]) else None)(ledger()), 5, "durable Uploading phase")
            if any(e["kind"] == "attempt_committed" for e in current["events"]):
                raise AssertionError("pre-Complete target already committed")
            raw = [json.loads(line) for line in (output / "raw-capture.jsonl").read_text().splitlines()]
            if any(row["method"].endswith("/Complete") for row in raw):
                raise AssertionError("worker already called Complete before SIGKILL")
            save(output / "ledger-before-kill.json", current)
            worker.kill()
            worker.wait(timeout=3)
            wait_for(lambda: "populated 0" in (cgroup / "cgroup.events").read_text(), 3, "worker guardian emptied subtree before replacement")
            observe("worker_sigkill_before_complete", pid=worker.pid, complete_calls_before_kill=0, cgroup_events=(cgroup / "cgroup.events").read_text())
            worker = launch(worker_command, "worker-recovered.log")
            status = bazel.wait(timeout=90)
            if status != 0:
                raise AssertionError("Bazel did not recover from pre-Complete worker SIGKILL")
        else:
            status = bazel.wait(timeout=90)
            if status == 0:
                raise AssertionError("corrupted/evicted input unexpectedly produced a successful action")
            failed = ledger()
            save(output / "ledger-failed.json", failed)
            if not failed["attempts"] or not any(a["status"] == "Internal" for a in failed["attempts"]):
                raise AssertionError("bad-input failure was not observed by an actual worker attempt")
            log_text = (output / "worker-fault.log").read_text()
            expected_error = "digest" if args.fault == "cas-corruption" else "ByteStream.Read"
            if expected_error not in log_text:
                raise AssertionError("worker did not report the injected content failure")
            if any(e["kind"] == "ac_published" for e in failed["events"]) or any(e.get("winner_attempt_id") and e["status_code"] == 0 for e in failed["executions"]):
                raise AssertionError("bad-input attempt became a successful winner or reached AC")
            observe("fault_build_rejected", bazel_exit=status, attempts=len(failed["attempts"]))
            stop(worker)
            if args.fault == "cas-corruption":
                target.unlink()
                subprocess.run(["docker", "restart", container], check=True, capture_output=True)
                wait_for(lambda: healthy(http + "/status"), 10, "isolated cache restart after removing only damaged blob")
                observe("corruption_repair", action="deleted deliberately damaged blob, restarted own cache; Bazel must reupload original content")
            recovery = bazel_run("recovery")
            worker = launch(worker_command, "worker-recovered.log")
            if recovery.wait(timeout=90) != 0:
                raise AssertionError("Bazel failed after original input reupload")
            if blob_path(digest) is None or blob_path(digest).read_bytes() != payload:
                raise AssertionError("recovery did not restore original digest-addressed input")
        if (fixture / "bazel-bin/probe.out").read_bytes() != payload:
            raise AssertionError("actual output differs from source payload")
        final = ledger()
        save(output / "ledger-final.json", final)
        commits = [e for e in final["events"] if e["kind"] == "attempt_committed"]
        writes = [e for e in final["events"] if e["kind"] == "ac_published"]
        expected_commits = 1 if args.fault == "worker-before-complete" else 2
        if len(commits) != expected_commits or len(writes) != 1 or len({e["execution_id"] for e in commits}) != len(commits):
            raise AssertionError("fault recovery did not preserve one terminal commit per execution and only one successful AC publication")
        if "populated 0" not in (cgroup / "cgroup.events").read_text():
            raise AssertionError("action processes remain before cleanup")
        stop(worker)
        stop(scheduler)
        time.sleep(0.2)
        stop(recorder)
        verify_original_fault_frames(output, args.fault)
        with (output / "normalized.jsonl").open("wb") as normalized, (output / "adapter.log").open("wb") as errors:
            adapted = subprocess.run([ROOT / "build/bin/remex-adapt", "--capture", output / "raw-capture.jsonl"], stdout=normalized, stderr=errors)
        with (output / "checker.json").open("wb") as checked:
            checker = subprocess.run([ROOT / "build/bin/remex-checker", "--ledger", output / "ledger-final.json", "--evidence", output / "normalized.jsonl"], stdout=checked, stderr=subprocess.PIPE)
        if adapted.returncode != 0 or checker.returncode != 0:
            raise AssertionError("independent adaptation/checker did not pass; inspect saved original evidence")
        report = json.loads((output / "checker.json").read_text())
        save(output / "result.json", {"ok": True, "fault": args.fault, "executions": len(final["executions"]), "attempts": len(final["attempts"]), "commits": len(commits), "ac_publications": len(writes), "checker": report, "scope": "actual process/cache fault with raw external evidence; invariant 6 requires a separate regression experiment"})
        print((output / "result.json").read_text())
    except BaseException as error:
        save(output / "failure.json", {"type": type(error).__name__, "message": str(error)})
        raise
    finally:
        for process in reversed(processes):
            stop(process)
        for log in logs:
            log.close()
        if container_created:
            with (output / "cache.log").open("wb") as log:
                subprocess.run(["docker", "logs", container], stdout=log, stderr=subprocess.STDOUT)
            subprocess.run(["docker", "rm", "-f", container], check=True, capture_output=True)
        if cgroup.exists():
            (cgroup / "cgroup.kill").write_text("1")
            time.sleep(0.05)
            for child in cgroup.iterdir():
                if child.is_dir():
                    child.rmdir()
            cgroup.rmdir()
        if database_created:
            subprocess.run(["psql", args.postgres, "-v", "ON_ERROR_STOP=1", "-c", "DROP DATABASE " + database], check=True, capture_output=True)
        save(output / "environment.json", {"cache_image": IMAGE, "fault": args.fault, "cache_max_gib": 1, "work": str(work), "binary_sha256": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in ["build/bin/remex-scheduler", os.path.relpath(WORKER, ROOT), "build/bin/remex-record", "build/bin/remex-adapt", "build/bin/remex-checker"]}})


if __name__ == "__main__":
    main()
