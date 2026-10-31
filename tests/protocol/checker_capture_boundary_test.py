"""Independent CLI regression cases for capture R12/R13/R14 and framing.

All captures here are explicitly synthetic protobuf fixtures, not deployment
evidence. The adapter and checker are separate compiled command processes.
"""
import base64
import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import checker_boundary_test as boundary

ADAPTER = Path(os.environ.get("REMEX_ADAPTER", boundary.ROOT / "build/bin/remex-adapt")).resolve()


def quoted(data):
    return '"' + "".join("\\%03o" % value for value in data) + '"'


def worker_proto(message, text):
    run = subprocess.run(["/usr/bin/protoc", "-I" + str(boundary.ROOT),
                          "--encode=remex.worker.v1." + message,
                          str(boundary.ROOT / "api/worker/v1/worker.proto")],
                         input=text.encode(), capture_output=True, check=True)
    return run.stdout


class Capture:
    def __init__(self):
        self.rows = []

    def add(self, call, method, kind, message=None, **extra):
        row = {"capture_id": "independent-synthetic", "sequence": len(self.rows) + 1,
               "call_id": call, "endpoint": "test", "method": method, "kind": kind,
               "message": None if message is None else base64.b64encode(message).decode(),
               "status_code": 0}
        row.update(extra)
        self.rows.append(row)

    def call(self, number, method, request, response, **extra):
        self.add(number, method, "start", **extra)
        self.add(number, method, "request", request)
        self.add(number, method, "request_forwarded")
        self.add(number, method, "response", response)
        self.add(number, method, "end")


def lease(capture, ledger, rows):
    key = ledger["executions"][0]["action_digest"]
    action = base64.b64decode(next(row["action"] for row in rows if row["kind"] == "action_blob"))
    request = worker_proto("LeaseRequest", 'worker_id:"w" properties {key:"pool" value:"cpu"} properties {key:"cpu" value:"4"}')
    response = worker_proto("LeaseResponse", 'available:true execution_id:"e" attempt_id:"a" epoch:1 action_digest {hash:"' + key["hash"] + '" size_bytes:' + str(key["size_bytes"]) + '} action:' + quoted(action))
    capture.call(1, "/remex.worker.v1.WorkerService/Lease", request, response)


def complete(capture, number, result, decision):
    method = "/remex.worker.v1.WorkerService/Complete"
    request = worker_proto("CompleteRequest", 'worker_id:"w" attempt_id:"a" epoch:1 action_result:' + quoted(result) + ' status_code:0 cgroup_empty:true attempt_dir:"/independent/raw-attempt"')
    response = worker_proto("CompleteResponse", "decision:" + decision)
    capture.add(number, method, "start")
    capture.add(number, method, "request", request)
    capture.add(number, method, "cgroup_observation", cgroup_path="/independent/synthetic-cgroup", cgroup_events="populated 0\n", memory_events="oom_kill 0\n")
    capture.add(number, method, "request_forwarded")
    capture.add(number, method, "response", response)
    capture.add(number, method, "end")


class IndependentCaptureBoundary(unittest.TestCase):
    def adapt(self, rows, success=True):
        with tempfile.TemporaryDirectory(prefix="remex-independent-capture-") as tmp:
            path = Path(tmp) / "capture.jsonl"
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            run = subprocess.run([str(ADAPTER), "--capture", str(path)], capture_output=True, text=True)
        if success:
            self.assertEqual(run.returncode, 0, run.stderr)
            return [json.loads(line) for line in run.stdout.splitlines()]
        self.assertNotEqual(run.returncode, 0, run.stdout)
        return []

    def test_forwarded_timed_out_ac_write_cannot_disappear(self):
        ledger, rows = boundary.fixture()
        key = ledger["executions"][0]["action_digest"]
        request = boundary.encode("UpdateActionResultRequest", 'action_digest {hash:"' + key["hash"] + '" size_bytes:' + str(key["size_bytes"]) + '} action_result {stdout_raw:"synthetic-success"}')
        capture = Capture()
        method = "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult"
        capture.add(1, method, "start", metadata={"x-remex-execution-id": ["e"], "x-remex-attempt-id": ["a"], "x-remex-epoch": ["1"]})
        capture.add(1, method, "request", request)
        capture.add(1, method, "request_forwarded")
        capture.add(1, method, "end", status_code=4)
        adapted = self.adapt(capture.rows)
        self.assertTrue(any(row["kind"] == "ac_write_attempt" for row in adapted), adapted)
        code, report = boundary.IndependentCheckerBoundary().run_checker(ledger, rows + adapted)
        self.assertNotEqual(code, 0)
        self.assertGreater(len(report["missing_evidence"]), 0)
        truncated = self.adapt(capture.rows[:-1])
        self.assertTrue(any(row["kind"] == "evidence_gap" for row in truncated), truncated)

    def test_fenced_report_does_not_require_stored_result_or_directory(self):
        ledger, rows = boundary.fixture()
        capture = Capture()
        lease(capture, ledger, rows)
        complete(capture, 2, boundary.encode("ActionResult", 'stdout_raw:"late rejected result"'), "FENCED")
        adapted = self.adapt(capture.rows)
        reports = [row for row in adapted if row["kind"] == "result_reported"]
        self.assertEqual(reports[0]["complete_decision"], "FENCED")
        ledger["executions"][0].update(current_epoch=2, winner_attempt_id="", state="Retrying")
        ledger["attempts"][0].update(attempt_dir="", result_digests=[], published=False)
        ledger["events"] = ledger["events"][:2]
        code, report = boundary.IndependentCheckerBoundary().run_checker(ledger, adapted)
        self.assertEqual((code, report["ok"]), (0, True), report)

    def test_warm_output_tree_comes_from_original_batch_read(self):
        ledger, rows = boundary.fixture()
        tree = boundary.encode("Tree", "root {}")
        key = boundary.digest(tree)
        digest_text = 'hash:"' + key["hash"] + '" size_bytes:' + str(key["size_bytes"])
        result = boundary.encode("ActionResult", 'output_directories {path:"out" tree_digest {' + digest_text + "}}")
        for successful in (True, False):
            with self.subTest(successful_batch_read=successful):
                capture = Capture()
                lease(capture, ledger, rows)
                request = boundary.encode("BatchReadBlobsRequest", "digests {" + digest_text + "}")
                response = boundary.encode("BatchReadBlobsResponse", "responses {digest {" + digest_text + "} data:" + quoted(tree) + " status {code:" + ("0" if successful else "15") + "}}")
                capture.call(2, "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs", request, response)
                complete(capture, 3, result, "ACCEPTED")
                adapted = self.adapt(capture.rows)
                reports = [row for row in adapted if row["kind"] == "result_reported"]
                if successful:
                    self.assertEqual(reports[0]["trees"], [{"digest": key, "data": base64.b64encode(tree).decode()}])
                else:
                    self.assertTrue(any(row["kind"] == "evidence_gap" for row in adapted), adapted)

    def test_failed_complete_preserves_unknown_submission_and_kernel_observation(self):
        ledger, rows = boundary.fixture()
        capture = Capture()
        lease(capture, ledger, rows)
        result = boundary.encode("ActionResult", 'stdout_raw:"unknown transport result"')
        complete(capture, 2, result, "ACCEPTED")
        capture.rows = [row for row in capture.rows if not (row["call_id"] == 2 and row["kind"] == "response")]
        for i, row in enumerate(capture.rows, 1):
            row["sequence"] = i
        capture.rows[-1]["status_code"] = 14
        adapted = self.adapt(capture.rows)
        reported = next(row for row in adapted if row["kind"] == "result_reported")
        self.assertEqual(reported["complete_decision"], "UNKNOWN")
        self.assertEqual(base64.b64decode(reported["result"]), result)
        self.assertTrue(any(row["kind"] == "cgroup_empty" and row["empty"] for row in adapted))
        ledger["executions"][0].update(winner_attempt_id="", state="Running")
        ledger["attempts"][0].update(attempt_dir="", result_digests=[], published=False)
        ledger["events"] = ledger["events"][:2]
        code, report = boundary.IndependentCheckerBoundary().run_checker(ledger, adapted)
        self.assertEqual((code, report["ok"]), (0, True), report)
        for broken in ("nonempty", "missing-observation", "truncated"):
            with self.subTest(broken=broken):
                altered = copy.deepcopy(capture.rows)
                if broken == "nonempty":
                    next(row for row in altered if row["kind"] == "cgroup_observation")["cgroup_events"] = "populated 1\n"
                elif broken == "missing-observation":
                    altered = [row for row in altered if row["kind"] != "cgroup_observation"]
                else:
                    altered.pop()
                for i, row in enumerate(altered, 1):
                    row["sequence"] = i
                evidence = self.adapt(altered)
                code, report = boundary.IndependentCheckerBoundary().run_checker(ledger, evidence)
                self.assertNotEqual(code, 0, report)

    def test_capture_sequence_and_call_identity_fail_closed(self):
        base = Capture()
        base.call(1, "/example/Method", b"", b"")
        for case in ("gap", "start-at-two", "duplicate-start", "duplicate-end", "method-change", "endpoint-change", "missing-start"):
            with self.subTest(case=case):
                rows = copy.deepcopy(base.rows)
                if case == "gap":
                    rows.pop(2)
                elif case == "start-at-two":
                    for row in rows:
                        row["sequence"] += 1
                elif case == "duplicate-start":
                    rows[1]["kind"] = "start"
                elif case == "duplicate-end":
                    rows.append(dict(rows[-1], sequence=len(rows) + 1))
                elif case == "method-change":
                    rows[1]["method"] = "/example/Other"
                elif case == "endpoint-change":
                    rows[1]["endpoint"] = "another"
                elif case == "missing-start":
                    rows = rows[1:]
                    for i, row in enumerate(rows, 1):
                        row["sequence"] = i
                self.adapt(rows, success=False)

    def test_unknown_winning_submission_cannot_hide_modified_or_missing_result(self):
        ledger, rows = boundary.fixture()
        report = next(row for row in rows if row["kind"] == "result_reported")
        report["complete_decision"] = "UNKNOWN"
        ledger["executions"][0]["result"] = report["result"]
        ledger["attempts"][0]["result"] = report["result"]
        code, checked = boundary.IndependentCheckerBoundary().run_checker(ledger, rows)
        self.assertEqual(code, 0, checked)
        for result in (None, base64.b64encode(b"\xff").decode(), base64.b64encode(boundary.encode("ActionResult", 'stdout_raw:"forged"')).decode()):
            with self.subTest(result=result):
                altered = copy.deepcopy(rows)
                next(row for row in altered if row["kind"] == "result_reported")["result"] = result
                code, checked = boundary.IndependentCheckerBoundary().run_checker(ledger, altered)
                self.assertNotEqual(code, 0, checked)


if __name__ == "__main__":
    unittest.main(verbosity=2)
