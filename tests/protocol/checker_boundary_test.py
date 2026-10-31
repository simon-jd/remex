"""Independent JSON/CLI contract checks; fixtures are synthetic, never evidence.

Run after building checker with:
  REMEX_CHECKER=build/bin/remex-checker python3 tests/protocol/checker_boundary_test.py
"""

import copy
import base64
import functools
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
CHECKER = Path(os.environ.get("REMEX_CHECKER", ROOT / "build/bin/remex-checker")).resolve()


@functools.lru_cache(maxsize=64)
def encode(message, text):
    """Use the checked-in upstream proto, independent of checker parsing code."""
    run = subprocess.run([
        "/usr/bin/protoc", "-I" + str(ROOT / "worker/third_party"), "-I/usr/include",
        "--encode=build.bazel.remote.execution.v2." + message,
        str(ROOT / "worker/third_party/build/bazel/remote/execution/v2/remote_execution.proto"),
    ], input=text.encode(), capture_output=True, check=True)
    return run.stdout


def digest(data):
    return {"hash": hashlib.sha256(data).hexdigest(), "size_bytes": len(data)}


def raw_action(ledger, rows):
    """Construct synthetic original CAS bytes from this test's intended scenario."""
    execution = ledger["executions"][0]
    command = digest(encode("Command", 'arguments: "/bin/true"'))
    root = digest(encode("Directory", ""))
    fields = [f'command_digest {{ hash: "{command["hash"]}" size_bytes: {command["size_bytes"]} }}',
              f'input_root_digest {{ hash: "{root["hash"]}" size_bytes: {root["size_bytes"]} }}']
    properties = " ".join("properties { name: " + json.dumps(k) + " value: " + json.dumps(v) + " }"
                          for k, v in sorted(execution["platform"].items()))
    fields.append("platform { " + properties + " }")
    if execution.get("do_not_cache"):
        fields.append("do_not_cache: true")
    action = encode("Action", " ".join(fields))
    execution["action_digest"] = digest(action)
    rows[:] = [row for row in rows if row["kind"] != "action_blob"]
    rows.append({"source": "cas", "kind": "action_blob", "sequence": 1,
                 "execution_id": "e", "action_digest": digest(action),
                 "action": base64.b64encode(action).decode()})


def ac_write(ledger, rows):
    result = next(row["result"] for row in rows if row["kind"] == "result_reported")
    return {"source": "ac", "kind": "ac_write", "sequence": 9,
            "execution_id": "e", "attempt_id": "a", "epoch": 1,
            "action_digest": ledger["executions"][0]["action_digest"],
            "result": result, "status_code": 0}


def fixture():
    actual = {"pool": "cpu", "cpu": "4"}
    required = {"pool": "cpu", "cpu.min": "2"}
    ledger = {
        "schema_version": 1,
        "executions": [{"id": "e", "platform": required, "current_epoch": 1,
                        "winner_attempt_id": "a", "state": "Committed"}],
        "attempts": [{"id": "a", "execution_id": "e", "epoch": 1,
                      "worker_id": "w", "worker_platform": actual,
                      "attempt_dir": "/isolated/a", "cgroup_empty": True,
                      "exit_code": 0, "status": "OK", "published": False}],
        "events": [
            {"sequence": 1, "kind": "execution_created", "execution_id": "e"},
            {"sequence": 3, "kind": "lease_granted", "execution_id": "e", "attempt_id": "a", "epoch": 1},
            {"sequence": 7, "kind": "cas_verified", "execution_id": "e", "attempt_id": "a", "epoch": 1},
            {"sequence": 8, "kind": "attempt_committed", "execution_id": "e", "attempt_id": "a", "epoch": 1},
        ],
    }
    identity = {"execution_id": "e", "attempt_id": "a", "epoch": 1}
    rows = [
        dict(identity, source="worker", kind="lease_verified", sequence=2, worker_platform=actual),
        dict(identity, source="worker", kind="cgroup_empty", sequence=4, attempt_dir="/isolated/a", empty=True),
        dict(identity, source="worker", kind="result_reported", sequence=5, attempt_dir="/isolated/a"),
        dict(identity, source="cas", kind="cas_verified", sequence=6, digests=[]),
    ]
    rows[2]["result"] = base64.b64encode(encode("ActionResult", 'stdout_raw: "synthetic-success"')).decode()
    rows[2]["status_code"] = 0
    raw_action(ledger, rows)
    return copy.deepcopy(ledger), copy.deepcopy(rows)


class IndependentCheckerBoundary(unittest.TestCase):
    def run_checker(self, ledger, rows, regression=None):
        self.assertTrue(CHECKER.is_file(), f"Build checker binary first: {CHECKER}")
        with tempfile.TemporaryDirectory(prefix="remex-independent-check-") as tmp:
            lp = Path(tmp) / "ledger.json"
            ep = Path(tmp) / "external.jsonl"
            lp.write_text(json.dumps(ledger))
            ep.write_text("".join(json.dumps(row) + "\n" for row in rows))
            command = [str(CHECKER), "--ledger", str(lp), "--evidence", str(ep)]
            if regression is not None:
                rp = Path(tmp) / "regression.json"
                rp.write_text(json.dumps(regression))
                command += ["--regression", str(rp)]
            run = subprocess.run(command,
                                 capture_output=True, text=True, check=False)
            report = json.loads(run.stdout)
        return run.returncode, report

    def test_independent_baseline(self):
        code, report = self.run_checker(*fixture())
        self.assertEqual((code, report["ok"]), (0, True), report)

    def test_scheduler_ledger_alone_cannot_pass(self):
        ledger, _ = fixture()
        code, report = self.run_checker(ledger, [])
        self.assertNotEqual(code, 0)
        self.assertFalse(report["ok"])
        self.assertGreater(len(report["missing_evidence"]), 0)

    def test_hardware_cache_marker_is_not_a_worker_capability(self):
        ledger, rows = fixture()
        ledger["executions"][0]["platform"]["remex-hw"] = "true"
        ledger["executions"][0]["hardware_test"] = True
        raw_action(ledger, rows)
        code, report = self.run_checker(ledger, rows)
        self.assertEqual((code, report["ok"]), (0, True), report)

    def test_external_uncacheable_write_is_detected(self):
        ledger, rows = fixture()
        ledger["executions"][0]["do_not_cache"] = True
        raw_action(ledger, rows)
        rows.append(ac_write(ledger, rows))
        code, report = self.run_checker(ledger, rows)
        self.assertNotEqual(code, 0)
        self.assertTrue(any(v["code"] == "uncacheable_published" for v in report["violations"]))

    def test_numeric_capabilities_reject_nonfinite_or_nonpositive_requirements(self):
        for want, actual in [("NaN", "4"), ("Inf", "Inf"), ("0", "4"), ("-1", "4"), ("2", "Inf")]:
            with self.subTest(want=want, actual=actual):
                ledger, rows = fixture()
                ledger["executions"][0]["platform"]["cpu.min"] = want
                ledger["attempts"][0]["worker_platform"]["cpu"] = actual
                rows[0]["worker_platform"]["cpu"] = actual
                raw_action(ledger, rows)
                code, report = self.run_checker(ledger, rows)
                self.assertNotEqual(code, 0, report)
                self.assertTrue(any(v["invariant"] == 3 for v in report["violations"]))

    @staticmethod
    def regression():
        return {"conclusion": "CULPRIT", "culprit": "bad", "revisions": ["base", "parent", "bad"],
                "signature": "bug", "k": 1, "runs": 4, "budget": 10,
                "probes": [
                    {"revision": "parent", "purpose": "confirm-parent", "decision": "GOOD",
                     "observations": [{"category": "PASS"}, {"category": "PASS"}]},
                    {"revision": "bad", "purpose": "confirm-culprit", "decision": "BAD",
                     "observations": [{"category": "TEST_FAILURE", "signature": "bug"}, {"category": "PASS"}]},
                ]}

    def test_adjacent_parent_confirmation_passes(self):
        code, report = self.run_checker(*fixture(), regression=self.regression())
        self.assertEqual((code, report["ok"]), (0, True), report)

    def test_nonadjacent_parent_cannot_confirm_culprit(self):
        regression = self.regression()
        regression["probes"][0]["revision"] = "base"
        code, report = self.run_checker(*fixture(), regression=regression)
        self.assertNotEqual(code, 0)
        self.assertTrue(any(v["code"] == "confirmation_missing" for v in report["violations"]))

    def test_other_failure_signature_invalidates_parent_confirmation(self):
        regression = self.regression()
        regression["probes"][0]["observations"].append({"category": "TEST_FAILURE", "signature": "another-bug"})
        regression["runs"] = 5
        code, report = self.run_checker(*fixture(), regression=regression)
        self.assertNotEqual(code, 0)
        self.assertTrue(any(v["code"] == "confirmation_missing" for v in report["violations"]))

    def test_ledger_cannot_hide_original_do_not_cache(self):
        ledger, rows = fixture()
        ledger["executions"][0]["do_not_cache"] = True
        raw_action(ledger, rows)
        ledger["executions"][0]["do_not_cache"] = False
        rows.append(ac_write(ledger, rows))
        code, report = self.run_checker(ledger, rows)
        self.assertNotEqual(code, 0)
        self.assertTrue(any(v["code"] == "raw_uncacheable_published" for v in report["violations"]))

    def test_wrong_ac_key_is_rejected(self):
        ledger, rows = fixture()
        write = ac_write(ledger, rows)
        write["action_digest"] = digest(b"different action")
        rows.append(write)
        code, report = self.run_checker(ledger, rows)
        self.assertNotEqual(code, 0)
        self.assertTrue(any(v["code"] == "ac_key_mismatch" for v in report["violations"]))

    def test_ledger_cannot_hide_original_nonzero_exit(self):
        ledger, rows = fixture()
        rows[2]["result"] = base64.b64encode(encode("ActionResult", 'exit_code: 1 stdout_raw: "failure"')).decode()
        rows.append(ac_write(ledger, rows))
        code, report = self.run_checker(ledger, rows)
        self.assertNotEqual(code, 0)
        self.assertTrue(any(v["code"] == "derived_exit_code_mismatch" for v in report["violations"]))
        self.assertTrue(any(v["code"] == "raw_uncacheable_published" for v in report["violations"]))

    def test_raw_action_digest_is_verified(self):
        ledger, rows = fixture()
        row = next(row for row in rows if row["kind"] == "action_blob")
        row["action"] = base64.b64encode(b"corrupted original Action").decode()
        code, report = self.run_checker(ledger, rows)
        self.assertNotEqual(code, 0)
        self.assertTrue(any(v["code"] == "action_digest_mismatch" for v in report["violations"]))

    def test_derived_fields_without_raw_action_cannot_pass(self):
        ledger, rows = fixture()
        rows = [row for row in rows if row["kind"] != "action_blob"]
        code, report = self.run_checker(ledger, rows)
        self.assertNotEqual(code, 0)
        self.assertGreater(len(report["missing_evidence"]), 0)

    def test_tree_descendant_requires_independent_cas_presence(self):
        ledger, rows = fixture()
        file_digest = digest(b"raw Tree child payload")
        file_text = f'files {{ name: "file" digest {{ hash: "{file_digest["hash"]}" size_bytes: {file_digest["size_bytes"]} }} }}'
        child_digest = digest(encode("Directory", file_text))
        tree = encode("Tree", f'root {{ directories {{ name: "nested" digest {{ hash: "{child_digest["hash"]}" size_bytes: {child_digest["size_bytes"]} }} }} }} children {{ {file_text} }}')
        tree_digest = digest(tree)
        result = encode("ActionResult", f'output_directories {{ path: "output" tree_digest {{ hash: "{tree_digest["hash"]}" size_bytes: {tree_digest["size_bytes"]} }} }}')
        rows[2]["result"] = base64.b64encode(result).decode()
        rows[2]["trees"] = [{"digest": tree_digest, "data": base64.b64encode(tree).decode()}]
        ledger["attempts"][0]["result_digests"] = [tree_digest, file_digest]
        rows[3]["digests"] = [tree_digest, file_digest]
        code, report = self.run_checker(ledger, rows)
        self.assertEqual((code, report["ok"]), (0, True), report)
        rows[3]["digests"] = [tree_digest]
        code, report = self.run_checker(ledger, rows)
        self.assertNotEqual(code, 0)
        self.assertTrue(any("every result digest" in v["detail"] for v in report["missing_evidence"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
