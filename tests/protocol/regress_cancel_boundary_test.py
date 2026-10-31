"""Independent R18 CLI fixtures: execution cancellation must survive UNKNOWN attempts."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
BINARY = Path(os.environ.get("REMEX_CLI", ROOT / "build/bin/remex"))


class CancellationBoundary(unittest.TestCase):
    def classify(self, attempts, state="Cancelled", code=1):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            (directory / "bep.json").write_text(json.dumps({"id": {"testResult": {"label": "//:cancelled"}}, "testResult": {"status": "REMOTE_FAILURE"}}) + "\n")
            (directory / "ledger.json").write_text(json.dumps({"executions": [{"id": "e", "state": state, "status_code": code}], "attempts": attempts}))
            run = subprocess.run([str(BINARY), "classify", "--bep", str(directory / "bep.json"), "--ledger", str(directory / "ledger.json")], capture_output=True, text=True, check=True)
            return json.loads(run.stdout)

    def test_execution_cancelled_with_unknown_running_attempt(self):
        result = self.classify([{"id": "a", "execution_id": "e", "phase": "Running", "status": "UNKNOWN"}])
        self.assertEqual(result["category"], "CANCELLED")
        self.assertEqual(result["evidence"], ["execution:e"])

    def test_cancelled_before_lease(self):
        self.assertEqual(self.classify([])["category"], "CANCELLED")

    def test_numeric_cancelled_status_preserved(self):
        self.assertEqual(self.classify([], state="Running", code=1)["category"], "CANCELLED")

    def test_independent_oom_overrides_cancellation(self):
        result = self.classify([{"id": "a", "execution_id": "e", "status": "UNKNOWN", "oom_kill": 1}])
        self.assertEqual(result["category"], "INFRASTRUCTURE")
        self.assertEqual(result["evidence"], ["attempt:a"])

    def test_transport_failure_overrides_cancellation(self):
        self.assertEqual(self.classify([{"id": "a", "execution_id": "e", "status": "Unavailable"}])["category"], "INFRASTRUCTURE")


if __name__ == "__main__":
    unittest.main(verbosity=2)
