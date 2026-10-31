"""Independent negative controls for the hardware corpus evidence accounting.

These are synthetic orchestration tests, not GPU/RTL execution evidence.
"""
import importlib.util
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("independent_hardware_corpus", ROOT / "scripts/hardware-corpus.py")
corpus = importlib.util.module_from_spec(spec)
spec.loader.exec_module(corpus)


class HardwareEvidenceBoundary(unittest.TestCase):
    def test_missing_and_infrastructure_never_agree(self):
        for missing in (None, {"category": "INFRASTRUCTURE"}, {"category": "UNKNOWN"}, {"category": "CANCELLED"}):
            for executed in (False, True):
                self.assertEqual(corpus.compare_observations({"category": "PASS"}, missing, executed), "INCONCLUSIVE")

    def test_different_failure_signature_cannot_agree(self):
        oracle = {"category": "TEST_FAILURE", "signature": "new-fault"}
        retained = {"category": "TEST_FAILURE", "signature": "old-fault"}
        self.assertEqual(corpus.compare_observations(oracle, retained, False), "STALE_OBSERVATION")
        self.assertEqual(corpus.compare_observations(oracle, retained, True), "REPLAY_DISAGREEMENT")

    def test_current_cached_or_nonwinning_attempt_is_not_hardware_evidence(self):
        key = {"hash": "a" * 64, "size_bytes": 1}
        bep = {"testActionOutput": [{"name": "test.log", "uri": "bytestream://cas/blobs/" + key["hash"] + "/1"}]}
        execution = {"id": "e", "winner_attempt_id": "winner", "do_not_cache": True, "platform": {"pool": "gpu"}}
        attempt = {"id": "old", "execution_id": "e", "worker_platform": {"pool": "gpu"}, "result_digests": [key]}
        ledger = {"executions": [execution], "attempts": [attempt]}
        self.assertEqual(corpus.hardware_test_attempts(ledger, bep, "gpu"), [])
        attempt["id"] = "winner"
        execution["cached_result"] = True
        self.assertEqual(corpus.hardware_test_attempts(ledger, bep, "gpu"), [])
        execution["cached_result"] = False
        attempt["worker_platform"]["pool"] = "cpu"
        self.assertEqual(corpus.hardware_test_attempts(ledger, bep, "gpu"), [])

    def run_accounting(self, new_failure=False, oracle_mismatch=False):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            repository = directory / "repository"
            repository.mkdir()
            commits = [format(i + 1, "040x") for i in range(96)]
            (repository / "oracle.json").write_text(json.dumps({"schema_version": 2, "count": 96, "commits": commits, "scenarios": {"cuda_race": {"hardware": "gpu", "workspace": "cuda", "target": "//race:race_test"}}}))
            evidence = directory / "evidence"
            args = SimpleNamespace(repository=repository, indices="0-1", seeds="0,6", scenarios="cuda_race",
                                   evidence=evidence, bazel="/bin/true", remex="/bin/true", selector="/bin/true",
                                   bazel_flag=[], cuda_flag=[], rtl_flag=[], executor="grpc://executor",
                                   cache="grpc://cache", admin="http://admin", instance="synthetic",
                                   selector_cache=False, selector_input=[], resume_from=None, resume_legacy=False, reuse_resume_workspace=False)
            def choose(_args, _wrapper, _work, before, *_rest):
                return ({"cuda_race"} if before is None else set()), {}, []
            def observe(_args, _wrapper, _work, _scenario, name, index, revision, seed, arm, _evidence):
                failed = new_failure and index == 1 and seed == 6 and arm == "oracle"
                return {"category": "TEST_FAILURE" if failed else "PASS", "signature": "new" if failed else "", "revision": revision, "scenario": name, "seed": seed, "arm": arm, "test_seconds": 1, "matches_planted_oracle": not oracle_mismatch}
            with patch.object(corpus, "git", return_value=""), patch.object(corpus, "command", return_value=(0, "", 0)), patch.object(corpus, "selection", side_effect=choose), patch.object(corpus, "test_observation", side_effect=observe):
                code = corpus.evaluate(args)
            summary = json.loads((evidence / "summary.json").read_text())
            comparisons = [json.loads(line) for line in (evidence / "comparisons.jsonl").read_text().splitlines()]
            return code, summary, comparisons

    def test_skips_use_own_same_seed_selected_observation(self):
        code, summary, rows = self.run_accounting()
        self.assertEqual(code, 0)
        self.assertFalse(summary["full_96_revisions"])
        self.assertEqual(summary["oracle_invocations"], 4)
        self.assertEqual(summary["selected_invocations"], 2)
        for row in rows:
            self.assertEqual(row["selected"]["arm"], "selected")
            self.assertEqual(row["selected"]["seed"], row["seed"])
            if row["index"] == 1:
                self.assertFalse(row["selected_executed"])
                self.assertNotEqual(row["selected_observation_revision"], row["oracle"]["revision"])

    def test_skipped_new_failure_exits_nonzero(self):
        code, summary, _ = self.run_accounting(new_failure=True)
        self.assertEqual(code, 2)
        self.assertEqual(summary["comparisons"]["MISSED_FAILURE"], 1)

    def test_agreement_cannot_hide_planted_oracle_mismatch(self):
        code, summary, _ = self.run_accounting(oracle_mismatch=True)
        self.assertEqual(code, 2)
        self.assertEqual(summary["comparisons"]["AGREE"], 4)
        self.assertGreater(summary["planted_oracle_mismatches"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
