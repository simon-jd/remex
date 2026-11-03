#!/usr/bin/env python3
"""The generated performance history: exactly one commit changes the kernel's code."""
import json
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def git(repository, *args):
    return subprocess.run(["git", "-C", str(repository), *args], check=True, capture_output=True, text=True).stdout


class PerfHistoryTests(unittest.TestCase):
    def test_only_the_culprit_changes_the_kernel(self):
        with tempfile.TemporaryDirectory(prefix="remex-perf-history-") as temp:
            out = Path(temp) / "history"
            subprocess.run([sys.executable, str(ROOT / "scripts/perf-history.py"), "generate", "--output", str(out),
                            "--commits", "12", "--culprit", "7", "--driver-major", "595"],
                           check=True, capture_output=True)
            oracle = json.loads((out / "oracle.json").read_text())
            commits = git(out, "rev-list", "--reverse", "HEAD").split()
            self.assertEqual(commits, oracle["commits"])
            self.assertEqual(oracle["culprit"], commits[7])
            tiled, code = [], []
            for commit in commits:
                source = git(out, "show", f"{commit}:cuda/transpose_test.cu")
                tiled.append("__shared__ float tile" in source)
                code.append(re.sub(r"// Revision \d+ of the transpose sample\.|// Initial transpose sample\.", "", source))
            self.assertEqual(tiled, [True] * 7 + [False] * 5)
            # Apart from the comment, only the culprit changes the source.
            self.assertEqual(len(set(code[:7])), 1)
            self.assertEqual(len(set(code[7:])), 1)
            self.assertIn('"test.gpu.driver_major": "595"', git(out, "show", "HEAD:cuda/BUILD.bazel"))
            with self.assertRaises(subprocess.CalledProcessError):
                subprocess.run([sys.executable, str(ROOT / "scripts/perf-history.py"), "generate", "--output", str(out),
                                "--driver-major", "595"], check=True, capture_output=True)


if __name__ == "__main__":
    unittest.main()
