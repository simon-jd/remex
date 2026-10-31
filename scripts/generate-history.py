#!/usr/bin/env python3
"""Create a fresh, deterministic 96-commit corpus without modifying this checkout."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
out = args.output.resolve()
if out.exists():
    raise SystemExit(f"Refusing to overwrite existing path: {out}")
out.mkdir(parents=True)
source = Path(__file__).resolve().parents[1] / "samples"
for family in ("cpp-history", "cuda", "rtl"):
    shutil.copytree(source / family, out / family,
                    ignore=shutil.ignore_patterns("bazel-*", ".cache", "MODULE.bazel.lock"))
def git(*argv, **kw):
    return subprocess.check_output(["git", "-c", "core.hooksPath=/dev/null", "-C", str(out), *argv], **kw).decode().strip()
git("init", "--quiet", "--initial-branch=main")
git("config", "user.name", "remex planted corpus")
git("config", "user.email", "remex-corpus@localhost")
(out / ".gitignore").write_text("**/bazel-*\n**/MODULE.bazel.lock\n.remex-probe-cache/\noracle.json\n")
original_math = (out / "cpp-history/math.h").read_text()
original_cuda = (out / "cuda/vector_add.cu").read_text()
original_rtl = (out / "rtl/fifo.sv").read_text()
original_race = (out / "cuda/race/race_test.cu").read_text()
original_rtl_flaky = (out / "rtl/flaky/fifo_flaky.sv").read_text()
for text, marker in ((original_math, "return n * (n + 1) / 2;"),
                     (original_cuda, "c[i] = a[i] + b[i];"),
                     (original_rtl, "assign full = count == DEPTH;"),
                     (original_race, "constexpr bool kPlantedRace = false;"),
                     (original_rtl_flaky, "localparam bit PLANTED_BUG = 1'b0;")):
    if text.count(marker) != 1:
        raise SystemExit(f"Expected exactly one planting marker: {marker}")
commits = []
for index in range(96):
    (out / "revision.txt").write_text(f"{index}\n")
    (out / "cpp-history/math.h").write_text(original_math.replace("return n * (n + 1) / 2;", "return n * (n + 1) / 2 + (n == 9);" ) if index >= 31 else original_math)
    (out / "cuda/vector_add.cu").write_text(original_cuda.replace("c[i] = a[i] + b[i];", "c[i] = a[i] + b[i] + 1;" ) if index >= 47 else original_cuda)
    (out / "rtl/fifo.sv").write_text(original_rtl.replace("assign full = count == DEPTH;", "assign full = count == DEPTH - 1;" ) if index >= 63 else original_rtl)
    (out / "cuda/race/race_test.cu").write_text(original_race.replace("constexpr bool kPlantedRace = false;", "constexpr bool kPlantedRace = true;") if index >= 72 else original_race)
    (out / "rtl/flaky/fifo_flaky.sv").write_text(original_rtl_flaky.replace("localparam bit PLANTED_BUG = 1'b0;", "localparam bit PLANTED_BUG = 1'b1;") if index >= 80 else original_rtl_flaky)
    (out / "flaky.cc").write_text('''#include <cstdint>
#include <cstdlib>
#include <iostream>
int main(int argc, char** argv) {
  uint64_t x = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 0;
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  x ^= x >> 31;
  if (REGRESSION && (x % 4 == 0)) {
    std::cerr << "REMEX_SIGNATURE: planted_flaky\\n";
    return 1;
  }
  return 0;
}
'''.replace("REGRESSION", "true" if index >= 72 else "false"))
    git("add", ".")
    timestamp = f"2026-01-{1+index//24:02d}T{index%24:02d}:00:00+0000"
    env = dict(os.environ, GIT_AUTHOR_DATE=timestamp, GIT_COMMITTER_DATE=timestamp)
    git("commit", "--quiet", "-m", f"planted revision {index:02d}", env=env)
    commits.append(git("rev-parse", "HEAD"))
def scenario(index, workspace, target, signature, **extra):
    return dict(culprit_index=index, culprit=commits[index], workspace=workspace,
                target=target, signature=signature, **extra)

oracle = {"schema_version": 2, "commits": commits, "count": 96,
          "scenarios": {
              "cpp": scenario(31, "cpp-history", "//:math_test", "cpp_triangular"),
              "cuda": scenario(47, "cuda", "//:cuda_test", "cuda_vector_add", hardware="gpu"),
              "rtl": scenario(63, "rtl", "//:fifo_test", "rtl_fifo", hardware="rtl"),
              "flaky": scenario(72, ".", "native:flaky.cc", "planted_flaky", failure_probability=0.25, hardware=None),
              "cuda_race": scenario(72, "cuda", "//race:race_test", "cuda_lost_update", hardware="gpu", seed_gate="splitmix64(seed) % 4 == 0", seed_gate_probability=0.25),
              "rtl_flaky": scenario(80, "rtl", "//flaky:fifo_flaky_test", "rtl_simultaneous_count", hardware="rtl", seed_gate="splitmix64(seed) % 4 == 0", seed_gate_probability=0.25),
          },
          "note": "CUDA race executes a GPU kernel with barrier-exposed non-atomic lost updates; RTL flaky executes a seed-gated SystemVerilog count-update bug. The 25% figure describes the seed gate over distributed seeds, not an intrinsic race probability. Fixed REMEX_SEED replays inputs. Native flaky.cc is retained solely as the CPU algorithm corpus. Source planting is not execution evidence."}
(out / "oracle.json").write_text(json.dumps(oracle, indent=2)+"\n")
print(json.dumps({"repository":str(out),"commits":96,"oracle":str(out/"oracle.json")}))
