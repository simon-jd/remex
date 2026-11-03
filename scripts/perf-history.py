#!/usr/bin/env python3
"""A CUDA performance regression in a generated history, bisected by kernel time.

    scripts/perf-history.py generate --output build/perf-history
    scripts/perf-history.py bisect --repository build/perf-history \
        --evidence evidence/perf-bisect --instance remex --cuda-flag=... > summary.json

generate writes a new 40-commit Git history of one Bazel CUDA workspace,
cuda/, whose test transposes a 4096 x 4096 float matrix through a shared
memory tile and prints the median of 100 timed launches as
"REMEX_METRIC transpose_ms=<ms>". One commit (index 26 by default) replaces
the tiled kernel with one that writes each element straight to its
transposed position, so a warp's writes are no longer coalesced. The output
is still correct, so the test keeps passing and only the time changes. Every third
commit edits a comment in the kernel source, so each of those revisions
compiles anew.

bisect runs "remex bisect" with bisect.metric on that history against real
remex workers: the test builds on a CPU worker and runs on a GPU worker.
Each probe checks out the revision in one persistent worktree, runs the
test once with test caching off, checks the invocation's ledger for the
GPU attempt and its NVML identity (hardware-corpus.py's checks), and
records nvidia-smi's clocks, temperature and power before and after the
run in the observation's detail.
"""
import argparse
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("hardware_corpus", ROOT / "scripts/hardware-corpus.py")
corpus = importlib.util.module_from_spec(spec)
spec.loader.exec_module(corpus)

METRIC = "transpose_ms"
SCENARIO = dict(workspace="cuda", target="//:transpose_test", hardware="gpu", signature="cuda_transpose_mismatch")

KERNEL = r"""#include <cuda_runtime.h>
#include <algorithm>
#include <cstdio>
#include <vector>

// @REVISION@
#define CUDA_CHECK(call)                                                        \
  do {                                                                          \
    cudaError_t e = (call);                                                     \
    if (e != cudaSuccess) {                                                     \
      std::fprintf(stderr, "CUDA infrastructure: %s\n", cudaGetErrorString(e)); \
      return 125;                                                               \
    }                                                                           \
  } while (0)

constexpr int kN = 4096;
constexpr int kTile = 32;
constexpr int kRows = 8;
constexpr int kLaunches = 100;

@KERNEL@

int main() {
  const size_t count = size_t(kN) * kN;
  std::vector<float> host(count), result(count);
  for (size_t i = 0; i < count; ++i) host[i] = float(i % 65521);
  float *in = nullptr, *out = nullptr;
  CUDA_CHECK(cudaMalloc(&in, count * sizeof(float)));
  CUDA_CHECK(cudaMalloc(&out, count * sizeof(float)));
  CUDA_CHECK(cudaMemcpy(in, host.data(), count * sizeof(float), cudaMemcpyHostToDevice));
  const dim3 grid(kN / kTile, kN / kTile), block(kTile, kRows);
  for (int i = 0; i < 10; ++i) transpose<<<grid, block>>>(out, in);
  CUDA_CHECK(cudaGetLastError());
  cudaEvent_t start, stop;
  CUDA_CHECK(cudaEventCreate(&start));
  CUDA_CHECK(cudaEventCreate(&stop));
  std::vector<float> ms(kLaunches);
  for (float& t : ms) {
    CUDA_CHECK(cudaEventRecord(start));
    transpose<<<grid, block>>>(out, in);
    CUDA_CHECK(cudaEventRecord(stop));
    CUDA_CHECK(cudaEventSynchronize(stop));
    CUDA_CHECK(cudaEventElapsedTime(&t, start, stop));
  }
  std::sort(ms.begin(), ms.end());
  CUDA_CHECK(cudaMemcpy(result.data(), out, count * sizeof(float), cudaMemcpyDeviceToHost));
  CUDA_CHECK(cudaFree(in));
  CUDA_CHECK(cudaFree(out));
  for (int y = 0; y < kN; ++y) {
    for (int x = 0; x < kN; ++x) {
      if (result[size_t(x) * kN + y] != host[size_t(y) * kN + x]) {
        std::fprintf(stderr, "REMEX_SIGNATURE: cuda_transpose_mismatch\n");
        return 1;
      }
    }
  }
  std::printf("transpose %dx%d, %d launches, min %.5f ms\n", kN, kN, kLaunches, ms.front());
  std::printf("REMEX_METRIC transpose_ms=%.5f\n", (ms[kLaunches / 2 - 1] + ms[kLaunches / 2]) / 2);
  return 0;
}
"""

# Reads a 32 x 32 tile into shared memory with coalesced loads and writes it
# back transposed, also coalesced; the padding column keeps the column reads
# of the tile in different banks.
TILED = """// Stage each 32 x 32 tile in shared memory so reads and writes are both coalesced.
__global__ void transpose(float* out, const float* in) {
  __shared__ float tile[kTile][kTile + 1];
  int x = blockIdx.x * kTile + threadIdx.x;
  int y = blockIdx.y * kTile + threadIdx.y;
  for (int j = 0; j < kTile; j += kRows) tile[threadIdx.y + j][threadIdx.x] = in[(y + j) * kN + x];
  __syncthreads();
  x = blockIdx.y * kTile + threadIdx.x;
  y = blockIdx.x * kTile + threadIdx.y;
  for (int j = 0; j < kTile; j += kRows) out[(y + j) * kN + x] = tile[threadIdx.x][threadIdx.y + j];
}"""

# The culprit's simpler kernel: the same result, but each warp's writes go
# to 32 different rows, one sector each.
DIRECT = """// Each thread copies its elements straight to the transposed position.
__global__ void transpose(float* out, const float* in) {
  const int x = blockIdx.x * kTile + threadIdx.x;
  const int y = blockIdx.y * kTile + threadIdx.y;
  for (int j = 0; j < kTile; j += kRows) out[x * kN + (y + j)] = in[(y + j) * kN + x];
}"""

BUILD = """load("@rules_cc//cc:defs.bzl", "cc_test")
load("@rules_cuda//cuda:defs.bzl", "cuda_library")

cuda_library(
    name = "transpose",
    srcs = ["transpose_test.cu"],
    alwayslink = True,
)

cc_test(
    name = "transpose_test",
    deps = [":transpose"],
    tags = ["remex-hw", "no-remote-cache"],
    exec_properties = {
        "pool": "cpu",
        "test.pool": "gpu",
        "OSFamily": "Linux",
        "ISA": "x86-64",
        "test.gpu.cc": "8.9",
        "test.gpu.driver_major": "@DRIVER@",
    },
)
"""


def git(repository, *args, env=None):
    return subprocess.run(["git", "-C", str(repository), *args], check=True, capture_output=True, text=True,
                          env=env).stdout.strip()


def generate(args):
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    driver = args.driver_major or subprocess.run(
        ["nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"],
        check=True, capture_output=True, text=True).stdout.split(".")[0].strip()
    git(out, "init", "-q", "-b", "main")
    git(out, "config", "user.name", "remex perf history")
    git(out, "config", "user.email", "perf-history@remex.invalid")
    workspace = out / "cuda"
    workspace.mkdir()
    for name in ("MODULE.bazel", "MODULE.bazel.lock"):
        shutil.copy(ROOT / "samples/cuda" / name, workspace / name)
    (workspace / "BUILD.bazel").write_text(BUILD.replace("@DRIVER@", driver))
    commits, comment, kernel = [], "Initial transpose sample.", TILED
    for index in range(args.commits):
        message = "Add the transpose sample"
        if index == args.culprit:
            kernel, message = DIRECT, "Transpose without the shared-memory tile"
        elif index and index % 3 == 0:
            comment, message = f"Revision {index} of the transpose sample.", f"Reword the kernel comment ({index})"
        elif index:
            message = f"Update the notes ({index})"
        (workspace / "transpose_test.cu").write_text(KERNEL.replace("@REVISION@", comment).replace("@KERNEL@", kernel))
        (out / "NOTES.md").write_text(f"revision {index}\n")
        stamp = f"2026-01-01T00:{index // 60:02d}:{index % 60:02d}Z"
        env = dict(os.environ, GIT_AUTHOR_DATE=stamp, GIT_COMMITTER_DATE=stamp)
        git(out, "add", "-A", env=env)
        git(out, "commit", "-q", "-m", message, env=env)
        commits.append(git(out, "rev-parse", "HEAD"))
    oracle = dict(commits=commits, culprit_index=args.culprit, culprit=commits[args.culprit], metric=METRIC,
                  scenario=SCENARIO, driver_major=driver)
    (out / "oracle.json").write_text(json.dumps(oracle, indent=2) + "\n")
    print(json.dumps(dict(output=str(out), culprit=commits[args.culprit], culprit_index=args.culprit)))


def gpu_state():
    fields = "clocks.sm,clocks.mem,temperature.gpu,power.draw,pstate,utilization.gpu"
    result = subprocess.run(["nvidia-smi", "--query-gpu=" + fields, "--format=csv,noheader,nounits"],
                            capture_output=True, text=True)
    values = [v.strip() for v in result.stdout.split(",")]
    names = ["sm_mhz", "mem_mhz", "temp_c", "power_w", "pstate", "util_pct"]
    return " ".join(f"{n}={v}" for n, v in zip(names, values)) if len(values) == len(names) else "unavailable"


def probe(args):
    state = json.loads(args.config.read_text())
    if args.revision not in state["commits"]:
        raise ValueError("revision is outside the generated history")
    options = argparse.Namespace(**state["arguments"])
    work = Path(state["worktree"])
    evidence = args.probe_evidence / "hardware-run"
    with args.config.with_suffix(".lock").open("w") as lock:
        corpus.fcntl.flock(lock, corpus.fcntl.LOCK_EX)
        git(work, "checkout", "--detach", "--force", args.revision)
        before = gpu_state()
        scenario = dict(SCENARIO, culprit_index=state["culprit_index"])
        observation = corpus.test_observation(options, Path(state["wrapper"]), work, scenario, "cuda_perf",
                                              state["commits"].index(args.revision), args.revision, args.seed,
                                              "bisect", evidence)
        after = gpu_state()
    result = {k: observation[k] for k in ("category", "signature", "detail") if observation.get(k)}
    if result["category"] == "INCONCLUSIVE":
        result["category"] = "UNKNOWN"
    log = (evidence / "bazel.log").read_text() if (evidence / "bazel.log").exists() else ""
    values = re.findall(r"REMEX_METRIC " + METRIC + r"=([0-9.]+)", log)
    if result["category"] == "PASS" and values:
        result["metrics"] = {METRIC: float(values[-1])}
    result["detail"] = (result.get("detail", "") + f" gpu before: {before}; after: {after}").strip()
    result["evidence"] = [str(evidence)]
    print(json.dumps(result))


def bisect(args):
    args.repository = args.repository.resolve()
    oracle = json.loads((args.repository / "oracle.json").read_text())
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    wrapper = corpus.make_wrapper(args, evidence)
    work = evidence / "worktree"
    git(args.repository, "worktree", "add", "--detach", str(work), oracle["commits"][-1])
    state = evidence / "probe-state.json"
    arguments = {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()}
    corpus.save(state, dict(arguments=arguments, commits=oracle["commits"], culprit_index=oracle["culprit_index"],
                            worktree=str(work), wrapper=str(wrapper)))
    config = dict(bisect=dict(revisions=oracle["commits"], alpha=args.alpha, budget=args.budget, seed=0,
                              metric=dict(name=METRIC, runs=args.runs)),
                  runner=dict(repository=str(args.repository), evidence_directory=str(evidence / "probes"),
                              probe_command=[sys.executable, str(Path(__file__).resolve()), "probe", "--config", str(state)]))
    corpus.save(evidence / "bisect-config.json", config)
    try:
        code, output, elapsed = corpus.command([args.remex, "bisect", "--config", evidence / "bisect-config.json"],
                                               args.repository, evidence / "remex-result.json", args.experiment_timeout)
        result = json.loads(output) if code == 0 else dict(conclusion="INCONCLUSIVE", reason="remex did not finish")
        summary = dict(exit=code, elapsed_seconds=round(elapsed, 1), expected_culprit=oracle["culprit"],
                       expected_culprit_index=oracle["culprit_index"],
                       culprit_matches=result.get("conclusion") == "CULPRIT" and result.get("culprit") == oracle["culprit"],
                       result=result)
        corpus.save(evidence / "summary.json", summary)
        print(json.dumps(summary, indent=2))
        return 0 if summary["culprit_matches"] else 2
    finally:
        corpus.command([wrapper, "shutdown"], work / SCENARIO["workspace"], evidence / "shutdown.log", 60)
        git(args.repository, "worktree", "remove", "--force", str(work))


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="action", required=True)
    gen = sub.add_parser("generate", help="write a new history; an existing output directory is refused")
    gen.add_argument("--output", type=Path, required=True)
    gen.add_argument("--commits", type=int, default=40)
    gen.add_argument("--culprit", type=int, default=26)
    gen.add_argument("--driver-major", help="the test.gpu.driver_major the test requires (default: nvidia-smi)")
    run = sub.add_parser("bisect", help="run remex bisect on the kernel time against real workers")
    run.add_argument("--repository", type=Path, required=True)
    run.add_argument("--evidence", type=Path, required=True)
    run.add_argument("--alpha", type=float, default=0.05)
    run.add_argument("--runs", type=int, default=10)
    run.add_argument("--budget", type=int, default=400)
    run.add_argument("--experiment-timeout", type=int, default=3600)
    run.add_argument("--bazel", default=str(ROOT / ".tools/bazel.official"))
    run.add_argument("--remex", default=str(ROOT / "build/bin/remex"))
    run.add_argument("--executor", default="grpc://127.0.0.1:18980")
    run.add_argument("--cache", default="grpc://127.0.0.1:19092")
    run.add_argument("--admin", default="http://127.0.0.1:18081")
    run.add_argument("--instance", default="remex-perf-history")
    run.add_argument("--bazel-flag", action="append", default=[])
    run.add_argument("--cuda-flag", action="append", default=[])
    run.add_argument("--timeout", type=int, default=1800)
    adapter = sub.add_parser("probe", help=argparse.SUPPRESS)
    adapter.add_argument("--config", type=Path, required=True)
    adapter.add_argument("revision")
    adapter.add_argument("seed", type=int)
    adapter.add_argument("probe_evidence", type=Path)
    args = parser.parse_args()
    if args.action == "generate":
        generate(args)
    elif args.action == "probe":
        probe(args)
    else:
        args.rtl_flag, args.selector = [], str(ROOT / ".tools/target-determinator")
        sys.exit(bisect(args))


if __name__ == "__main__":
    main()
