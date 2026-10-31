# Seeded hardware regression corpus

`cuda/race:race_test` launches a real CUDA kernel. Its healthy path uses
`atomicAdd`; the planted path performs conflicting non-atomic global writes,
with a block barrier exposing the lost update. `rtl/flaky:fifo_flaky_test`
executes a Verilator-generated model of an independent SystemVerilog FIFO. Its
planted bug increments occupancy during simultaneous push/pop. Both Bazel tests
have `remex-hw` and `no-remote-cache` tags and require their GPU/RTL execution pool.

Both tests consume an unsigned 64-bit `REMEX_SEED`. SplitMix64(seed) modulo 4 gates
the faulty path. Seed 0 is quiet; seeds 6 and 9 trigger it. The approximately
25% figure describes this seed gate over distributed seeds, not a claimed
intrinsic probability of GPU race scheduling. A fixed seed repeats the hardware
inputs. Device/runtime errors have a separate infrastructure diagnostic.

Generate a **new** history; existing destinations are refused:

```sh
python3 scripts/hardware-corpus.py generate --output build/planted-hardware-history
```

The generated `oracle.json` names all 96 commit IDs. Existing deterministic C++,
CUDA and RTL defects start at indices 31, 47 and 63. The CUDA race starts at 72,
and the independent RTL seed-gated defect at 80. Native `flaky.cc` remains at 72
for algorithm testing. The JSON records source planting, not executed evidence.

With real remex workers and CAS running, start with the CUDA boundary. The example
uses CUDA 13.2, GCC 15 and a matching NVIDIA userspace library unpacked under
`.deps/`; adapt these paths and flags to the actual worker toolchain:

```sh
python3 scripts/hardware-corpus.py run \
  --repository build/planted-hardware-history \
  --evidence evidence/hardware-cuda-boundary \
  --indices 71-72 --scenarios cuda_race --seeds 0,6 \
  --instance remex --bazel-flag=--jobs=4 \
  --cuda-flag=--repo_env=CUDA_PATH=/usr/local/cuda-13.2 \
  --cuda-flag=--@rules_cuda//cuda:archs=sm_89 \
  --cuda-flag=--@rules_cuda//cuda:copts=--allow-unsupported-compiler \
  --cuda-flag=--action_env=LD_LIBRARY_PATH="$PWD/.deps/nvidia-595.91.07/root/usr/lib/x86_64-linux-gnu" \
  --cuda-flag=--test_env=LD_LIBRARY_PATH="$PWD/.deps/nvidia-595.91.07/root/usr/lib/x86_64-linux-gnu"
```

For RTL use a fresh evidence directory with `--indices 79-80 --scenarios
rtl_flaky --seeds 0,6`. For the complete experiment use `--indices all
--scenarios cuda,rtl,cuda_race,rtl_flaky` and the desired seeds. By default, four
seeds include one triggering seed. More seeds measure the planted gate more
faithfully. A boundary run does not validate all 96 revisions.

The evaluator uses a separate detached worktree and Bazel output root, with
persistent servers for the evaluated workspaces and batch mode for disposable
selector worktrees. It runs
real `bazel query` to verify the requested tests and their tags, then real
`target-determinator` followed by `remex select`. The first evaluated revision
runs all requested targets as the selected-arm baseline. Pass `--environment
current-environment.json` containing verified nonempty `image` and `driver`
identities, plus any relevant `platform`, `bazel_flags` and toolchain identities,
to establish a stable selection environment. The file is reread at each
revision; changes force full selection. Missing environment evidence, selector
errors, and policy-sensitive changes conservatively select every requested
hardware target. No source-path heuristic replaces target-determinator.

The full oracle and selected arm execute independently with identical seeds.
An unselected target retains only the selected arm's earlier observation for
that same seed; its source revision remains in the comparison record. A fresh
cross-arm disagreement is reported separately from a missed failure. Normal
compilation may hit AC, while tests always run once with test caching disabled
and remote fallback disabled. Worker pool, empty cgroup, GPU NVML evidence and
the winning test action are checked. The BEP `test.log` digest must match that
winning attempt's output digests; a compilation attempt cannot stand in for
test execution.

Each invocation preserves its seed, exact command, revision, BEP, gRPC log,
execution log, invocation-scoped ledger, classified observation and matched
hardware attempt. `comparisons.jsonl` preserves incremental results;
`summary.json` reports actual execution scope, selection outcomes, oracle
mismatches and GPU test durations. Exit status 0 requires every comparison to
agree and every observation to match the planted oracle; infrastructure or
incomplete evidence is not counted as success. These artifacts remain after
the evaluator removes its temporary worktree.

Run the planner/evidence regressions with:

```sh
python3 -m unittest discover -s tests/regress -p 'test_*.py' -v
```

These Python tests verify generation and evidence rules; they do not claim
hardware execution.

For actual hardware flaky bisection, use `bisect --scenario cuda_race` or
`bisect --scenario rtl_flaky`, with the same repository, connection and
toolchain flags as `run`, and a fresh evidence directory. For example:

```sh
python3 scripts/hardware-corpus.py bisect \
  --repository build/planted-hardware-history --evidence evidence/rtl-flaky-bisect \
  --scenario rtl_flaky --instance remex --calibration-runs 64 --budget 1000 --seed 0
python3 scripts/hardware-corpus.py git-baseline \
  --repository build/planted-hardware-history --evidence evidence/rtl-flaky-git \
  --scenario rtl_flaky --instance remex --seed 0
```

`bisect` invokes the actual `remex bisect` CLI. Each instrumented probe checks out
the requested commit and performs a fresh Bazel/remex hardware test; it does not
look up an earlier oracle result. One private warm workspace keeps ordinary
compilation reusable across probes. `git-baseline` runs actual `git bisect run`
with one increasing seed per trial: PASS maps to good, the expected failure
signature to bad, and infrastructure or unrelated failures to skip. This is a
one-shot baseline with no flaky confidence guarantee, so its reported culprit
can differ from the planted oracle. Both commands save their raw results and
an explicit `culprit_matches_oracle` comparison. The remex command additionally
retains calibration, confidence bounds, fresh confirmation trials and budget
accounting in its result JSON. Neither command claims success merely because a
process exited normally.

For the same git-bisect flow with Bazel's three-attempt retry policy, use a
separate evidence directory and add `--baseline-retry-attempts 3`. This option
only affects `git-baseline`; the oracle and remex statistical probes still execute
one test attempt per seed. Each retry keeps its BEP event, downloaded `test.log`,
actual failure signatures and candidate matching winner IDs in
`hardware-retries.json`. Identical logs may make individual retry-to-winner
pairing ambiguous; the proof requires an equal number of distinct hardware
winners and a complete matching, and explicitly records that ambiguity.
A failing attempt followed by a pass remains a passing Bazel baseline outcome
with `flaky_observed=true`; the failed attempt is retained as evidence.

All retries within one baseline trial receive the same fixed `REMEX_SEED`.
Consequently, retries repeat the same gated inputs in these examples. This
comparison does not measure general Bazel retry effectiveness for uncontrolled
hardware nondeterminism. The baseline advances its seed between git-bisect
trials. Seeds beginning at 0 can remain quiet for every visited revision and
misidentify the already-declared bad endpoint without executing that endpoint.

The driver runs commands under a Linux child subreaper. On timeout or death of
its owning driver, the supervisor stops and kills the complete owned descendant
tree, including probe processes which created another session and orphaned
grandchildren. Query and selector commands use the same boundary. Successful
commands may retain their normal persistent Bazel server; experiment cleanup
explicitly shuts down the private workspace servers before removing worktrees.

A stopped `run` can continue into a **new** evidence directory with
`--resume-from <old-evidence>`. Complete revisions are imported after checking
command flags, Bazel version, exact invocation-scoped BEP/ledger hardware
proof, original observation identities, expected outcomes and a fresh `remex
classify` of the preserved raw evidence. An incomplete revision is rerun in
full. `resume-provenance.json` records hashes of imported artifacts and the
original JSONL, including selection/environment records; later continuations
verify this chain. Infrastructure observations remain inconclusive after
import. New runs record executable hashes and an experiment contract; changed
contracts cannot resume. Old runs without such a manifest require explicit
`--resume-legacy`, which records that historical executable hashes are
unavailable rather than pretending to verify them.

Optional `--selector-cache` requires repeated `--selector-input PATH` roots
covering the native toolchain/system inputs. Each lookup fingerprints the
actual Bazel external dependency tree, declared input roots, executable
contents, complete process environment hash, fixed flags, recorded device
and environment, and exact before/after workspace Git trees. Other tracked
repository files also participate. Only this generator's numeric
`revision.txt` marker is excluded, after checking its value, absence of a root
Bazel workspace and absence of tracked references to it. A different corpus
layout disables this optimization. Files are checked by inode, size, mode,
mtime and ctime on every fingerprint, with content hashes recomputed on any
change; symlink targets are included. Hashing a large toolchain costs time on
the first lookup.

Only a successful actual target-determinator call with unchanged inputs can
populate the cache. A hit references the original stdout and its digest in
`*-selector-provenance.json`; it is never reported as a new selector call.
Global fallback and environment-change rules still run through `remex select`
for each revision. Cache failures fall back conservatively. Oracle tests and
selected-arm tests remain fresh independent hardware executions; this cache
does not reuse their results or suppress oracle revisions.

The cache verifies `external/_main` points at the checked main workspace and
excludes that backlink from recursive external dependency hashing. The main
workspace is represented by its Git tree, with a clean tracked/untracked
check. Only the root `MODULE.bazel.lock` and Bazel's four standard output
symlinks may be ignored; the lockfile is still fingerprinted. This prevents
`bazel-*` output cycles from treating fresh test logs and volatile build
status as changed selector inputs. Other ignored files disable caching.

`--reuse-resume-workspace` additionally reuses a predecessor's **cleaned,
stopped** temporary worktree location and Bazel output state to avoid another
cold dependency download. It refuses an existing worktree and records the
locations in `workspace-provenance.json`. Original invocation evidence is
never overwritten. Each new driver also archives its source and SHA-256.

The reported GPU test duration is worker execution time, including the test
wrapper and CUDA initialization. It is not GPU kernel active time or device
utilization. Final analysis should use the winning ActionResult's execution
start/end timestamps and report repair runs and infrastructure overhead
separately. Wall time from concurrent CLI experiments is not a GPU-time metric.
