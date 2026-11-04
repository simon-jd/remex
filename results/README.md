# Results

Each file holds one JSON object per run or per simulated configuration.

| File | Produced by | What it measured |
|---|---|---|
| `control-plane.jsonl` | `scripts/bench-control-plane.sh` (`cmd/remex-loadtest`) | The scheduler in this repository: five runs of every cell, including deep queues. Each record names the commit it measured and its environment (CPU, memory, disk, kernel, Go, PostgreSQL version and settings). |
| `cpu-build.jsonl` | `scripts/benchmark-cpu.py --cores 8,10,12,14 --jobs 4 --trials 5`, at commit 5ee4b65 | Wall time of local and remote, cold and warm builds of `samples/cpp-history` with workers that long-poll Lease, one row per build, with Bazel's per-strategy action counts, the test binaries' SHA-256, and for remote builds a summary of the scheduler's ledger (executions, median wait for a lease, the scheduler's span, summed worker time, and the ledger's phases). |
| `cpu-build-lease-poll.jsonl` | the same, at commit 3f8873f | The same builds with the earlier scheduler and workers, whose idle workers polled Lease every 500 ms. Its ledger summaries lack the phases. |
| `cpu-build-breakdown.jsonl`, `cpu-build-lease-poll-breakdown.jsonl` | `scripts/benchmark-cpu.py --summarize` (commit ae0f77e) of each run's output directory | For every build of the two runs above: Bazel's timing (analysis, first action, execution phase), for remote builds Bazel's remote calls by method and the ledger summary with the phases each worker recorded in its results (input fetch, setup, command, output upload). |
| `control-plane-workers.jsonl` | `REMEX_BENCH_MATRIX=workers scripts/bench-control-plane.sh`, at commit 8615dfd | The scheduler with the chunked event log: 64, 512 and 4,096 workers on both stores, and 10 s in-memory runs with GC on and `GOGC=off`, five runs each. Records carry GC cycles and pauses, how many probe heartbeats overlapped a GC mark, and the probe heartbeats over 10 ms with whether a GC cycle was marking and how many goroutines were blocked on a lock meanwhile. |
| `control-plane-workers-event-slice.jsonl` | the same, at commit 7951b40 | The scheduler with the deadline heap that still kept its events in one slice, five runs of each cell. Its slow heartbeats record whether a commit reallocated the event log while they waited. |
| `control-plane-workers-deadline-scan.jsonl` | the same, with `cmd/remex-loadtest` built at commit f30b45c | The scheduler before the deadline heap, three runs of each cell. Its load test predates the GC and slow-heartbeat fields. |
| `control-plane-deep-queue-sorted.jsonl` | `cmd/remex-loadtest -queue` with the bench script's flags | The deep-queue cells at the commit before Lease used per-platform queues, when it sorted every runnable execution on each call (commit recorded in each row). |
| `control-plane-group-commit.jsonl` | `scripts/bench-control-plane.sh` (`cmd/remex-loadtest`) | `group-commit-r1`, `group-commit-r2`: the scheduler after group commit was added, before the environment and commit were recorded. `per-mutation-r1`, `per-mutation-r2`: an earlier version of the scheduler that made one PostgreSQL commit per ledger mutation under the scheduler lock. Two runs of each, in the order listed. |
| `control-plane-per-mutation.jsonl`, `control-plane-per-mutation-repeats.jsonl` | the same load test | More runs of the one-commit-per-mutation version (7 and 4 rows). |
| `control-plane-global-lock.jsonl` | the same load test | An earlier version of the scheduler that copied the whole ledger on every mutation and called the action cache while holding the scheduler lock. |
| `scheduler-restart.jsonl` | `tests/faults/bazel_restart_test.py` | `"worker": "after"`: the worker in this repository. `"worker": "before"`: an earlier worker whose Complete gave up after three immediate tries, so an attempt that was uploading when the scheduler died was run again. `worker_sha256` and `scheduler_sha256` identify the binaries used. |
| `bisect-simulation.jsonl` | `cmd/remex-bisectsim` | The bisection in this repository, with its default budget, against `git bisect run` with an exit-code-only script (`one-shot`, `rerun-5`), with `exit 125` on infrastructure failures (`git-skip`), and with N reruns per revision (`git-rerun`, N in `reruns`), on 96- and 1,000-commit simulated histories. `wrong_upper95` is the one-sided 95% upper bound on the wrong-answer rate. |
| `cuda-perf-bisect.json` | `scripts/perf-history.py bisect` | One metric bisection of the generated CUDA history on the RTX 4090 through real workers: the remex result with every probe, run, kernel time, p-value and `nvidia-smi` state, the expected culprit, and when the GPU lock was held. |
| `bisect-simulation-fixed-calibration.jsonl` | `cmd/remex-bisectsim` | An earlier calibration rule, a fixed 20 runs on the known-bad revision instead of sampling until 10 failures, against the exit-code-only baselines, with a 5,000-run budget. |

The earlier versions are not part of this repository; their records are kept
because [docs/design-tradeoffs.md](../docs/design-tradeoffs.md) cites them as
the reason for a design decision. The control-plane table in
[docs/benchmarks.md](../docs/benchmarks.md) comes from `control-plane.jsonl`, measured at commit 25d4067, before the deadline
heap and the chunked event log; the worker sweep from
`control-plane-workers*.jsonl`.

`scripts/bench-control-plane.sh` runs PostgreSQL in a local
`postgres:18-alpine` container on the same host. Only `control-plane.jsonl`,
`control-plane-workers*.jsonl` and `control-plane-deep-queue-sorted.jsonl`
record the hardware, the PostgreSQL configuration and the commit; the older
control-plane files do not.
