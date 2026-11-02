# Results

Each file holds one JSON object per run or per simulated configuration.

| File | Produced by | What it measured |
|---|---|---|
| `control-plane.jsonl` | `scripts/bench-control-plane.sh` (`cmd/remex-loadtest`) | The scheduler in this repository: five runs of every cell, including deep queues. Each record names the commit it measured and its environment (CPU, memory, disk, kernel, Go, PostgreSQL version and settings). |
| `control-plane-workers.jsonl` | `REMEX_BENCH_MATRIX=workers scripts/bench-control-plane.sh` | The scheduler with the deadline heap: 64, 512 and 4,096 workers on both stores, and 10 s in-memory runs with GC on and `GOGC=off`, five runs each. Records carry GC cycles and pauses, the event log's size and reallocations, and the probe heartbeats over 10 ms with what they overlapped. |
| `control-plane-workers-deadline-scan.jsonl` | the same, with `cmd/remex-loadtest` built at commit f30b45c | The scheduler before the deadline heap, three runs of each cell. Its load test predates the GC and slow-heartbeat fields. |
| `control-plane-deep-queue-sorted.jsonl` | `cmd/remex-loadtest -queue` with the bench script's flags | The deep-queue cells at the commit before Lease used per-platform queues, when it sorted every runnable execution on each call (commit recorded in each row). |
| `control-plane-group-commit.jsonl` | `scripts/bench-control-plane.sh` (`cmd/remex-loadtest`) | `group-commit-r1`, `group-commit-r2`: the scheduler after group commit was added, before the environment and commit were recorded. `per-mutation-r1`, `per-mutation-r2`: an earlier version of the scheduler that made one PostgreSQL commit per ledger mutation under the scheduler lock. Two runs of each, in the order listed. |
| `control-plane-per-mutation.jsonl`, `control-plane-per-mutation-repeats.jsonl` | the same load test | More runs of the one-commit-per-mutation version (7 and 4 rows). |
| `control-plane-global-lock.jsonl` | the same load test | An earlier version of the scheduler that copied the whole ledger on every mutation and called the action cache while holding the scheduler lock. |
| `scheduler-restart.jsonl` | `tests/faults/bazel_restart_test.py` | `"worker": "after"`: the worker in this repository. `"worker": "before"`: an earlier worker whose Complete gave up after three immediate tries, so an attempt that was uploading when the scheduler died was run again. `worker_sha256` and `scheduler_sha256` identify the binaries used. |
| `bisect-simulation.jsonl` | `cmd/remex-bisectsim` | The bisection in this repository, with its default budget, against `git bisect run` with an exit-code-only script (`one-shot`, `rerun-5`), with `exit 125` on infrastructure failures (`git-skip`), and with N reruns per revision (`git-rerun`, N in `reruns`), on 96- and 1,000-commit simulated histories. `wrong_upper95` is the one-sided 95% upper bound on the wrong-answer rate. |
| `bisect-simulation-fixed-calibration.jsonl` | `cmd/remex-bisectsim` | An earlier calibration rule, a fixed 20 runs on the known-bad revision instead of sampling until 10 failures, against the exit-code-only baselines, with a 5,000-run budget. |

The earlier versions are not part of this repository; their records are kept
because [docs/design-tradeoffs.md](../docs/design-tradeoffs.md) cites them as
the reason for a design decision. The README's control-plane table comes
from `control-plane.jsonl`, measured at commit 25d4067; the worker sweep
from `control-plane-workers*.jsonl`.

`scripts/bench-control-plane.sh` runs PostgreSQL in a local
`postgres:18-alpine` container on the same host. Only `control-plane.jsonl`,
`control-plane-workers*.jsonl` and `control-plane-deep-queue-sorted.jsonl`
record the hardware, the PostgreSQL configuration and the commit; the older
control-plane files do not.
