# Results

Each file holds one JSON object per run or per simulated configuration.

| File | Produced by | What it measured |
|---|---|---|
| `control-plane-group-commit.jsonl` | `scripts/bench-control-plane.sh` (`cmd/remex-loadtest`) | `group-commit-r1`, `group-commit-r2`: the scheduler in this repository. `per-mutation-r1`, `per-mutation-r2`: an earlier version of the scheduler that made one PostgreSQL commit per ledger mutation under the scheduler lock. Two runs of each, in the order listed. |
| `control-plane-per-mutation.jsonl`, `control-plane-per-mutation-repeats.jsonl` | the same load test | More runs of the one-commit-per-mutation version (7 and 4 rows). |
| `control-plane-global-lock.jsonl` | the same load test | An earlier version of the scheduler that copied the whole ledger on every mutation and called the action cache while holding the scheduler lock. |
| `scheduler-restart.jsonl` | `tests/faults/bazel_restart_test.py` | `"worker": "after"`: the worker in this repository. `"worker": "before"`: an earlier worker whose Complete gave up after three immediate tries, so an attempt that was uploading when the scheduler died was run again. `worker_sha256` and `scheduler_sha256` identify the binaries used. |
| `bisect-simulation.jsonl` | `cmd/remex-bisectsim` | The bisection in this repository, with its default budget, against `git bisect run` with an exit-code-only script (`one-shot`, `rerun-5`), with `exit 125` on infrastructure failures (`git-skip`), and with N reruns per revision (`git-rerun`, N in `reruns`), on 96- and 1,000-commit simulated histories. `wrong_upper95` is the one-sided 95% upper bound on the wrong-answer rate. |
| `bisect-simulation-fixed-calibration.jsonl` | `cmd/remex-bisectsim` | An earlier calibration rule, a fixed 20 runs on the known-bad revision instead of sampling until 10 failures, against the exit-code-only baselines, with a 5,000-run budget. |

The earlier versions are not part of this repository; their records are kept
because [docs/design-tradeoffs.md](../docs/design-tradeoffs.md) cites them as
the reason for a design decision. The README's headline numbers come only
from rows that measured the code here.

`scripts/bench-control-plane.sh` runs PostgreSQL in a local
`postgres:18-alpine` container on the same host. The records do not capture
the hardware or the PostgreSQL configuration.
