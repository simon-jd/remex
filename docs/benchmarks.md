# Benchmarks

The method, full tables and caveats of every benchmark; the
[README](../README.md) sums them up in one table.
[results/README.md](../results/README.md) lists what each result file
measured and at which commit.

Every comparison below is against an earlier
version of this scheduler, against `git bisect`, or against plain local
Bazel on the same cores (on several machines, on one of them). No existing REAPI scheduler (Buildbarn,
Buildfarm, NativeLink) was run on the same builds, so nothing here says how
remex compares with one. The earlier scheduler versions that
`results/` keeps records for are not in this repository's history.

## End-to-end build

`scripts/benchmark-cpu.py` builds and tests
`samples/cpp-history` (`//:math_test` and `//:abseil_test`: 90 compile, link
and test actions) from `bazel clean`, locally and through remex. PostgreSQL,
bazel-remote, the scheduler, four CPU workers and Bazel all run on the same
four cores (one thread on each of four P-cores), and local builds use four
jobs. Cold builds start from an empty cache, warm builds reuse it, and every
build reruns the tests. Five trials each, of this scheduler and workers
(`results/cpu-build.jsonl`) and of the earlier ones whose
idle workers polled Lease every 500 ms (`results/cpu-build-lease-poll.jsonl`), each with its local builds; median wall time with the range:

| Build | Cold | Warm |
|---|---|---|
| Local Bazel, disk cache | 6.7 s (5.6–7.6) | 3.3 s (3.2–3.7) |
| Through remex and bazel-remote | 8.7 s (8.2–10.6) | 3.9 s (3.4–4.5) |
| Earlier run: local Bazel | 6.5 s (6.2–7.3) | 3.6 s (3.3–3.7) |
| Earlier run: remex, 500 ms Lease polls | 11.3 s (10.9–12.9) | 3.8 s (3.5–4.5) |

- **Idle workers wait in Lease.** An idle worker's Lease is now held until
  work is queued for it (for up to 1 s, then the worker asks again). With
  the 500 ms polls, actions of a cold build spent 8.7–9.5 s queued in all,
  every second of it while a worker sat idle
  (`results/cpu-build-lease-poll-breakdown.jsonl`); now 0.02–0.1 s, and the
  median action is leased in under 0.1 ms instead of 67–89 ms. The
  scheduler's span from the first execution to the last commit fell from
  7.6–9.3 s to 4.7–5.5 s, and the four workers were busy 80–86% of it
  instead of 50–59%. Cold remote builds take 1.3 times as long as local
  ones, down from 1.7.
- **Where the rest goes.** By Bazel's own timing
  (`results/cpu-build-breakdown.jsonl`), most of the remaining 2.0 s is in
  the execution phase: 5.1 s (4.8–5.6) remotely against 3.3 s (2.8–3.6)
  locally. Bazel also starts the first action later, at 3.8 s against
  3.3 s (medians; the ranges overlap). Workers spent 16–18 s on the 90
  actions, 12.6–13.9 s of it running the commands. Per action, a worker
  spends a median 5–6 ms fetching inputs from the CAS, 17–19 ms creating
  output directories, the cgroup and the process and having the scheduler
  record RUNNING, and about 5 ms uploading outputs. For the other 14–20% of
  their time in the span the workers are idle with nothing queued: the
  next actions are waiting for their dependencies' results, or for Bazel
  to upload their inputs.
  A cold build makes 701–703 ByteStream.Write calls to bazel-remote, one
  per missing blob, 3.7–6.8 s summed over calls that overlap.
- **Warm builds are no faster.** They hit the cache for all 88 compile and
  link actions and run only the two tests remotely. The tests no longer
  wait for a lease (97–194 ms each before), but the scheduler's span is
  0.16–0.33 s of a 3.9 s build, and the medians moved by less than the
  run-to-run spread. Compared with local builds, Bazel starts executing
  later (3.5 s against 3.1 s) and its execution phase, here 88 cache
  lookups, 48 output downloads and the two tests, takes 0.22 s against
  0.10 s; these records do not show why the start is later.
- All 40 builds produced identical test binaries. It is one small
  workspace on one host, with the workers sharing the client's cores; for
  separate machines see the next section.

## Several machines

`scripts/benchmark-nodes.py` ran on four CPU_X64_M nodes of a Snowflake
trial compute pool, as one ML job whose four instances are joined by Ray.
Each node has 8 vCPUs (Intel Xeon Platinum 8370C), 32 GB and Ubuntu 24.04
with g++ 13.3. Node 0 runs Bazel 9.3, PostgreSQL 18.6, bazel-remote 2.6.3
and the scheduler. Each other node runs eight `remex-worker` processes,
started by a Ray actor pinned to it, with `--isolation process-group`
(below). The nodes have no internet: the binaries, built in an
`ubuntu:24.04` container, and the workspace come from the job's stage,
checked against SHA-256 sums.

The workload is `//worker:remex-worker` at commit c5338cd: this
repository's worker, with gRPC 1.84, protobuf, BoringSSL and Abseil built
from source. A build is 3,549 actions, 2,780 of which run commands, and every
external repository is vendored (`--vendor_dir`). The method is that of
`scripts/benchmark-cpu.py`: every build starts from `bazel clean`, cold
builds start from an empty cache (a new disk cache, or a new bazel-remote
and PostgreSQL), warm builds reuse it, and analysis runs once before any
timed build. Local builds run on node 0 with 8 jobs and the process-wrapper
sandbox; remex builds use one job per worker. Three trials
(`results/nodes-scaling.jsonl`; the job's report and logs are in
`results/snowflake/20261105T075322Z-remex-scale/`); median wall time with
the range:

| Build | Workers | Cold | Warm |
|---|---|---|---|
| Local Bazel on node 0, disk cache | (8 jobs) | 762 s (758–764) | 5.5 s (5.2–7.7) |
| remex, one worker node | 8 | 867 s (865–869) | 6.6 s (6.2–8.0) |
| remex, two worker nodes | 16 | 434 s (427–459) | 6.2 s (5.9–6.9) |
| remex, three worker nodes | 24 | 296 s (296–305) | 6.0 s (5.9–6.8) |

- **Three worker nodes build 2.6 times as fast as local Bazel** on one
  node, and 2.9 times as fast as one worker node. One worker node takes
  1.14 times as long as local Bazel, with the same number of cores running
  actions; node 0's own cores then run only Bazel, the scheduler,
  PostgreSQL and bazel-remote.
- **Where the time goes.** Actions hardly waited for a worker: the
  median wait for a lease is 0 ms in every build, and the time all 2,780
  actions spent queued sums to at most 1.4 s per build. Workers held a
  lease for 97–98% of the scheduler's span with one node, 96% with two and
  94–95% with three. Per action, the medians are 91–93 ms fetching inputs
  from bazel-remote on node 0 with one worker node and 105–106 ms with
  three, 6–7 ms of setup, 425–476 ms running the command and 24–29 ms
  uploading outputs. Input fetches are 6–7% of the
  workers' time; the commands are 89–92%. Bazel uploads the inputs the
  cache lacks with 32,133 ByteStream.Write calls per cold build.
- **Warm builds never reach the scheduler.** All 2,780 actions hit
  bazel-remote's action cache, and the builds take 0.5–1.1 s longer
  (medians) than local Bazel's hits in its disk cache.
- All 24 builds produced the same `remex-worker` binary.
- **Isolation mode.** The nodes run as root without `CAP_SYS_ADMIN`, and
  `/sys/fs/cgroup` is a tmpfs over cgroup v1 hierarchies: a worker cannot
  get a delegated cgroup v2 subtree there (the probe in
  `results/snowflake/20261105T062048Z-remex-probe2/`). The workers
  therefore ran with `--isolation process-group` ([worker/README.md](../worker/README.md)):
  every command in a process group of its own, the worker a child
  subreaper that kills the group and every descendant before it reports an
  attempt, and rlimits per process. Leases, fencing and the ledger are the
  same in both modes; what this mode lacks is per-action memory accounting
  and OOM counts, which these rows therefore do not have.

**Faults across machines.** The same cold build on three worker nodes, in
the job `20261105T164448Z-remex-faults` (`results/nodes-faults.jsonl`):
100 s in, about a third of the way through, either every worker process
on the third node is SIGKILLed and left dead, or the scheduler is
SIGKILLed and started again 1 s later. Three runs of each; medians with
the range, against 296 s for the same build without a fault:

| Fault | Build | Kill to build done | Executions committed once, twice, never | Attempts run again |
|---|---|---|---|---|
| The third node's 8 workers killed | 408 s (392–432) | 308 s (292–332) | 2,780, 0, 0 | 8 in each run |
| Scheduler killed, back 1.1–1.2 s later | 299 s (296–301) | 199 s (196–201) | 2,780, 0, 0 | 0 |

- **A killed worker node costs its share of the remaining work, not
  results.** The eight leases its workers held expired, and those eight
  actions ran again on the other two nodes. Every execution was committed
  exactly once, and the build lost no results. It took 112 s longer at
  the median, about what losing a third of the workers for the rest of
  the build costs. Three seconds after the kill, no action process was
  left on the killed node.
- **A killed scheduler cost nothing measurable.** The restarted scheduler
  recovered the 24 leases running at the kill, every one of those attempts
  finished and was committed, and no action ran twice. The builds took as
  long as builds without a fault.
- All six builds produced the same `remex-worker` binary as the scaling
  runs.

## Control plane under load

`cmd/remex-loadtest` drives the scheduler in
process: 64 synthetic workers loop Lease → Heartbeat → Complete against a
ledger that already holds 0–5,000 finished executions, while a probe worker
heartbeats one long-running action every 100 ms. `-stall 3s` makes every
action-cache write take 3 s, and `-queue N` keeps N runnable actions queued
ahead of the workers. `scripts/bench-control-plane.sh` ran each cell five
times for 20 s (`results/control-plane.jsonl`, measured before the
deadline heap described below); actions per second are the median with the
range over the five runs:

| Store | History | Queued | AC write stall | Actions/s | Probe heartbeat, median / worst | Attempts lost |
|---|---|---|---|---|---|---|
| PostgreSQL | 0 | – | – | 1,479 (1,173–1,592) | <0.01 ms / 3.3–5.7 ms | 0 |
| PostgreSQL | 1,000 | – | – | 1,500 (1,272–1,603) | <0.01 ms / 3.1–9.4 ms | 0 |
| PostgreSQL | 1,000 | – | 3 s | 21 | <0.01 ms / 3.0–3.7 ms | 0 |
| memory | 0 | – | – | 25,863 (22,077–27,511) | <0.02 ms / 49–249 ms | 0 |
| memory | 1,000 | – | – | 27,030 (19,168–28,389) | <0.02 ms / 37–177 ms | 0 |
| memory | 5,000 | – | – | 26,334 (20,586–27,406) | <0.02 ms / 33–303 ms | 0 |
| memory | 1,000 | – | 3 s | 21 | <0.02 ms / 0.05–0.07 ms | 0 |
| memory | 0 | 1,000 | – | 26,683 (24,183–29,480) | <0.01 ms / 73–221 ms | 0 |
| memory | 0 | 10,000 | – | 26,603 (24,797–27,736) | <0.01 ms / 24–197 ms | 0 |
| memory | 0 | 50,000 | – | 25,684 (24,895–28,986) | <0.01 ms / 81–321 ms | 0 |

- **A slow cache.** With every action-cache write stalled for 3 s, each
  worker waits for its own write, so 64 workers cap the loop at
  64 / 3 s ≈ 21 actions/s. Nobody else waits: publication calls the cache
  outside the scheduler lock, the probe's heartbeats stay under 4 ms, and no
  lease expires.
- **Group commit** puts 27.5 ledger changes in each PostgreSQL transaction
  on average, and a call answers once everything it observed is durable.
  Each synthetic worker still waits for its own commits, so the loop is
  bound by commit latency: more concurrent workers make bigger groups. With
  the in-memory store there is no fsync to share.
- **Deep queues.** Lease takes the oldest action from a per-platform queue,
  so throughput does not depend on how much work is queued. Sorting every
  runnable action on each Lease, as the scheduler did before, completed a
  median of 1,640, 162 and 26 actions/s with 1,000, 10,000 and 50,000
  queued, and the probe's heartbeat then waited up to 3.3 s
  (`results/control-plane-deep-queue-sorted.jsonl`).
- **What this does not show.** Arrival is closed-loop: every worker submits
  one action and then runs one, so there is no open-loop arrival rate. The
  probe's heartbeats change nothing and get CONTINUE, one of the two answers
  allowed to skip the durability wait ([design.md](design.md)).
  With the in-memory store the worst probe heartbeat is tens to hundreds of
  milliseconds in every cell, queued work or not: these runs predate the
  chunked event log, and the worker sweep below measures what copying the
  log cost. Ranges are over five runs, not confidence intervals.
- **Where.** One host: Intel Core i9-13900K (32 threads), 123 GiB of RAM,
  a Samsung 990 EVO Plus NVMe, Linux 7.0, Go 1.27.2, and PostgreSQL 18.6 in
  a container with default settings (fsync and synchronous_commit on,
  wal_sync_method fdatasync, shared_buffers 128MB). Every record carries its
  environment and the commit it measured.

## More workers

Lease, Heartbeat and Complete used to scan every live
attempt's lease deadline under the scheduler lock; they now keep deadlines
in a min-heap with a per-worker index. With 4,096 workers on the in-memory
store that raised the median from 3,361 to 23,559 actions/s
(`results/control-plane-workers-deadline-scan.jsonl`, three
runs, against `results/control-plane-workers-event-slice.jsonl`). The
in-memory ledger also kept every event in one slice, and each time the
slice outgrew its capacity the commit that grew it copied the whole log
under the lock: up to about 300 MB at 4 million events. Commits now append
events to chunks of 4,096 (`scheduler/eventlog.go`), and the audit export
still lists every event in order. `REMEX_BENCH_MATRIX=workers
scripts/bench-control-plane.sh` runs the empty-history cells with 64, 512
and 4,096 workers, five runs each, with the chunked log
(`results/control-plane-workers.jsonl`) and with one slice
(`results/control-plane-workers-event-slice.jsonl`):

| Store | Workers | Actions/s, chunks | Actions/s, one slice | Probe heartbeat median / worst, chunks | Same, one slice |
|---|---|---|---|---|---|
| memory | 64 | 32,310 (31,074–34,180) | 27,770 (24,838–30,548) | <0.01 ms / 4.4–93 ms | <0.01 ms / 33–211 ms |
| memory | 512 | 32,255 (28,217–32,812) | 27,918 (23,675–28,962) | 2.0 ms / 17–51 ms | 2.2 ms / 35–276 ms |
| memory | 4,096 | 25,579 (24,394–27,953) | 23,559 (20,818–24,761) | 23 ms / 73–134 ms | 23 ms / 112–204 ms |
| PostgreSQL | 64 | 1,570 (1,385–1,617) | 1,268 (1,188–1,425) | <0.01 ms / 2.1–7.7 ms | <0.01 ms / 2.5–9.5 ms |
| PostgreSQL | 512 | 2,799 (2,699–2,869) | 2,680 (1,977–2,707) | <0.01 ms / 9.3–30 ms | <0.01 ms / 20–43 ms |
| PostgreSQL | 4,096 | 3,448 (3,298–3,600) | 3,165 (2,874–3,419) | <0.01 ms / 67–129 ms | <0.01 ms / 75–217 ms |

- **The copies were most of the worst case.** With 64 workers, three of
  the five runs had no probe heartbeat over 7.2 ms, and two heartbeats in
  all took over 10 ms, against 29 with one slice, of which 21 waited on a
  copy. The 10 s runs stayed at or below 2.7 ms with GC on and 2.6 ms with
  `GOGC=off` (one slice: 16–62 ms and 7–37 ms). The in-memory store
  completes 9–16% more actions per second. The longest stop-the-world GC
  pause fell from the runtime histogram bucket ending at 50 ms to 0.5 ms
  with 512 workers, and from 101 ms to 3.7 ms with 4,096, consistent with
  the runtime having to wait for a copy in progress before it can stop
  the world.
- **What is left.** For every probe heartbeat the load test now records
  whether a GC cycle was marking while it waited (5–7% of heartbeats in
  the in-memory cells with GC on), and for each one over 10 ms how many goroutines
  were blocked on a lock meanwhile (the runtime's sampled total). Both
  slow heartbeats at 64 workers, 10.8 and 93 ms, overlapped a GC mark.
  During the 93 ms one, 70 goroutines on average were blocked on a lock,
  so every worker was queued behind one holder; the records do not show
  what kept the holder that long.
- **More workers queue for the one lock.** During the five slowest
  heartbeats of each run, 150–1,300 goroutines were blocked on locks with
  512 workers and 2,200–6,200 with 4,096. That queue sets the 4,096-worker
  median of 23 ms, the same with either log: of the 517 heartbeats over
  10 ms only 50 overlapped a GC mark, close to the 6.8% of all heartbeats
  that did. A mark makes the slowest ones slower: 20 of the 25 slowest
  with 4,096 workers and 16 of 25 with 512 overlapped one. Splitting the
  scheduler lock is not implemented.
- **PostgreSQL.** These cells are 4–24% faster too, although their ledgers
  hold only 0.2–0.5 million events, so the copies were small. The two
  sweeps were not interleaved; part of that difference may be the host.

## Scheduler restart

`tests/faults/bazel_restart_test.py` runs a real
Bazel 9.3 build against a scheduler with a PostgreSQL ledger and 2 s leases,
SIGKILLs the scheduler at a chosen phase, and restarts it 0.8 s later. Five
runs per phase (`results/scheduler-restart.jsonl`, rows with
`"worker": "after"`):

| Scheduler killed while the action | Same attempt finished | Kill to Bazel done |
|---|---|---|
| runs (an 8 s command) | 5 of 5 | 8.4–8.5 s |
| uploads its 64 MiB output | 5 of 5 | 2.1–3.9 s (median 2.2) |

- **Every other fault keeps its outcome.** A queued action runs once. A
  lease whose response is held past the lease is retried at epoch 2. A
  crash before or after the action-cache write publishes the committed
  result exactly once. Killed and stopped workers are retried.
- **Down longer than the lease (3 s):** a running attempt is abandoned and
  retried. An attempt that had already finished still delivers its result,
  and the restarted scheduler accepts it under the recovered lease.

## Bisecting flaky failures

The algorithm is described in the [README](../README.md#bisecting-flaky-failures)
and in [design.md](design.md#regression-tooling).

`cmd/remex-bisectsim` runs 1,000 bisections per cell on simulated 1,000-commit
histories. The culprit is placed at random and fails with probability p, and
each run separately hits an infrastructure failure with probability 10%. The
model assumes independent runs, a constant failure rate and one failure
signature. The baselines are `git bisect run` with three probe scripts: one
that only checks the exit code, so an infrastructure failure looks like a
test failure; one that exits 125 on an infrastructure failure, so git skips
that revision; and one that reruns the test until it has N results, retries
infrastructure failures and reports bad at the first test failure, with N
swept from 1 to 100. remex runs with the CLI defaults (α = 0.05, a budget
of 2,000 runs). Results are in `results/bisect-simulation.jsonl`:

| Failure rate p | git bisect, exit code only: correct | git bisect, exit 125: correct | git bisect, smallest N reaching 99% correct: correct / wrong, median runs | **remex: correct / wrong / inconclusive** | remex runs, median (p95) |
|---|---|---|---|---|---|
| 1 | 58% | 81% | N = 1: 100% / 0%, 11 | **100% / 0% / 0%** | 50 (57) |
| 0.5 | 3% | 5% | N = 10: 99.3% / 0.7%, 67 | **96.3% / 0% / 3.7%** | 128 (196) |
| 0.25 | 0.2% | 0.9% | N = 30: 99.8% / 0.1%, 186 | **95.5% / 0% / 4.5%** | 275 (467) |
| 0.1 | 0.1% | 0.3% | N = 100: 99.8% / 0.1%, 609 | **93.6% / 0% / 6.4%** | 725 (1,180) |

- **With the failure rate known, reruns win.** A rerun count picked for the
  true p is right at least as often as remex and costs fewer runs in every
  row, at the price of a few wrong answers (up to 0.7%) where remex gave
  none. `exit 125` alone stops infrastructure noise from blaming the wrong
  commit for a deterministic failure (81% correct, the rest inconclusive),
  but one run per revision still misses flaky failures.
- **remex does not need the failure rate.** One N has to cover the rarest
  flake expected. N = 100 covers p = 0.1 but costs a median of 561 runs on a
  deterministic failure, where remex needs 50; N = 10 is cheap but wrong 85%
  of the time at p = 0.1. remex calibrates on the known-bad revision and
  sizes its probes from that.
- **Wrong is rare; inconclusive is the price.** remex gave no wrong answer in
  any cell above. Zero in 1,000 trials bounds its wrong rate at 0.3% (the
  one-sided 95% Clopper–Pearson bound, `wrong_upper95` in the results);
  across all 16 configurations it was wrong once in 16,000 bisections. Every
  inconclusive answer comes from the final confirmation rejecting the
  boundary the search reached, after a bad revision passed all its runs.
- **Why the bound stays exact.** Stopping at the r-th failure keeps the
  Clopper–Pearson bound exact, because
  P(r-th failure within n runs) = P(Binomial(n, p) ≥ r).

## GPU performance bisection: one run, synthetic 40-commit history

With `bisect.metric` set,
remex bisects a measured value instead of a failure (`regress/metric.go`):
it measures both endpoints, classifies each probed revision by which
endpoint's median its values are nearer, and requires a one-sided
Mann–Whitney U p-value below α against the other endpoint's values (checked
against scipy in `regress/testdata/scipy-reference.json`).
`scripts/perf-history.py` generates a 40-commit Bazel CUDA workspace whose
test transposes a 4096 × 4096 matrix and prints the median time of 100
launches. Commit 26 drops the shared-memory tile, so each warp's writes are
no longer coalesced; the output stays correct and the test keeps passing.
One run on real workers, the test compiled on a CPU worker and run on the GPU
worker (`results/cuda-perf-bisect.json`):

| | Revisions measured | Runs | Median kernel time | Result |
|---|---|---|---|---|
| Known good, index 0 | 1 | 10 | 0.1565 ms | |
| Known bad, index 39 | 1 | 10 | 0.4701 ms | p = 8×10⁻⁵ |
| Search | 19, 29, 24, 26, 25 | 50 | 0.1565 / 0.4700 ms | good, bad, good, bad, good |
| Confirmation | 25 and 26 | 40 | 0.1565 / 0.4700 ms | p = 6×10⁻⁶ and 3×10⁻⁶ |

- It named commit 26, the planted one, after 110 test runs in 356 s, all
  inside one 6-minute hold of the GPU lock. `nvidia-smi` after every run
  read P0, SM 2,520 MHz, 38–45 °C and 51–60 W; clocks were not locked.
- The slowdown is large (3.0 times), so this shows the pipeline working on
  real hardware rather than how small a change it can find. Kernel times
  repeat to the timer's resolution, so the p-values come from the normal
  approximation with ties; each is per probe, with no correction across
  the search.

## Planted failures on real hardware

`scripts/hardware-corpus.py` is a harness that plants five regressions in a
96-commit history (deterministic C++, CUDA and RTL bugs, and a seed-gated
CUDA race and RTL FIFO bug) and bisects them on real workers
([samples/hardware-corpus.md](../samples/hardware-corpus.md)). No results
from it are committed.
