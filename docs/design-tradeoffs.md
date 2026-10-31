# Design trade-offs

Most of remex follows from a few choices about where the ledger lives and
when an answer may leave the scheduler: one leader with the ledger in memory,
group commit, fencing on the scheduler, and committing a result before it is
cached. The bisection tool adds one more, its stopping rule. Some of these
choices replaced an earlier design of the scheduler or worker; where they
did, the measurement that settled it is quoted. Those earlier designs are not
in this repository, and [results/README.md](../results/README.md) says which
records measured them. Control-plane numbers are from `cmd/remex-loadtest`
(64 synthetic workers, 20 s runs, `results/control-plane-*.jsonl`); bisection
numbers are from `cmd/remex-bisectsim`.

## One leader with the ledger in memory

**Decision.** A single scheduler process holds the whole ledger (executions,
attempts, an append-only event log) in memory behind one lock, with indexes
for every lookup an RPC needs, and persists changes to PostgreSQL. It holds a
PostgreSQL advisory lock to be the only writer.

**Alternatives.** A stateless scheduler that queries PostgreSQL on every RPC;
a replicated state machine (Raft) across several schedulers.

**Why.** Every decision a lease, heartbeat or Complete makes reads current
state, and reading it from memory keeps those RPCs at microseconds. A single
writer needs no conflict handling: there is never a serialization failure to
retry. Raft would remove the single point of failure, at the cost of a
consensus round per change and a far larger correctness argument.

**Cost.** One machine's memory bounds the ledger, and a crash is an outage
until a supervisor restarts the process; recovery reloads the ledger and
gives in-flight leases a grace period.

## Per-platform run queues

**Decision.** Keep runnable executions in a min-heap of ledger positions for
each distinct platform. A Lease looks at the head of every queue whose
platform the worker satisfies and takes the oldest; entries whose execution
is no longer runnable are dropped when they reach a head.

**Alternatives.** Sort every runnable execution on each Lease and take the
first match (what the scheduler did before); one global queue scanned until
a match.

**Why.** Lease runs under the scheduler lock, so its cost is paid by every
other RPC. Sorting made it O(n log n) in the queue length: with 50,000
actions queued the load test completed a median of 26 actions/s and the
probe's heartbeat waited up to 3.3 s. With the heaps the same cell runs at a
median of 25,684 actions/s, as fast as with an empty queue. A global queue
would still scan past work for other pools, such as GPU actions waiting
while CPU workers poll.

**Cost.** A Lease is linear in the number of distinct platforms, which is
assumed to be small. Ledger positions are creation order, so a retried
execution keeps its place; that depends on loading executions in creation
order after a restart.

## Group commit

**Decision.** A mutation applies its change to the in-memory ledger and
appends it to a commit log, then releases the lock. One writer saves
everything appended while its previous write was in flight as a single
PostgreSQL transaction. A call answers only once the log is durable up to the
position it observed, its own changes included.

**Alternatives.** One fsync'd transaction per mutation under the lock (an
earlier design); `synchronous_commit = off` or a write-behind cache that
answers before the write.

**Why.** An action makes about six commits (created, leased, running,
uploading, committed, published). Made one at a time under the lock, they
capped the PostgreSQL ledger at 104–124 actions/s across ten runs
(`results/control-plane-per-mutation*.jsonl` and the `per-mutation` rows of
`results/control-plane-group-commit.jsonl`) and put an fsync in front of
every heartbeat (median 59–74 ms). Grouped, 27.5 changes share one
transaction on average, and the ledger reaches 1,000–1,700 actions/s in the
runs made alternately with those (1,173–1,603 in the ten current runs of
`results/control-plane.jsonl`), with heartbeats under 0.1 ms at the median
and 3–10 ms at worst, while nothing a
client sees can be lost in a crash, which an asynchronous commit cannot
promise. Waiting for the observed position, not only for one's own change,
matters: a stream must not report a completion that is applied in memory but
not yet durable.

**What may skip the wait.** Only answers that no lost change could
contradict. The store always holds a prefix of memory, so a heartbeat that
changes nothing and gets CONTINUE saw no fencing change in memory and there
is none in the store either; an empty lease answer tells the worker nothing.
Everything else waits, including FENCED decisions and the audit metadata
Complete sends to the cache.

**Cost.** Memory runs ahead of the store, so a failed write cannot be undone
in place: any failed write stops the scheduler, and a restart serves the
durable prefix. With one commit per mutation, a write that failed without
losing the connection could fail just its own call and leave the ledger
untouched. In PostgreSQL every write failure already means a lost connection
or a fault the scheduler cannot reason about, so the difference shows only
under injected failures.

## Leases with epochs, fenced on the scheduler

**Decision.** Every attempt is a `(worker, epoch)` lease with a scheduler-side
deadline renewed by heartbeats. A heartbeat or Complete is accepted only at
the execution's current epoch, from the leasing worker, before the deadline;
expiry bumps the epoch. A worker measures its own lease from the start of its
last acknowledged heartbeat.

**Alternatives.** Trust workers to report their own failures; fence with
wall-clock lease timestamps shared between machines.

**Why.** A worker can be partitioned, paused or killed at any point, and only
the scheduler can decide that an attempt is dead. Because the worker starts
its clock before the scheduler extends the deadline, it gives up no later
than the scheduler expires it, as long as the two clocks run at the same
rate: an expired attempt is not still running on its worker when the retry
starts.

**Cost.** A transient partition longer than the lease costs a retry of the
whole action.

## Workers ride out a scheduler restart

**Decision.** A worker keeps its attempt while the scheduler is unreachable
and gives it up only when the scheduler says so (FENCED, CANCEL) or its own
lease runs out. Until then heartbeats, phase changes and Complete are
retried, every RPC to the scheduler waits for the channel to reconnect
within its deadline, and reconnects back off at most 1 s. A finished
attempt's last Complete may still be waiting when the lease runs out: only
running work has to stop with the lease, and the scheduler decides whether
the result still counts. The restarted scheduler gives every recovered
lease a grace period.

**Alternatives.** Give up after the first failed RPC, or after a few
immediate retries, and let fencing retry the action (what an earlier version
of the worker did); hold attempts indefinitely and reconcile them after the
outage.

**Why.** Fencing already makes late and repeated calls harmless: a replay
of the committed winner is ACCEPTED without changing it, and a fenced
attempt stays FENCED. Retrying therefore costs nothing in safety, and the
worker's own lease bounds how long it holds on, so it never runs an attempt
the scheduler has already given away. Waiting for the channel matters with
gRPC: an RPC on a channel in TRANSIENT_FAILURE fails at once, and with no
RPC waiting on the connection, gRPC 1.51 advances a background reconnect
only from its 5 s backup poller. A worker could miss a scheduler that was
back within a second. In the Bazel restart test, with the scheduler killed
while the action uploaded, the earlier worker re-ran the action, 8.9–13.8 s
from kill to result over five runs; this worker's attempt commits 2.1–3.9 s
after the kill (median 2.2 s; `results/scheduler-restart.jsonl`).

**Cost.** An outage longer than the lease still costs a retry of the whole
action. While the scheduler is down, each worker reconnects about once a
second, and a worker told to stop can take one RPC deadline (at most 5 s)
to notice.

## Complete is accepted only for a finished attempt

**Decision.** Complete is accepted only if the worker reports the attempt's
cgroup empty, the attempt directory was never used before, the scheduler saw
the command start (a RUNNING heartbeat), and every output blob is present in
the CAS; the CAS walk runs outside the lock and the attempt is fenced again
afterwards.

**Alternatives.** Accept the worker's word for the whole result; walk the CAS
under the lock.

**Why.** An empty cgroup means no process can still write outputs; the walk
means a result is cached only after every blob it points at was found.
Walking outside the lock keeps one slow CAS from stalling every other RPC,
and fencing again catches a lease that expired during the walk. The cgroup
state and OOM count are still the worker's report: the scheduler cannot see
the worker's host, so it trusts them, while it checks the epoch, the attempt
directory and the CAS itself.

**Cost.** Every completion costs a walk of its output trees and a
`FindMissingBlobs` call.

## Commit, then publish

**Decision.** A result is made durable in the ledger (`Committed`) before it is
written to the action cache (`Published`), and publication runs off the lock,
one publisher per execution.

**Alternatives.** Publish first; a two-phase commit across the ledger and
bazel-remote.

**Why.** A crash between the two steps leaves a committed result that recovery
re-verifies and publishes, so the cache never holds a result the ledger does
not. bazel-remote has no transactions to take part in a two-phase commit.
Publishing off the lock matters when the cache is slow: an earlier design
that called the cache while holding the lock completed 0.3 actions/s with
every action-cache write stalled for 3 s, and 54 attempts lost their lease
while they waited (`results/control-plane-global-lock.jsonl`, memory and
PostgreSQL stores alike). Off the lock, the same load loses none.

**Cost.** A result can be committed and not yet cached for as long as the
cache is down; Tick retries the same result and never re-executes it.

## Flaky-failure bisection with an exact stopping rule

**Decision.** Calibrate on the known-bad revision until it has failed 10 times,
take a one-sided Clopper–Pearson lower bound on its failure rate, run each
probe often enough that a bad revision passes with probability at most 0.05,
and confirm the boundary with fresh runs; report RANGE or INCONCLUSIVE rather
than guess.

**Alternatives.** `git bisect run` with one run per revision; a fixed number
of reruns per probe; a fixed-size calibration of 20 runs (an earlier
version).

**Why.** The rate of a flaky failure is not known in advance, and a fixed
rerun count has to be chosen for the rarest one expected. On simulated
1,000-commit histories with 10% infrastructure noise, `git bisect run` with
N reruns per revision is right 99% of the time only once N fits the true
rate: N = 10 at p = 0.5, but N = 100 at p = 0.1, which then costs 561 runs on
a deterministic failure. Calibrating sizes each probe for the failure at
hand: remex is right 94–100% of the time from p = 0.1 to 1, gave no wrong
answer in any of those cells, and costs 50 runs on a deterministic failure.
Sampling until the r-th failure keeps the bound exact, because the chance of
the r-th failure within n runs is P(Binomial(n, p) ≥ r); the fixed 20-run
calibration was 58% correct at p = 0.1
(`results/bisect-simulation-fixed-calibration.jsonl`).

**Cost.** A tuned rerun count beats it: with N picked for the true p, git
bisect is right 99.3–100% of the time for fewer runs than remex at every p
tested, though it is occasionally wrong (up to 0.7%). remex instead ends
inconclusive 4–6% of the time for flaky failures, when the final
confirmation rejects a boundary that a passing bad probe produced. Runs: a
median of 725 at p = 0.1 with a p95 of 1,180, inside the CLI's default
budget of 2,000.

## bazel-remote unmodified for CAS and action cache

**Decision.** Use the stock `bazel-remote` for blobs and the action cache; the
scheduler never stores or forwards blob bytes.

**Alternatives.** A CAS built into the scheduler.

**Why.** Blob traffic scales with builds, not with scheduling decisions, and
keeping it out of the scheduler keeps the single leader small. bazel-remote
is what Bazel users already run.

**Cost.** The worker's CAS client issues one request per blob, so a cold
remote build pays a round trip for every input and output blob. Batching
reads is the next step there; no measurement of that overhead is included
in `results/`.
