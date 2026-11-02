# remex scheduler

The service implements the official REAPI `Capabilities.GetCapabilities`,
`Execution.Execute`, and `Execution.WaitExecution` services, plus the remex worker
protocol. It advertises SHA-256 and REAPI 2.0–2.11, using generated upstream v2.12.0
messages. There is no cache server in this component: CAS and ActionCache are
accessed through a separate bazel-remote gRPC endpoint.

```sh
GOFLAGS=-buildvcs=false go run ./scheduler/cmd/remex-scheduler \
  --listen :8980 --admin 127.0.0.1:8081 --cas 127.0.0.1:9092 \
  --postgres 'postgres://remex:password@127.0.0.1:5432/remex?sslmode=disable'
```

`--memory` is an explicit non-durable development alternative. PostgreSQL obtains
a session advisory lock, so a second scheduler is rejected. The implementation
is deliberately single leader: changed JSONB execution/attempt rows and new
append-only events are committed in one batched transaction, grouping every
change queued while the previous write was in flight. Recovery reads those
normalized tables. Client RPC cancellation does not cancel durable
transactions; a separate 30-second transaction deadline bounds commits. Any
database write failure makes the scheduler fail closed and the CLI exit, so
restart reacquires leadership and reloads durable state. Throughput under
load is measured by `cmd/remex-loadtest`; see the top-level README.

Workers register before execution submissions. `Lease` refreshes measured
properties before assignment; unfulfillable requests complete with
FAILED_PRECONDITION and an explanation. CPU/memory minima are numeric; other
requirements compare exactly. `remex-hw=true` is a cache-policy marker. Bazel
`no-remote-cache` translates to `Action.do_not_cache`, which is also enforced.
The deployment is for a single trusted team; transport and admin endpoints must
be protected by the deployment network. Worker assertions are not an untrusted
hardware attestation service.

Lease deadlines are in-process monotonic `time.Time` values, kept in a min-heap
with a per-worker index (`deadlines.go`): every RPC first expires what is due,
visiting only the due attempts, and Lease finds the caller's own attempt
without looking at other workers'. Persisted wall time is audit data only. On restart active attempts receive `--recovery-grace` before
expiry; a running command's remaining server-side timeout is conservatively the
same grace. The worker remains responsible for its own monotonic command timer.
A worker's first RUNNING heartbeat starts that timer; materialization is excluded.
UPLOADING ends command timing and preserves an already observed timeout.
At the command deadline the worker's own steady timer kills the cgroup; the
scheduler keeps the reporting lease alive. RUNNING beyond one extra lease
interval is cancelled. Complete normalizes expired commands to DEADLINE_EXCEEDED,
including timeouts during CAS validation, so they cannot be published as success.
Kernel oom_kill evidence similarly forces ResourceExhausted and blocks AC.
A lease expiry retries with epoch+1 up to
`--max-attempts`. Disconnected Execute streams do not cancel executions.

Complete verifies current epoch/worker/lease, requires a fresh attempt directory
and an empty cgroup, walks output Tree file references, calls FindMissingBlobs,
then rechecks fencing and durably commits exactly one winner. It writes AC only
after the commit and only for status OK, exit code 0, cacheable actions. Pending
writes are retried after restart, with a fresh CAS presence check. An AC write
whose final ledger update fails may be repeated with the identical winner and
result (idempotent publication). This is not a CAS pinning protocol: eviction
between checking and publishing remains possible; bazel-remote dependency checks
on cache reads provide the downstream guard.

Admin HTTP:

- `GET /healthz`: process liveness.
- `GET /export` (alias `/ledger`): versioned complete ledger JSON. Execution
  `invocation_ids` come from Bazel REAPI RequestMetadata and include all joined
  invocations when a cacheable in-flight action is deduplicated.
- `GET /audit/durable-export`: read the durable store directly, without taking
  the scheduler mutex. A synchronous external CAS/AC observer can use this while
  publication is awaiting its RPC response; the response contains only the
  persisted event prefix, not scheduler assertions about uncommitted work.
- `GET /explain?id=executions/<id>` or `/v1/executions/<id>`: execution/reason.
- `POST /cancel?id=executions/<id>`: operator cancellation; the next heartbeat
  tells the worker to kill its cgroup.

```sh
GOFLAGS=-buildvcs=false go test -race ./scheduler/...
REMEX_TEST_POSTGRES='postgres://.../remex?sslmode=disable' \
REMEX_TEST_CAS=127.0.0.1:9092 GOFLAGS=-buildvcs=false \
  go test -v ./scheduler -run 'Integration$'
```

Integration tests use an isolated temporary PostgreSQL schema. They still need
exclusive scheduler ownership during the PostgreSQL test. Unit tests use a real
gRPC server over `bufconn` and cover replay, CAS-before-commit-before-AC ordering,
expiry during CAS verification, retry epochs, recovery, policy, output Trees,
timeouts, cancellation and persistence failure. Real hardware/cgroup behavior,
Bazel version compatibility and a full fault matrix need deployment evidence.

`OutputDirectory.path=""` is accepted as the complete working directory, as
specified by REAPI. Empty file and symlink paths remain invalid. Raw protobuf
fixtures for this boundary are in `scheduler/testdata/`.

Ledger snapshots (`/export` and the in-memory store) use explicit deep
copies, including independent byte buffers, maps and event details. To
compare the explicit copy with a JSON round trip on a synthetic
600-execution ledger:

```sh
GOFLAGS=-buildvcs=false go test ./scheduler -run '^$' \
  -bench BenchmarkLedgerClone600Executions -benchtime=3x -benchmem
```

This microbenchmark measures snapshot cost only. Mutations do not take
snapshots: each copies only the rows it writes, and the PostgreSQL store
saves exactly those rows. `cmd/remex-loadtest -history N` measures how
throughput changes as the ledger grows.
