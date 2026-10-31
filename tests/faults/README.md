# Fault tests

These tests treat the scheduler and worker as black boxes. They call only the
public REAPI and worker gRPC services or the scheduler's exported store
interface, and they never modify scheduler or worker private state.

`go test -race ./tests/...` runs deterministic protocol and failure-boundary
tests with a separate gRPC CAS/AC test server. This is synthetic fault injection,
not evidence of a real disk failure or a bazel-remote eviction.

The actual C++ worker tests are opt-in:

```sh
REMEX_TEST_WORKER_BINARY="$PWD/build/worker/remex-worker" \
REMEX_TEST_CGROUP_ROOT=/path/to/delegated/project/cgroup \
REMEX_TEST_CAS=127.0.0.1:19092 \
go test -race -count=1 -v ./tests/faults -run '^TestReal'
```

The root must be an already delegated cgroup v2 subtree with CPU, memory and PID
controllers enabled. Each test creates its own child subtree and a scheduler on
an ephemeral loopback port. Tests never connect to the deployment PostgreSQL
leader. The OOM case limits only its test subtree to 64 MiB with swap disabled.
SIGKILL and SIGSTOP target only the worker launched by the test. Cleanup kills
only the dedicated test subtree, after the correctness assertion, so cleanup
cannot make a residual-process assertion pass.

The real cases distinguish successful execution, command timeout (including a
`set -m` child), kernel OOM, missing executable, actual exit 127, worker SIGKILL,
and lease expiry while the worker is stopped and subsequently resumed.
The metadata case checks actual observed phase times. The transient-NVML case
launches the actual worker service but injects a small synthetic NVML shared
library; its UUIDs are deliberate fixtures, not hardware evidence.

An unavailable real environment is reported as SKIP. To record an actual run,
set all three variables and use `-count=1`; do not describe a skipped case as
passed hardware or cgroup validation.

The independent checker JSON/CLI cases are synthetic algorithm fixtures. They
require `/usr/bin/protoc` and use the checked-in upstream REAPI source to encode
original Action/ActionResult/Tree protobufs:

```sh
(cd checker && go build -o ../build/bin/remex-checker ./cmd/remex-check)
REMEX_CHECKER="$PWD/build/bin/remex-checker" python3 tests/protocol/checker_boundary_test.py
python3 tests/protocol/checker_capture_boundary_test.py
```

The real Bazel/PostgreSQL process recovery test creates and drops only a unique
test database; it needs `psql`, cached Bazel dependencies, the built scheduler and
worker, a real CAS, and a delegated cgroup. Set normal Go cache environment
variables if the environment uses a separate writable cache:

```sh
python3 tests/faults/bazel_restart_test.py \
  --cgroup-root /path/to/delegated/project/cgroup \
  --artifacts evidence/runs/my-restart-run \
  --fault scheduler-running
```

Other supported faults are `worker-materializing`, `worker-uploading`,
`worker-running`, `worker-stopped`, `scheduler-queued`, `scheduler-leased`,
`scheduler-uploading`, `scheduler-before-ac` and `scheduler-after-ac`.
Use `--capture` to retain complete original worker/cache RPCs, adapt and check.
Build `remex-test-leasegate` below before queued/leased/stopped scenarios.
The default isolated scheduler
ports are 21980/21081; select other unused ports with `--grpc-port`/`--admin-port`.
Recorder/control gates reserve `grpc-port + 10`, `+11`, `+12`; AC boundary modes
also reserve `grpc-port + 100` for a test-only forwarding
proxy. They pause a real request, SIGKILL only their own scheduler, restart with
the same database, and check original Bazel WaitExecution logs and durable
winner/publication state. Output directories must be new. Arguments support
custom scheduler/worker/Bazel/CAS paths and PostgreSQL test credentials.

Actual disk-full tests must target a disposable, isolated **32 MiB tmpfs**
bazel-remote with uncompressed storage and AC instance key mangling enabled.
Never point this command at the shared CAS:

```sh
REMEX_TEST_ISOLATED_SMALL_CAS=127.0.0.1:29092 \
go test -race -count=1 -v ./tests/faults -run '^TestIsolatedCacheUploadAndDiskFull$'
```

The four cases check wrong digests in both upload APIs, instance isolation and
an observed ENOSPC. The test requires the cache to be healthy first and verifies
failed upload digests remain absent.

The raw-capture scenarios create their own original 1 GiB
bazel-remote container and run actual Bazel with an isolated PostgreSQL database.
Build only the test forwarding helpers, then run one scenario at a time:

```sh
go build -o build/bin/remex-test-leasegate ./tests/faults/leasegate
go build -o build/bin/remex-test-uploadgate ./tests/faults/uploadgate
python3 tests/faults/actual_cas_fault_test.py \
  --cgroup-root /path/to/delegated/project/cgroup \
  --fault cas-corruption --artifacts evidence/runs/my-corruption-run
```

Modes are `cas-corruption`, `cas-eviction`, and `worker-before-complete`. Defaults
reserve ports 21980–21985, 39092 and 38080. Override `--base-port`, `--cas-port`
and `--http-port` for parallel isolated runs. The lease gate keeps a real worker
registered while preventing it from acquiring work before the mutation; it
forwards actual leases normally after release. The upload gate withholds only
the response to a successful final stderr write, so no Complete has been sent.
The LRU test uploads approximately 1 GiB of filler only to its own cache.

The tests retain the unchanged recorder capture, adapt it, run the independent
checker, and additionally decode original protocol frames to prove the injected
fault occurred. To rerun just the read-only original-frame assertions:

```sh
python3 tests/faults/verify_capture_fault.py \
  --fault cas-corruption --artifacts evidence/runs/my-corruption-run
```

Corruption recovery deliberately removes only the planted bad cache blob and
restarts the test cache before Bazel re-uploads it. This is not automatic healing.
Both failed and recovered executions may have durable terminal commits; only the
successful result can reach AC. A zero-attempt Unschedulable response cannot pass
the content-fault assertions.

For precise actual REAPI Action timeout/cancel/OOM/set-m scenarios, build the
independent public-protocol client and run a named case:

```sh
go build -o build/bin/remex-test-actionprobe ./tests/faults/actionprobe
python3 tests/faults/worker_fault_capture.py \
  --cgroup-root /path/to/delegated/project/cgroup \
  --fault timeout-setm --artifacts evidence/runs/my-timeout-capture
```

Supported cases: `oom`, `timeout-setm`, `success-setm`, `cancel-before-deadline`,
`cancel-at-deadline`, `cancel-after-deadline`. Defaults reserve 23980–23983;
`--base-port` changes them. These use the compiled C++ worker, original shared
CAS under a unique namespace, and a unique PostgreSQL DB; they are explicitly
standalone REAPI invocations rather than Bazel builds. The OOM limit applies
only to the new delegated test subtree. Every case saves original protobufs,
raw capture, normalized evidence, checker output and kernel observations.

