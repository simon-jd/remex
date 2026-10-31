# C++20 worker

The worker speaks the project worker gRPC protocol and the official REAPI v2 / ByteStream CAS protocols. It never writes ActionCache. A delegated cgroup v2 subtree with `cgroup.kill`, CPU and memory controllers is mandatory; failed cleanup prevents `Complete`.

## Build

From the repository root:

```sh
bazel build //worker:remex-worker
bazel test //worker:worker-tests //worker:remote-tests //worker:polling-tests
```

`MODULE.bazel` pins gRPC C++ 1.84.0. Vendored protobuf schemas are unmodified remote-apis v2.12.0 and the Google APIs revision in `third_party/README.md`. Generated code is build output, not committed source.

For a local distribution-toolchain build (requires CMake, C++20, protobuf compiler/development headers, gRPC compiler/development headers, OpenSSL and Abseil development headers):

```sh
cmake -S worker -B build/worker -DCMAKE_BUILD_TYPE=Release
cmake --build build/worker -j 4
ctest --test-dir build/worker --output-on-failure
```

CMake supports project-local Ubuntu development packages unpacked under `.deps/worker-prefix/usr` via `REMEX_DEPS_PREFIX`. It intentionally uses `/usr/bin/protoc` to match the distribution protobuf runtime rather than an unrelated compiler on `PATH`. The development environment uses gRPC 1.51.1/protobuf 3.21.12; this does not establish the pinned Bazel 1.84.0 build's verification status.

## Run

Create a **project-only** child under an already delegated user/service cgroup subtree, and enable `cpu memory pids` for that child's descendants. Do not give the worker the entire host cgroup hierarchy. The worker itself remains outside its per-attempt cgroups.

```sh
build/worker/remex-worker \
  --id cpu-1 --scheduler localhost:8980 \
  --root /var/tmp/remex/cpu-1 \
  --cgroup-root /path/to/delegated/remex \
  --property pool=cpu --property image=sha256:YOUR_TOOLCHAIN_IMAGE
```

`--cas` optionally overrides the CAS endpoint in the lease. Both `grpc://` and `grpcs://` endpoints are accepted; TLS uses system trust. The initial intended deployment is one trusted team, without client authentication or network namespace isolation. All workers and Bazel clients must use the same toolchain image and image property. Absolute executable paths refer to that image. Environment variables come only from `Command`; host secrets and `LD_LIBRARY_PATH` are not inherited by actions.

`--probe` prints actual CPU/memory properties and GPU identity without registration. `--check-cgroup --cgroup-root PATH` verifies real cgroup filesystem identity, kill support, populated=0 and counters. `--once` exits after one received attempt, useful for controlled tests. Separate workers must use separate `--root` directories and worker IDs.

For GPU, use `--property pool=gpu`. NVML is loaded dynamically. Exactly one visible NVML device is supported. Its UUID, CUDA compute capability and driver version are reread before each lease and compared again before command execution. Optional declared `gpu.cc` / `gpu.driver_major` must agree with the hardware; mismatches refuse registration/lease. CPU workers cannot advertise GPU properties. Snapshots are included in Complete and ActionResult auxiliary metadata. The process keeps one resident NVML library/session and serializes access; this avoids the eventfd leak observed with repeated load/unload cycles on NVIDIA 595.91. A failed query resets initialization, and the next probe reinitializes before reading fresh identity. Library handles are retained, while device handles and snapshots are reread. In service mode, transient NVML loading, initialization, enumeration or identity-query failures pause acquisition and log exponential retries from 500 ms up to 5 s. Every retry probes again; no previously successful device snapshot is reused. `--probe` and `--once` surface these failures immediately. Unsupported multi-GPU configurations, mismatched declared properties and other permanent configuration errors remain explicit failures.

Initial registration and registration after a scheduler restart also retry transient gRPC errors with the same bounded delay. Authentication, authorization and invalid-request errors fail explicitly. Successful registration is followed by a new device probe before leasing. SIGTERM/SIGINT interrupts backoff promptly. Retries continue until recovery or shutdown; the 5 s bound limits the delay, not the retry count.

## Correctness and limits

- SHA256 and byte size are checked for every CAS read, including cached reads. Corrupt cached content is discarded and refetched. Uploads query FindMissingBlobs and use chunked ByteStream writes.
- Input trees reject duplicate names, absolute paths, `..` in node paths, NULs, symlink traversal, escaping symlinks and excessive depth. After materialization, every symlink graph is resolved component by component, expanding links before processing target `..`; legitimate relative runfiles links remain supported. Output links undergo the same graph validation. Blobs are copied into a newly generated UUID attempt directory; writable hard links never share cache contents.
- Command timeout starts only when the gated child has joined its cgroup and a RUNNING heartbeat is acknowledged. Both child stdout and stderr are captured separately. Command environment and working directory follow REAPI, and outputs are relative to the working directory.
- Each attempt has an independent guardian outside the action cgroup. A CLOEXEC liveness pipe detects worker termination, including SIGKILL; the guardian uses pre-opened cgroup controls to kill and confirm emptiness. Normal cleanup reaps the guardian; after abrupt worker death the empty cgroup remains available as evidence. This protects worker death, not simultaneous termination of the entire supervisor process tree.
- A CANCEL or FENCED decision, or expiry of the worker's own monotonic lease, stops execution. Heartbeats and phase changes that fail in transport are retried until that lease runs out, measured from the start of the last acknowledged heartbeat. The worker kills the entire attempt cgroup, including children that created their own process groups, and waits for `populated=0` before reporting. Successful commands also undergo the same cleanup.
- A CLOEXEC startup-error pipe distinguishes failed `execve`/`chdir` from a program that actually returns 126 or 127; only the latter is an action exit code. Resource JSON includes the actual `cgroup_path`, allowing an independent recorder to read kernel counters before Complete returns.
- Execution metadata records observed worker, input-fetch, execution and output-upload start/completion times. Execution completion is recorded immediately after process/cgroup cleanup, before output upload; worker completion is recorded before Complete. The original durable queue timestamp comes from the scheduler's Lease response and is retained across retry epochs. Unreached worker phases remain unset.
- Output files and directories use REAPI ActionResult/Tree encoding. Stdout/stderr are uploaded as blobs. Only the scheduler can fence, commit and publish the result.
- Host capacity advertisements can be lowered with `cpu.min`/`mem_gb.min`; these are scheduling capabilities, not per-action resource limits. OOM counters are recorded, but this version does not impose memory.max automatically.
- In-memory transfer is bounded to 1 GiB per blob. Local cache and attempt directories are retained without eviction; operators must monitor free disk and preserve needed attempt evidence. The trusted-team scope is not a hostile-command security sandbox.

## Tests

Core tests cover digest corruption, size mismatches, cache corruption recovery, path/symlink traversal, duplicate Directory nodes, executable bits, output Trees and resource matching. The gRPC fixture tests real client streaming, instance names, multi-chunk blobs, corruption and lease loss; it is not a replacement for a bazel-remote integration test. Polling tests exercise the production loop with injected failures to verify fresh-snapshot gating, capped backoff, initial registration recovery, scheduler re-registration and fail-fast modes; these fixtures are not GPU hardware evidence.

The cgroup integration test is skipped unless explicitly supplied an existing delegated project subtree:

```sh
REMEX_TEST_CGROUP_ROOT=/path/to/delegated/remex \
  ctest --test-dir build/worker --output-on-failure
```

It runs real commands and independently checks timeout, cancellation and successful-command descendant cleanup, including a `set -m` process group escape. It never emulates kernel cgroup files or substitutes fake GPU identities.

The opt-in `nvml-tests` executable (Bazel `//worker:nvml-tests`) requires `REMEX_TEST_NVML=1` and one real visible GPU with matching userspace libraries. It performs 600 fresh identity queries, then 100 deliberate shutdowns of only its own NVML client session, requiring a failed query followed by successful reinitialization. It checks `/proc/self/fd` remains bounded through both paths; no driver or system service is changed. Without opt-in it reports SKIP; it never substitutes a simulated GPU.
