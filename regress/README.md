# Regression CLI

Build with `go build -o bin/remex ./cmd/remex` from the repository root. Exact Action
export uses the official REAPI and ByteStream protobuf/gRPC clients. Bundle export/replay additionally needs `zstd`;
replay needs Python 3 and the command's recorded toolchain on the receiving host.

```
remex classify --bep evidence/bep.json --ledger evidence/invocation-ledger.json
remex select --input selection.json
remex bisect --config bisect.json
remex explain --endpoint http://localhost:18081 --execution executions/ID
remex explain --ledger evidence/ledger.json --execution executions/ID
remex export --ledger evidence/ledger.json --execution executions/ID --cas localhost:19092 --out /absolute/new-bundle
remex export --inputs /absolute/materialized-inputs --manifest manifest.json --out /absolute/new-bundle
```

`classify` reads individual BEP test results, ignoring test summaries and cached
results. Supply only this invocation's attempts to `--ledger`: OOM and non-OK
infrastructure statuses take precedence over test failures. A nonzero process
exit code alone does not establish a test failure. `--signature` supplies an
explicit normalized failure signature. Otherwise signatures include the label,
status and SHA-256 of the failure log or BEP status details. A failure without
distinguishing text has no signature and cannot establish a culprit. For stable
domain-specific normalization, use the instrumented probe command described below.

Selection input:

```json
{"targets":[{"label":"//gpu:test","tags":["remex-hw","no-remote-cache"]}],"affected":["//gpu:test"],"changed_files":["kernel.cu"],"selector_error":"","platform_changed":false,"image_changed":false,"driver_changed":false}
```

Supply the affected labels from target-determinator or an equivalent dependency
analyzer. This command does not claim to compute a build graph. Analyzer failure,
global Bazel configuration, platform/toolchain/deploy changes, image/driver changes
and malformed hardware tags select every hardware test. Extra global path prefixes
can be supplied as `global_prefixes`.

Bisection input:

```json
{
  "good":"GOOD_COMMIT", "bad":"BAD_COMMIT",
  "bisect":{"alpha":0.05,"calibration_failures":10,"calibration_runs":200,"budget":1000,"seed":0},
  "runner":{"repository":"/absolute/repository","bazel":"bazel","targets":["//:test"],"flags":["--remote_executor=grpc://localhost:18980","--remote_cache=grpc://localhost:19092"],"scheduler_admin":"http://localhost:18081","evidence_directory":"/absolute/evidence/probes"}
}
```

The runner checks out a fresh detached Git worktree per probe and invokes Bazel
with test-result caching disabled, remote cache acceptance disabled, one run and
one flaky attempt. It stores BEP, remote gRPC, execution and console logs. Local
`test.log` references are copied and hashed for signatures. Remote `bytestream://`
logs are downloaded through the official ByteStream API, with SHA-256 and size
verification and a 16 MiB limit. Missing or invalid logs cannot supply failure
signatures. `scheduler_admin` records the ledger and filters classification by
the probe's explicit invocation UUID; unrelated concurrent failures are excluded.
Set `workspace_subdirectory` for a Bazel workspace beneath the Git repository root.
Use the same platform/toolchain flags for every revision. Worktrees are removed
after each probe; evidence remains. The repository's first-parent history must
contain good as an ancestor of bad. Explicit ordered `bisect.revisions` is also
supported for pre-resolved histories.

For an instrumented runner, set `runner.probe_command` to an argument array. The
CLI appends `REVISION SEED EVIDENCE_DIRECTORY`, runs it in `runner.repository`, and
expects one Observation JSON on stdout, for example
`{"category":"TEST_FAILURE","signature":"fifo-ordering","evidence":["path"]}`.
The command must actually execute that revision (for example via a worktree or
`git show`); it is responsible for collecting invocation-scoped ledger evidence.
Return zero for a valid observation. A command error becomes infrastructure noise.
Categories are PASS, TEST_FAILURE, INFRASTRUCTURE, TIMEOUT, CANCELLED, UNKNOWN.

The algorithm calibrates the known-bad point, computes an exact one-sided
Clopper–Pearson lower bound, and chooses `ceil(log(alpha)/log(1-p_lower))` trials.
The probability interpretation assumes independent trials and a stationary failure
rate; it is a per-probe bound, not a family-wise guarantee over a whole search.
Infrastructure runs consume budget but never statistical evidence. A different
failure signature skips a revision. Search requires a monotonic good-to-bad history.
Every culprit requires fresh, complete `2k` trials on both adjacent commits.
Budget exhaustion, absent reproduction or failed confirmation is INCONCLUSIVE;
unclassifiable interior revisions can yield RANGE. The JSON preserves every probe.

`export --ledger ... --execution ... --cas ...` fetches and verifies the original
Action, Command, every Directory and every file against their SHA-256 and byte
size. It reconstructs only the Action's initial `input_root_digest`, including
executable bits and safe relative symlinks, in a fresh directory. It never reads
the post-execution attempt directory. The bundle's `source` records original
Action/Command wire bytes, their digests, the input root digest, and every
Directory's wire bytes and path. This preserves the Merkle provenance as well as
the actual inputs. A missing/corrupt CAS blob, inconsistent ledger protobuf or
platform, or absent winning worker provenance fails the export.

For this mode, optional `--manifest context.json` supplies `bazel_version`,
`bazel_flags`, and `revision`; `--revision COMMIT` can set the revision explicitly.
The execution's original Command fixes arguments, environment and working
directory; its Action fixes the platform, and its winning attempt supplies worker
and GPU data. Any manifest value that conflicts with those facts is rejected.
Exact Action replay uses only the recorded environment, without inheriting host
variables or injecting a replacement seed.
The CAS endpoint currently supports local insecure gRPC (`host:port` or `grpc://`).
This command requires the original blobs still to be retained by CAS. Existing
`--inputs` export remains available for caller-supplied trees, but that mode alone
does not prove that a tree was the original Action input.

A bundle replays with only the bundle mounted: no network and no scheduler,
database or CAS access. For example, in the toolchain image named by the
recorded platform's `image` value:

```sh
docker run --rm --network none --read-only \
  --tmpfs /tmp:rw,exec,nosuid,size=256m --cap-drop ALL \
  --security-opt no-new-privileges \
  --mount type=bind,source=/path/to/bundle,target=/bundle,readonly \
  --entrypoint /bin/bash \
  TOOLCHAIN_IMAGE \
  /bundle/run.sh
```

Replay requires the original toolchain/runtime ABI (and hardware for GPU actions).

Bundle metadata accepts `revision`, `bazel_version`, `bazel_flags`, `command`
(argument array), `working_directory`, `environment`, `platform`, `worker`, `gpu`
and `seed`. Export adds schema version and a SHA-256/size/executable manifest.
The bundle contains input bytes, never just CAS references. Export preserves
relative symlinks after resolving the complete input graph one component at a time.
It refuses escaping, cyclic, dangling or absolute links, special files,
input-relative output directories and existing destinations. The
standalone `run.sh` checks archive paths, member types, sizes and hashes before
executing the command in a new temporary directory. It verifies the link graph
again and creates links only after safely extracting ordinary files. Hardlink
archive entries remain forbidden. The bundle does not package a container image,
host drivers, licensed software or the interpreter itself.

Tests exercise real temporary Git histories, actual compressed bundle replay after
deleting source inputs, archive tampering/path traversal, signature changes,
calibration, budget/infra handling, skip ranges and failed confirmation. They do
not replace real Bazel/GPU validation or the planted-corpus evaluation.
