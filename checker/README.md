# Independent evidence checker

This directory is a separate Go module with no dependency on scheduler code.
It imports official REAPI protobufs and gRPC. The custom worker envelope is decoded
from its wire contract without importing its implementation.

```
cd checker
go test ./...
go run ./cmd/remex-check --ledger ../evidence/ledger.json \
  --evidence ../evidence/cache-normalized.jsonl,../evidence/worker-normalized.jsonl \
  --regression ../evidence/bisect.json
```

Exit status is nonzero for either a proven violation or missing independent
evidence. The JSON separates `violations` from `missing_evidence` and reports the
number of checks per invariant. An empty input never establishes correctness.
Invariants without relevant actions/probes have no coverage; an OK subset report
is not evidence that an entire fault matrix or all six invariants were exercised.

The ledger schema is `schema_version: 1`, with `executions`, `attempts` and
append-only `events`. See `model.go` for the independent JSON definitions. Events
include `lease_granted`, `lease_expired`, `stale_complete_rejected`, `cas_verified`,
`attempt_committed`, and `ac_published`. Execution winners, attempt epochs, event
order, cache eligibility and worker requirements are checked independently.

The supplied `remex-record` transparent gRPC proxy saves original request/response
protobufs and terminal RPC status from the cache and worker service in one ordered
capture. `remex-adapt` normalizes that capture without taking a final ledger as input.
It supports Lease, Complete, FindMissingBlobs, UpdateActionResult, complete identity
ByteStream reads/writes and identity BatchReadBlobs. Thus output Tree bytes are
available even on warm cache reads. Unsupported compressed or partial transfers
remain missing content evidence. Never create external records by copying ledger
claims into an alleged independent log.
The adapter requires one capture ID with contiguous sequence numbers starting at
one, one start/end per RPC, and a stable method and endpoint for each call ID.
Missing starts, sequence gaps, duplicate terminal records and merged RPC identities
cause a nonzero exit. Retain the full original capture; filtering arbitrary rows
before adaptation cannot establish complete evidence.

Each row contains `source`, `kind`, `sequence`, `execution_id`, `attempt_id`, `epoch`:

| Source / kind | Additional fields | Evidence requirement |
| --- | --- | --- |
| `cas` / `cas_verified` | `digests`: `[{"hash":"...","size_bytes":123}]` | Successful external verification covers every result blob before commit |
| `ac` / `ac_write` | `instance_name`, `action_digest`, raw `result` | Actual cache key and result match the fenced winner |
| `worker` / `lease_verified` | `worker_platform`, `device_verified`, `action_digest`, raw `action`, `command` | Captured fresh properties satisfy the digest-verified Action/Command |
| `worker` / `cgroup_empty` | `attempt_dir`, `empty: true`, `oom_kill` | Actual kernel state observed before forwarding Complete |
| `worker` / `result_reported` | `attempt_dir`, raw `result`, numeric `status_code`, `complete_decision`, `trees` | Actual worker report and scheduler decision |
| `capture` / `evidence_gap` | `detail` | Incomplete capture prevents an OK report |
| `ac` / `ac_write_attempt` | key, instance and raw result | Failed mutating RPC has an unknown outcome; prevents OK |

Raw protobuf fields are JSON base64. The checker independently derives cache
policy, platform and all result references from Action, Command, ActionResult and
recursively parsed Trees; omitted derived ledger references cannot hide a blob.
Legitimate empty results are allowed, absent raw result evidence is not. Cache
instance and exact published content are checked. Independent OOM observations
override a falsely successful ledger. FENCED/REJECTED results need not be stored;
a durable snapshot distinguishes post-commit duplicate rejection from committing
after fencing. `cpu.min` and `mem_gb.min` are numeric bounds; `remex-hw` is policy.

A fully captured Complete request whose RPC ends with a transport error is kept
with decision `UNKNOWN`; the adapter never invents an ACCEPTED response. Its raw
result and independently observed empty cgroup can still establish what the
worker submitted before a scheduler crash. A later durable commit must match that
raw content and have the same independent CAS/publication ordering proofs. An
uncommitted unknown submission need not be copied into the ledger, but still
requires cgroup-before-report evidence. A missing terminal capture record remains
a gap; unknown AC mutation outcomes still prevent an OK report.

Build and run the recorder separately from the unmodified cache:

```sh
cd checker
go build -o ../build/bin/remex-record ./cmd/remex-record
go build -o ../build/bin/remex-adapt ./cmd/remex-adapt
cd ..
build/bin/remex-record \
  --cache-listen 127.0.0.1:19093 --cache-target 127.0.0.1:19092 \
  --worker-listen 127.0.0.1:18981 --worker-target 127.0.0.1:19980 \
  --ledger-url http://127.0.0.1:19081/audit/durable-export \
  --cgroup-root /sys/fs/cgroup/ACTUAL_DELEGATED_ROOT \
  --out evidence/new-raw-capture.jsonl
build/bin/remex-adapt --capture evidence/new-raw-capture.jsonl > evidence/normalized.jsonl
```

The output file must not exist. Scheduler/worker CAS traffic must use 19093 and
workers' scheduler traffic must use 18981. This initial recorder uses plaintext
gRPC on loopback for the trusted single-host deployment. Stop clients/workers
before the recorder so RPCs complete. Capturing incurs I/O and observation overhead.

Scheduler outgoing metadata keys `x-remex-execution-id`, `x-remex-attempt-id`,
`x-remex-epoch`, `x-remex-ledger-sequence` provide correlation, not ordering proof.
Before forwarding a CAS verification response the recorder reads durable state
showing no commit; before forwarding an AC write it reads durable state showing
the committed winner. `/audit/durable-export` loads persisted state without the
scheduler execution mutex, avoiding deadlock during publication. Captures retain
both original snapshots. The checker compares them with the final ledger and
keeps `order_domain` capture sequences separate from ledger sequences.

The Complete observer reads `resource_usage_json.cgroup_path/cgroup.events` and
`memory.events`, constrained to the delegated root, before forwarding to the
scheduler. Remote workers need a host-local observer and causal transport protocol;
wall clocks alone do not establish cross-host ordering. Native capture evidence
contains `raw_record_ids` and `ledger_snapshot`. Manual normalized rows without
an order domain require an independently established causal sequence; label unit
fixtures synthetic.

Regression reports are checked for the three allowed conclusions, budget and run
accounting, matching-signature test evidence, and full `2k` confirmation of both
sides for a culprit. Independent replay of raw probe evidence is still necessary
to establish the truth of a supplied observation.

The unit fixtures are explicitly synthetic. They check that each invariant detects
its corresponding mutation and that scheduler self-report alone cannot pass. They
are not real external fault-injection evidence. The buffered gRPC integration test
checks actual transparent transport, but real multi-process captures and hardware
fault runs must still be retained separately.
