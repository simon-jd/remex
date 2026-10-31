# Worker protocol

`worker.proto` is the source of truth. `Action`, `Command` and `ActionResult`
byte fields contain the official REAPI v2 protobuf wire format, not JSON. The
worker can generate C++ stubs while consuming upstream REAPI C++ definitions.

A worker ID owns at most one active lease. Register before polling. Send fresh
measured platform properties on every Lease call; GPU workers include their NVML
snapshot and `gpu.cc`/`gpu.driver_major`. A registered worker's liveness window
is refreshed by polling and heartbeats.

Lease `queued_timestamp` is the original durable execution enqueue time, including
for retries and recovery. The worker copies it into the raw ActionResult metadata.
It is not a per-attempt queue duration. When an older scheduler omits the field,
the worker leaves it absent instead of substituting an artificial timestamp.

Keep heartbeating during materialization, command execution and upload. Report
RUNNING immediately when the command starts. Timeout runs from this notification.
Retry a heartbeat that fails in transport until the worker's own lease, measured
from the start of its last acknowledged heartbeat, runs out. After a CANCEL or
FENCED response, or once that lease has run out, kill the cgroup and wait until
populated=0. Only report Complete after cgroup cleanup; include a unique
attempt_dir and resource statistics. An empty serialized ActionResult is valid
for a successful command without outputs. A failed Complete RPC may be retried;
a committed winner gets the same ACCEPTED response without another publication.

Go generated files use protoc-gen-go v1.36.11 and protoc-gen-go-grpc v1.6.1:

```sh
protoc -I . --go_out=. --go_opt=module=github.com/simon-jd/remex \
  --go-grpc_out=. --go-grpc_opt=module=github.com/simon-jd/remex \
  api/worker/v1/worker.proto
```
