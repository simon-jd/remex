#!/usr/bin/env bash
# Native workers stay in the current user's delegated cgroup hierarchy.
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"
uid=$(id -u)
parent=${REMEX_CGROUP_PARENT:-/sys/fs/cgroup/user.slice/user-$uid.slice/user@$uid.service}
[[ -d "$parent" && -w "$parent" ]] || { echo "Set REMEX_CGROUP_PARENT to a writable cgroup v2 subtree delegated to this user." >&2; exit 1; }
[[ -x build/bin/remex-scheduler && -x build/worker/remex-worker ]] || { echo "Run make all first." >&2; exit 1; }
docker compose -f deploy/compose.yaml up -d --wait postgres bazel-remote
run=$(mktemp -d "$root/build/local-run-XXXXXX")
cgroup="$parent/remex-$(basename "$run")"
mkdir "$cgroup"
printf '%s\n' '+cpu +memory +pids' > "$cgroup/cgroup.subtree_control"
pids=()
# shellcheck disable=SC2329 # Invoked by the EXIT trap below.
cleanup() {
  trap - EXIT INT TERM
  for pid in "${pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
  rmdir "$cgroup" 2>/dev/null || true
  echo "Run evidence retained at $run"
}
trap cleanup EXIT
trap 'exit 130' INT TERM
cache_endpoint=${REMEX_CACHE_ENDPOINT:-127.0.0.1:${REMEX_CAS_PORT:-19092}}
worker_endpoint=${REMEX_WORKER_ENDPOINT:-127.0.0.1:${REMEX_EXEC_PORT:-18980}}
postgres_dsn=${REMEX_POSTGRES:-postgres://remex:remex-local-only@127.0.0.1:${REMEX_POSTGRES_PORT:-15432}/remex?sslmode=disable}
build/bin/remex-scheduler --listen "127.0.0.1:${REMEX_EXEC_PORT:-18980}" --admin "127.0.0.1:${REMEX_ADMIN_PORT:-18081}" --cas "$cache_endpoint" --postgres "$postgres_dsn" >"$run/scheduler.log" 2>&1 &
pids+=("$!")
healthy=0
for ((i=0;i<60;i++)); do
  if curl -fsS "http://127.0.0.1:${REMEX_ADMIN_PORT:-18081}/healthz" >/dev/null; then healthy=1; break; fi
  kill -0 "${pids[0]}" 2>/dev/null || { cat "$run/scheduler.log" >&2; exit 1; }
  sleep 0.5
done
[[ "$healthy" == 1 ]] || { echo "Scheduler did not become healthy" >&2; exit 1; }
start_worker() {
  local id=$1 pool=$2
  local extra=()
  if [[ -n ${REMEX_IMAGE:-} ]]; then extra+=(--property "image=$REMEX_IMAGE"); fi
  build/worker/remex-worker --scheduler "$worker_endpoint" --cas "$cache_endpoint" --cgroup-root "$cgroup" --root "$run/$id" --id "$id-$(basename "$run")" --property "pool=$pool" "${extra[@]}" >"$run/$id.log" 2>&1 &
  pids+=("$!")
}
start_worker cpu-1 cpu
start_worker cpu-2 cpu
start_worker rtl-1 rtl
if [[ ${REMEX_GPU:-0} == 1 ]]; then start_worker gpu-1 gpu; fi
printf '%s\n' "remex execution grpc://127.0.0.1:${REMEX_EXEC_PORT:-18980}" "remex cache grpc://127.0.0.1:${REMEX_CAS_PORT:-19092}" "Logs: $run" "Ctrl-C stops only this run's scheduler and workers; compose data is retained."
wait -n "${pids[@]}"
echo "A remex process exited; stopping this run. Inspect $run" >&2
exit 1
