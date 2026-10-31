#!/usr/bin/env bash
# Render user units, or run/check a worker in a pre-delegated cgroup v2 subtree.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
fail() { printf 'remex-install-worker: %s\n' "$*" >&2; exit 1; }
usage() {
  cat <<'EOF'
Usage:
  scripts/install-worker.sh render-units [OUTPUT_DIRECTORY]
  scripts/install-worker.sh run WORKER_ID cpu|rtl|gpu
  scripts/install-worker.sh check

Variables:
  REMEX_WORKER_BINARY    Native worker (default build/worker/remex-worker)
  REMEX_CGROUP_TEST      Integration test (default build/worker/cgroup-tests)
  REMEX_CGROUP_ROOT      Existing project-only delegated subtree; automatically
                       detected and split when run by a generated systemd unit
  REMEX_SCHEDULER        Default 127.0.0.1:18980
  REMEX_CAS              Default 127.0.0.1:19092
  REMEX_WORKER_DATA      Default build/installed-workers
  REMEX_GPU              Set 1 to render an additional GPU unit
  REMEX_NVML_LIBRARY_DIR Optional matched NVML library directory for native worker

Rendering writes only OUTPUT_DIRECTORY and does not enable or start services.
Native workers do not advertise a container image identity.
EOF
}
binary=${REMEX_WORKER_BINARY:-$repo/build/worker/remex-worker}
cgtest=${REMEX_CGROUP_TEST:-$repo/build/worker/cgroup-tests}
data=${REMEX_WORKER_DATA:-$repo/build/installed-workers}
mode=${1:---help}
case "$mode" in
  --help|-h) usage; exit 0 ;;
  render-units)
    dest=${2:-$repo/build/worker-install}
    [[ $# -le 2 ]] || fail "too many arguments"
    [[ -x "$binary" && -x "$cgtest" ]] || fail "build the worker and cgroup-tests first (make worker-build)"
    # systemd unit escaping differs from shell quoting. Restrict generated paths
    # instead of embedding untrusted/newline-bearing values in ExecStart.
    for path in "$repo" "$binary" "$cgtest" "$data" "${REMEX_NVML_LIBRARY_DIR:-/}"; do
      [[ "$path" =~ ^/[a-zA-Z0-9_./:-]*$ ]] || fail "generated unit paths must be absolute and contain no spaces or systemd specifiers: $path"
    done
    scheduler=${REMEX_SCHEDULER:-127.0.0.1:18980}
    cas=${REMEX_CAS:-127.0.0.1:19092}
    [[ "$scheduler" =~ ^[a-zA-Z0-9_.:/-]+$ && "$cas" =~ ^[a-zA-Z0-9_.:/-]+$ ]] || fail "invalid endpoint"
    mkdir -p "$dest"
    workers=(cpu-1 cpu-2 rtl-1)
    if [[ ${REMEX_GPU:-0} == 1 ]]; then workers+=(gpu-1); fi
    for id in "${workers[@]}"; do
      pool=${id%%-*}
      cat > "$dest/remex-worker-$id.service" <<EOF
[Unit]
Description=remex $pool worker $id (trusted team)
After=network-online.target

[Service]
Type=simple
Delegate=cpu memory pids
Environment=REMEX_WORKER_BINARY=$binary
Environment=REMEX_CGROUP_TEST=$cgtest
Environment=REMEX_WORKER_DATA=$data
Environment=REMEX_SCHEDULER=$scheduler
Environment=REMEX_CAS=$cas
Environment=REMEX_NVML_LIBRARY_DIR=${REMEX_NVML_LIBRARY_DIR:-}
ExecStart=$repo/scripts/install-worker.sh run $id $pool
Restart=on-failure
RestartSec=3
TimeoutStopSec=30
KillMode=control-group
NoNewPrivileges=yes

[Install]
WantedBy=default.target
EOF
    done
    printf 'Rendered %s units in %s; they are not enabled or started.\n' "${#workers[@]}" "$dest"
    ;;
  run|check)
    [[ -x "$binary" && -x "$cgtest" ]] || fail "worker binary and cgroup-tests are required"
    if [[ "$mode" == run ]]; then
      [[ $# == 3 ]] || fail "run requires WORKER_ID and pool"
      id=$2; pool=$3
      [[ "$id" =~ ^[a-zA-Z0-9_-]+$ ]] || fail "invalid worker ID"
      [[ "$pool" == cpu || "$pool" == rtl || "$pool" == gpu ]] || fail "pool must be cpu, rtl, or gpu"
    else
      [[ $# == 1 ]] || fail "check accepts no arguments"
    fi
    cgroup=${REMEX_CGROUP_ROOT:-}
    if [[ -z "$cgroup" ]]; then
      [[ "$mode" == run ]] || fail "check requires REMEX_CGROUP_ROOT"
      IFS=: read -r hierarchy controllers relative < /proc/self/cgroup
      [[ "$hierarchy" == 0 && -z "$controllers" && "$relative" == */remex-worker-"$id".service ]] || fail "set REMEX_CGROUP_ROOT or run in the generated delegated systemd service"
      service_root=/sys/fs/cgroup$relative
      [[ -w "$service_root/cgroup.procs" ]] || fail "service cgroup is not delegated"
      mkdir -p "$service_root/supervisor" "$service_root/attempts"
      # Keep the supervisor outside all action groups and leave the delegation
      # root empty before enabling domain controllers (cgroup v2 rule).
      printf '%s\n' "$$" > "$service_root/supervisor/cgroup.procs"
      printf '%s\n' '+cpu +memory +pids' > "$service_root/cgroup.subtree_control"
      cgroup=$service_root/attempts
    fi
    cgroup=$(realpath -e "$cgroup")
    [[ "$cgroup" == /sys/fs/cgroup/*/remex-* || "$cgroup" == /sys/fs/cgroup/remex-* ]] || fail "cgroup must be an explicitly named remex-* project subtree"
    [[ $(stat -fc %T "$cgroup") == cgroup2fs ]] || fail "cgroup root is not a real cgroup v2 filesystem"
    [[ -w "$cgroup/cgroup.subtree_control" ]] || fail "cgroup controller delegation is not writable"
    [[ ! -s "$cgroup/cgroup.procs" ]] || fail "cgroup.procs must be empty"
    # cgroup pseudo-files have size zero even when populated; read them too.
    [[ -z $(cat "$cgroup/cgroup.procs") ]] || fail "cgroup root contains processes"
    for controller in cpu memory pids; do
      grep -qw "$controller" "$cgroup/cgroup.controllers" || fail "controller $controller is not delegated"
    done
    printf '%s\n' '+cpu +memory +pids' > "$cgroup/cgroup.subtree_control"
    # Unlike --check-cgroup, this actually attaches a process and tests timeout,
    # cancellation, process-group escape and descendant cleanup.
    REMEX_TEST_CGROUP_ROOT="$cgroup" "$cgtest"
    if [[ "$mode" == check ]]; then exit 0; fi
    mkdir -p "$data/$id"
    if [[ -n ${REMEX_NVML_LIBRARY_DIR:-} ]]; then
      [[ -d "$REMEX_NVML_LIBRARY_DIR" ]] || fail "NVML directory does not exist"
      export LD_LIBRARY_PATH="$REMEX_NVML_LIBRARY_DIR${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    fi
    exec "$binary" --id "$id" --scheduler "${REMEX_SCHEDULER:-127.0.0.1:18980}" \
      --cas "${REMEX_CAS:-127.0.0.1:19092}" --root "$data/$id" --cgroup-root "$cgroup" --property "pool=$pool"
    ;;
  *) usage >&2; fail "unknown command: $mode" ;;
esac
