#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
sample=${1:-cpp-history}
case "$sample" in cpp-history|cuda|rtl);; *) echo "Usage: $0 [cpp-history|cuda|rtl] [extra Bazel flags...]" >&2; exit 2;; esac
if (($#)); then shift; fi
bazel=${REMEX_BAZEL:-$root/.tools/bazel.official}
[[ -x "$bazel" ]] || bazel=$(command -v bazel || command -v bazelisk)
mkdir -p "$root/evidence/runs"
evidence=$(mktemp -d "$root/evidence/runs/$sample-XXXXXX")
cd "$root/samples/$sample"
flags=(--remote_executor="grpc://127.0.0.1:${REMEX_EXEC_PORT:-18980}" --remote_cache="grpc://${REMEX_CACHE_ENDPOINT:-127.0.0.1:${REMEX_CAS_PORT:-19092}}" --remote_instance_name="${REMEX_INSTANCE:-remex}" --noremote_local_fallback --spawn_strategy=remote --strategy=TestRunner=remote --jobs=4 --test_output=errors --remote_grpc_log="$evidence/grpc.bin" --execution_log_compact_file="$evidence/execution.zstd" --build_event_json_file="$evidence/bep.json")
if [[ -n ${REMEX_IMAGE:-} ]]; then flags+=(--remote_default_exec_properties="image=$REMEX_IMAGE"); fi
if [[ "$sample" == cuda || "$sample" == rtl ]]; then flags+=(--nocache_test_results); fi
set +e
"$bazel" --output_user_root="$root/.cache/bazel" test //... "${flags[@]}" "$@" 2>&1 | tee "$evidence/bazel.log"
result=${PIPESTATUS[0]}
set -e
printf '%s\n' "$result" > "$evidence/exit-code.txt"
curl -fsS "http://127.0.0.1:${REMEX_ADMIN_PORT:-18081}/export" > "$evidence/ledger.json" || true
docker compose -f "$root/deploy/compose.yaml" logs --no-color bazel-remote > "$evidence/cache.log" || true
echo "Evidence: $evidence"
exit "$result"
