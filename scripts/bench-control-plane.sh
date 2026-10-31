#!/usr/bin/env bash
# Control-plane load test matrix for one build of cmd/remex-loadtest.
#   scripts/bench-control-plane.sh BINARY LABEL > results.jsonl
# Every cell runs REMEX_BENCH_RUNS times (default 5), one pass over the
# matrix at a time, so slow drift on the host spreads across all cells.
# REMEX_BENCH_QUEUES lists the -queue depths measured with the in-memory store.
# Each record carries the repository commit (suffixed -dirty if tracked
# files outside results/ differ from it) and the host environment.
# PostgreSQL runs in a throwaway container (never the deployment's volume)
# that is reset before every PostgreSQL run.
set -euo pipefail
bin=$1 label=$2
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
runs=${REMEX_BENCH_RUNS:-5}
queues=${REMEX_BENCH_QUEUES:-1000 10000 50000}
duration=${REMEX_BENCH_DURATION:-20s}
image=postgres:18-alpine
name=remex-bench-postgres port=${REMEX_BENCH_PG_PORT:-25432}
dsn="postgres://remex:bench@127.0.0.1:$port/remex?sslmode=disable"
commit=$(git -C "$root" rev-parse HEAD)
git -C "$root" diff --quiet HEAD -- . ':!results' || commit+=-dirty
storage=$(df --output=source "$(docker info -f '{{.DockerRootDir}}')" | tail -1)
disk=$(lsblk -dno MODEL "/dev/$(lsblk -no PKNAME "$storage" | head -1)" 2>/dev/null || true)
if ! docker inspect "$name" >/dev/null 2>&1; then
  docker run -d --rm --name "$name" -e POSTGRES_USER=remex -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=remex \
    -p "127.0.0.1:$port:5432" "$image" >/dev/null
  until docker exec "$name" pg_isready -U remex -d remex >/dev/null 2>&1; do sleep 1; done
fi
reset() { docker exec -e PGOPTIONS=--client-min-messages=warning "$name" psql -q -U remex -d remex -c 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;' >/dev/null; }
run() {
  "$bin" -label "$label" -duration "$duration" -commit "$commit" \
    -env postgres_image="$image" -env docker_storage="$storage" -env docker_disk="$disk" "$@"
}

for i in $(seq "$runs"); do
  for history in 0 1000 5000; do run -run "$i" -history "$history"; done
  run -run "$i" -history 1000 -stall 3s
  for history in 0 1000; do reset; run -run "$i" -history "$history" -postgres "$dsn"; done
  reset; run -run "$i" -history 1000 -stall 3s -postgres "$dsn"
  for queue in $queues; do run -run "$i" -queue "$queue"; done
done
