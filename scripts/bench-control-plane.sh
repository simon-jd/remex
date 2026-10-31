#!/usr/bin/env bash
# Control-plane load test matrix for one build of cmd/remex-loadtest.
#   scripts/bench-control-plane.sh BINARY LABEL > results.jsonl
# Uses a throwaway PostgreSQL container (never the deployment's volume) that
# is reset before every PostgreSQL run.
set -euo pipefail
bin=$1 label=$2
duration=${REMEX_BENCH_DURATION:-20s}
name=remex-bench-postgres port=${REMEX_BENCH_PG_PORT:-25432}
dsn="postgres://remex:bench@127.0.0.1:$port/remex?sslmode=disable"
if ! docker inspect "$name" >/dev/null 2>&1; then
  docker run -d --rm --name "$name" -e POSTGRES_USER=remex -e POSTGRES_PASSWORD=bench -e POSTGRES_DB=remex \
    -p "127.0.0.1:$port:5432" postgres:18-alpine >/dev/null
  until docker exec "$name" pg_isready -U remex -d remex >/dev/null 2>&1; do sleep 1; done
fi
reset() { docker exec "$name" psql -q -U remex -d remex -c 'DROP SCHEMA public CASCADE; CREATE SCHEMA public;' >/dev/null; }
run() { "$bin" -label "$label" -duration "$duration" "$@"; }

for history in 0 1000 5000; do run -history "$history"; done
run -history 1000 -stall 3s
for history in 0 1000; do reset; run -history "$history" -postgres "$dsn"; done
reset; run -history 1000 -stall 3s -postgres "$dsn"
