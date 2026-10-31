#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
exec "$root/.tools/bazel.official" --output_user_root="${REMEX_BAZEL_USER_ROOT:-/tmp/remex-bazel-local}" "$@"
