#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
mkdir -p "$root/.tools"
path="$root/.tools/bazel.official"
expected=d302d22ed77ee5658b87857249baa579db84300ecebe0a1e9a357b5b20f80eb8
if [[ ! -f "$path" ]] || [[ $(sha256sum "$path" | cut -d' ' -f1) != "$expected" ]]; then
  curl -fL --retry 3 https://releases.bazel.build/9.3.0/release/bazel-9.3.0-linux-x86_64 -o "$path.download"
  printf '%s  %s\n' "$expected" "$path.download" | sha256sum -c -
  mv "$path.download" "$path"
fi
chmod +x "$path"
"$path" --version
