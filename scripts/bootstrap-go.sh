#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
version=1.27.2
checksum=ecbadb99091a3f46e31f5f934b068b1864eafa7995211b39eaddf76996045fe5
out="$root/.tools/go$version"
if [[ -x "$out/bin/go" ]]; then "$out/bin/go" version; exit 0; fi
mkdir -p "$root/.tools"
archive="$root/.tools/go$version.linux-amd64.tar.gz"
curl -fL --retry 3 "https://dl.google.com/go/go$version.linux-amd64.tar.gz" -o "$archive"
printf '%s  %s\n' "$checksum" "$archive" | sha256sum -c -
staging=$(mktemp -d "$root/.tools/go-unpack-XXXXXX")
trap 'rm -rf "$staging"' EXIT
tar -xzf "$archive" -C "$staging"
mv "$staging/go" "$out"
"$out/bin/go" version
