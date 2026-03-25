#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

out_dir="${OUT_DIR:-$repo_root/dist}"
cache_dir="${GOCACHE:-$repo_root/.gocache}"
pkg="${PKG:-./cmd/sympath}"

build() {
  local goos="$1"
  local goarch="$2"
  local output="$3"

  echo "Building $output"
  GOCACHE="$cache_dir" GOOS="$goos" GOARCH="$goarch" go build -o "$out_dir/$output" "$pkg"
}

mkdir -p "$out_dir" "$cache_dir"

build darwin arm64 sympath-darwin-arm64
build linux amd64 sympath-linux-amd64
build windows amd64 sympath-windows-amd64.exe

echo "Artifacts written to $out_dir"
