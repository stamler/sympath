#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
dist_dir="${DIST_DIR:-$repo_root/dist}"
build_dir="${BUILD_DIR:-$dist_dir/build}"
cache_dir="${GOCACHE:-$repo_root/.gocache}"
version="${VERSION:-dev}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

checksum_file() {
  local path="$1"
  local hash
  if command -v sha256sum >/dev/null 2>&1; then
    hash="$(sha256sum "$path" | awk '{print $1}')"
    printf '%s  %s\n' "$hash" "$(basename "$path")"
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    hash="$(shasum -a 256 "$path" | awk '{print $1}')"
    printf '%s  %s\n' "$hash" "$(basename "$path")"
    return
  fi
  echo "Missing required command: sha256sum or shasum" >&2
  exit 1
}

package_posix() {
  local input="$1"
  local output="$2"
  local staging="$3"

  mkdir -p "$staging"
  cp "$build_dir/$input" "$staging/sympath"
  chmod 0755 "$staging/sympath"
  COPYFILE_DISABLE=1 tar -czf "$dist_dir/$output" -C "$staging" sympath
}

package_windows() {
  local input="$1"
  local output="$2"
  local staging="$3"

  mkdir -p "$staging"
  cp "$build_dir/$input" "$staging/sympath.exe"
  (cd "$staging" && zip -q "$dist_dir/$output" sympath.exe)
}

require_cmd tar
require_cmd zip

rm -rf "$build_dir"
mkdir -p "$dist_dir" "$build_dir" "$cache_dir"

OUT_DIR="$build_dir" GOCACHE="$cache_dir" VERSION="$version" "$repo_root/scripts/build-cross.sh"

rm -f \
  "$dist_dir/checksums.txt" \
  "$dist_dir/sympath-darwin-arm64.tar.gz" \
  "$dist_dir/sympath-linux-amd64.tar.gz" \
  "$dist_dir/sympath-windows-amd64.zip"

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/sympath-package.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

package_posix "sympath-darwin-arm64" "sympath-darwin-arm64.tar.gz" "$tmp_dir/darwin-arm64"
package_posix "sympath-linux-amd64" "sympath-linux-amd64.tar.gz" "$tmp_dir/linux-amd64"
package_windows "sympath-windows-amd64.exe" "sympath-windows-amd64.zip" "$tmp_dir/windows-amd64"

{
  checksum_file "$dist_dir/sympath-darwin-arm64.tar.gz"
  checksum_file "$dist_dir/sympath-linux-amd64.tar.gz"
  checksum_file "$dist_dir/sympath-windows-amd64.zip"
} >"$dist_dir/checksums.txt"

echo "Release artifacts written to $dist_dir"
