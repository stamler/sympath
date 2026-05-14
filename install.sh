#!/bin/sh
set -eu

DEFAULT_REPO="stamler/sympath"
PROFILE_BEGIN="# >>> sympath install >>>"
PROFILE_END="# <<< sympath install <<<"

log() {
  printf '%s\n' "$*"
}

warn() {
  printf 'Warning: %s\n' "$*" >&2
}

die() {
  printf 'Error: %s\n' "$*" >&2
  exit 1
}

die_extracted_binary_not_runnable() {
  if [ -e "$extracted_bin" ]; then
    die "downloaded archive contained sympath, but it is not executable after extraction.
If this is Synology DSM or another system with a restricted temporary directory, retry with:
  mkdir -p \"\$HOME/.cache\"
  curl -fsSL https://raw.githubusercontent.com/${SYMPATH_INSTALL_REPO:-$DEFAULT_REPO}/main/install.sh | TMPDIR=\"\$HOME/.cache\" sh"
  fi

  die "downloaded archive did not contain a runnable sympath binary"
}

die_downloaded_binary_version_failed() {
  die "read downloaded binary version: $downloaded_version_error
If this is Synology DSM or another system with a restricted temporary directory, retry with:
  mkdir -p \"\$HOME/.cache\"
  curl -fsSL https://raw.githubusercontent.com/${SYMPATH_INSTALL_REPO:-$DEFAULT_REPO}/main/install.sh | TMPDIR=\"\$HOME/.cache\" sh"
}

choose_temp_parent() {
  case "${TMPDIR:-}" in
    ""|/tmp|/tmp/*)
      printf '%s\n' "$HOME/.cache"
      ;;
    *)
      printf '%s\n' "$TMPDIR"
      ;;
  esac
}

cleanup_temp() {
  if [ -n "${tmp_dir:-}" ]; then
    rm -rf "$tmp_dir"
  fi
  if [ "${tmp_parent_created:-0}" = "1" ]; then
    rmdir "$tmp_parent" 2>/dev/null || true
  fi
}

current_euid() {
  if [ -n "${SYMPATH_INSTALL_TEST_EUID:-}" ]; then
    printf '%s\n' "$SYMPATH_INSTALL_TEST_EUID"
    return
  fi
  id -u
}

download_to() {
  source_path="$1"
  dest="$2"

  if [ -n "${SYMPATH_INSTALL_BASE_DIR:-}" ]; then
    cp "$source_path" "$dest"
    return
  fi

  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$source_path" -o "$dest"
    return
  fi
  if command -v wget >/dev/null 2>&1; then
    wget -qO "$dest" "$source_path"
    return
  fi

  die "curl or wget is required to download release assets"
}

compute_sha256() {
  path="$1"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$path" | awk '{print $1}'
    return
  fi
  if command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$path" | awk '{print $1}'
    return
  fi

  die "sha256sum or shasum is required to verify downloads"
}

# SYMPATH_INSTALL_BASE_URL and SYMPATH_INSTALL_BASE_DIR are internal
# test hooks. Public install overrides are documented in README.md.
release_asset_source() {
  asset="$1"

  if [ -n "${SYMPATH_INSTALL_BASE_DIR:-}" ]; then
    printf '%s/%s\n' "$SYMPATH_INSTALL_BASE_DIR" "$asset"
    return
  fi

  if [ -n "${SYMPATH_INSTALL_BASE_URL:-}" ]; then
    printf '%s/%s\n' "$SYMPATH_INSTALL_BASE_URL" "$asset"
    return
  fi

  repo_slug="${SYMPATH_INSTALL_REPO:-$DEFAULT_REPO}"
  if [ -n "${SYMPATH_INSTALL_VERSION:-}" ]; then
    printf 'https://github.com/%s/releases/download/%s/%s\n' "$repo_slug" "$SYMPATH_INSTALL_VERSION" "$asset"
    return
  fi

  printf 'https://github.com/%s/releases/latest/download/%s\n' "$repo_slug" "$asset"
}

detect_asset() {
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os:$arch" in
    Darwin:arm64)
      printf '%s\n' "sympath-darwin-arm64.tar.gz"
      ;;
    Linux:x86_64 | Linux:amd64)
      printf '%s\n' "sympath-linux-amd64.tar.gz"
      ;;
    Darwin:*)
      die "unsupported platform $os/$arch; the installer currently supports macOS arm64 and Linux amd64"
      ;;
    Linux:*)
      die "unsupported platform $os/$arch; the installer currently supports Linux amd64 only"
      ;;
    *)
      die "unsupported platform $os/$arch"
      ;;
  esac
}

choose_profile() {
  shell_name="${SHELL##*/}"

  case "$shell_name" in
    zsh)
      printf '%s\n' "$HOME/.zshrc"
      ;;
    bash)
      if [ -f "$HOME/.bash_profile" ] || [ ! -f "$HOME/.bashrc" ]; then
        printf '%s\n' "$HOME/.bash_profile"
      else
        printf '%s\n' "$HOME/.bashrc"
      fi
      ;;
    *)
      printf '%s\n' "$HOME/.profile"
      ;;
  esac
}

update_profile() {
  profile="$1"
  install_dir="$2"
  escaped_dir="$(printf '%s' "$install_dir" | sed 's/[\"\\$`]/\\&/g')"
  path_line="export PATH=\"$escaped_dir:\$PATH\""
  tmp_file="${profile}.tmp.$$"

  if [ -f "$profile" ]; then
    awk -v begin="$PROFILE_BEGIN" -v end="$PROFILE_END" '
      $0 == begin { skip = 1; next }
      $0 == end { skip = 0; next }
      !skip { print }
    ' "$profile" >"$tmp_file"
  else
    : >"$tmp_file"
  fi

  if [ -s "$tmp_file" ]; then
    printf '\n' >>"$tmp_file"
  fi

  {
    printf '%s\n' "$PROFILE_BEGIN"
    printf '%s\n' "$path_line"
    printf '%s\n' "$PROFILE_END"
  } >>"$tmp_file"

  mv "$tmp_file" "$profile"
}

main() {
  asset="$(detect_asset)"
  archive_source="$(release_asset_source "$asset")"
  checksums_source="$(release_asset_source checksums.txt)"
  install_dir="${SYMPATH_INSTALL_DIR:-$HOME/.local/bin}"
  target_path="$install_dir/sympath"
  profile="$(choose_profile)"
  tmp_dir=""
  tmp_parent="$(choose_temp_parent)"
  tmp_parent_created=0

  trap 'cleanup_temp' EXIT INT TERM HUP

  if [ ! -d "$tmp_parent" ]; then
    mkdir -p "$tmp_parent"
    tmp_parent_created=1
  fi

  tmp_dir="$(mktemp -d "$tmp_parent/sympath-install.XXXXXX")"
  archive_path="$tmp_dir/$asset"
  checksums_path="$tmp_dir/checksums.txt"
  extracted_dir="$tmp_dir/extracted"
  extracted_bin="$extracted_dir/sympath"

  if [ "$(current_euid)" = "0" ]; then
    warn "running as root installs sympath only for the root account at $install_dir; it does not create a system-wide install"
  fi

  mkdir -p "$install_dir" "$extracted_dir"

  log "Downloading $asset"
  download_to "$archive_source" "$archive_path"
  download_to "$checksums_source" "$checksums_path"

  expected_checksum="$(awk -v asset="$asset" '$2 == asset { print $1; exit }' "$checksums_path")"
  [ -n "$expected_checksum" ] || die "no checksum found for $asset"

  actual_checksum="$(compute_sha256 "$archive_path")"
  [ "$actual_checksum" = "$expected_checksum" ] || die "checksum verification failed for $asset"

  tar -xzf "$archive_path" -C "$extracted_dir"
  [ -x "$extracted_bin" ] || die_extracted_binary_not_runnable

  downloaded_version_error_path="$tmp_dir/version.stderr"
  if ! downloaded_version="$(SYMPATH_INTERNAL_NO_UPDATE_NOTICE=1 "$extracted_bin" version 2>"$downloaded_version_error_path")"; then
    downloaded_version_error="$(cat "$downloaded_version_error_path" 2>/dev/null || true)"
    die_downloaded_binary_version_failed
  fi
  installed_version=""
  if [ -x "$target_path" ]; then
    installed_version="$("$target_path" version 2>/dev/null || true)"
  fi

  if [ -n "$installed_version" ] && [ "$installed_version" = "$downloaded_version" ]; then
    log "sympath $installed_version is already installed at $target_path"
  else
    cp "$extracted_bin" "$target_path"
    chmod 0755 "$target_path"
    log "Installed sympath $downloaded_version to $target_path"
  fi

  update_profile "$profile" "$install_dir"
  log "Ensured PATH entry in $profile"

  resolved_path="$(command -v sympath 2>/dev/null || true)"
  if [ "$resolved_path" = "$target_path" ]; then
    log "sympath is available now: $resolved_path"
    return
  fi

  log "Open a new shell or run:"
  printf '  export PATH="%s:$PATH"\n' "$install_dir"
}

main "$@"
