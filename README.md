# sympath

`sympath` inventories directory trees into a consolidated SQLite database and can merge snapshots from multiple machines through a single local `~/.sympath` workspace.

## Install

### macOS (arm64) and Linux (amd64)

```sh
curl -fsSL https://raw.githubusercontent.com/stamler/sympath/main/install.sh | sh
```

### Windows (amd64)

```powershell
irm https://raw.githubusercontent.com/stamler/sympath/main/install.ps1 | iex
```

Re-running either installer upgrades `sympath` to the latest GitHub release when a newer version is available. If the current release is already installed, the installer leaves the binary in place and only repairs the PATH setup if needed.

Tagged release builds also cache the latest known GitHub release under `~/.sympath/update-check.json`. Successful `scan`, `ui`, and `version` commands may print a brief stderr-only notice when a newer release is already known, and `sympath update-check` forces a live refresh on demand.

Managed installs can also upgrade themselves with `sympath update`. In v1, that command only updates the standard per-user install locations (or `SYMPATH_INSTALL_DIR` when explicitly set). If `sympath` is being run from an ad-hoc copied binary, it fails with guidance to reinstall via the documented installer.

## Supported Targets

- macOS arm64
- Linux amd64
- Windows amd64

Intel macOS, Linux arm64, and other targets currently return a clear unsupported-platform error from the installer.

## Install Locations

- macOS and Linux: `~/.local/bin/sympath`
- Windows: `%LOCALAPPDATA%\Programs\sympath\bin\sympath.exe`

Both installers are per-account only. They do not attempt a system-wide install.

If the POSIX installer is run as `root`, it still installs into `/root/.local/bin` by default, updates only the `root` account's shell profile, and keeps all `sympath` data under `/root/.sympath`.

## Installer Overrides

Both installers accept the same public environment overrides:

- `SYMPATH_INSTALL_REPO`: alternate GitHub repo slug instead of `stamler/sympath`
- `SYMPATH_INSTALL_VERSION`: install a specific release tag instead of the latest release
- `SYMPATH_INSTALL_DIR`: alternate per-user install directory

Examples:

```sh
SYMPATH_INSTALL_VERSION=v1.2.3 curl -fsSL https://raw.githubusercontent.com/stamler/sympath/main/install.sh | sh
```

```powershell
$env:SYMPATH_INSTALL_DIR = "$HOME\bin"; irm https://raw.githubusercontent.com/stamler/sympath/main/install.ps1 | iex
```

## Releases

Tagged releases publish these assets:

- `sympath-darwin-arm64.tar.gz`
- `sympath-linux-amd64.tar.gz`
- `sympath-windows-amd64.zip`
- `checksums.txt`

Each archive contains a single executable at the archive root.

To build those artifacts locally:

```sh
scripts/package-release.sh
```

Set `VERSION=vX.Y.Z` to embed a specific release version in the binary.

## Usage

```sh
sympath scan /path/to/root
sympath ui
sympath version
sympath update
sympath update-check
```

Running `sympath` without a subcommand behaves like `sympath scan`.

`sympath version` keeps printing only the build version on stdout. When an update is known to be available, the automatic notice is written to stderr so scripts that read stdout stay stable.

`sympath update` performs the actual managed in-place upgrade for supported per-user installs. `sympath update-check` remains the read-only command for checking release status without changing the installation.

Interactive scans show a live progress line on `stderr` with a spinner,
a bouncing shaded block track, and running file counts while the inventory is
being built.

## Remotes

The installer does not create or populate `~/.sympath/remotes`. Remote aggregation stays manual.

- macOS and Linux edit `~/.sympath/remotes`
- Windows edits `%USERPROFILE%\.sympath\remotes`
- only the designated aggregator machine should populate that file

Each non-comment line in `remotes` is a single SSH target such as `mac-mini`, `dean@fileserver`, or `root@fileserver`.

Remote fetches run as the SSH login account for that target:

- `root@host` reads `/root/.sympath/*.sympath`
- `user@host` reads that user's `~/.sympath/*.sympath`

If root SSH login is disabled on the remote machine, `root@host` will not work until the remote host configuration allows it.
