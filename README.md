# sympath

`sympath` inventories directory trees into a consolidated SQLite database and can merge snapshots from multiple machines through a single local `~/.sympath` workspace.

## Install

### macOS (arm64) and Linux (amd64)

```sh
curl -fsSL https://raw.githubusercontent.com/stamler/sympath/main/install.sh | sh
```

On Synology DSM, or on other systems where `/tmp` is restricted, the POSIX
installer may fail with `downloaded archive did not contain a runnable sympath
binary`. In that case, use a home-backed temporary directory:

```sh
mkdir -p "$HOME/.cache"
curl -fsSL https://raw.githubusercontent.com/stamler/sympath/main/install.sh | TMPDIR="$HOME/.cache" sh
```

After installing, either open a new shell or update the current shell's `PATH`:

```sh
export PATH="$HOME/.local/bin:$PATH"
sympath version
```

### Windows (amd64)

```powershell
irm https://raw.githubusercontent.com/stamler/sympath/main/install.ps1 | iex
```

Re-running either installer upgrades `sympath` to the latest GitHub release when a newer version is available. If the current release is already installed, the installer leaves the binary in place and only repairs the PATH setup if needed.

On Synology DSM, use the same home-backed temporary directory when updating:

```sh
TMPDIR="$HOME/.cache" sympath update
```

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
sympath import-s3-checksum-report /path/to/manifest.json
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

`sympath ui` launches a local compare interface for two inventoried roots. It
can compare by path or by content, optionally ignore common OS metadata files,
and, in path mode, collapses fully missing folder trees into compact rows like
`path/to/folder/* (N files)` by default. That compact view can be toggled off
in the UI.

## S3

`sympath import-s3-checksum-report` imports a locally downloaded Amazon S3
Batch Operations Compute checksum completion-report bundle. Pass the report's
completion `manifest.json`; `sympath` finds the referenced local CSV files,
stores all report rows, and publishes `s3://bucket` roots using only
full-object SHA-256 rows for content comparison.

To create the checksum report in S3, first set account-specific values:

```sh
export AWS_ACCOUNT_ID=123456789012
export AWS_REGION=ca-central-1
export S3_BUCKET=example-archive
export S3_INVENTORY_PREFIX=inventory
export S3_BATCH_ROLE_ARN=arn:aws:iam::123456789012:role/S3BatchOperationsChecksumRole
```

Then create an S3 Batch Operations job that computes full-object SHA-256
checksums and writes both the completion report and generated manifest back to
the same bucket:

```sh
aws s3control create-job \
  --account-id "$AWS_ACCOUNT_ID" \
  --region "$AWS_REGION" \
  --confirmation-required \
  --operation '{"S3ComputeObjectChecksum":{"ChecksumAlgorithm":"SHA256","ChecksumType":"FULL_OBJECT"}}' \
  --report "{\"Bucket\":\"arn:aws:s3:::$S3_BUCKET\",\"Format\":\"Report_CSV_20180820\",\"Enabled\":true,\"ReportScope\":\"AllTasks\",\"ExpectedBucketOwner\":\"$AWS_ACCOUNT_ID\"}" \
  --manifest-generator "{\"S3JobManifestGenerator\":{\"ExpectedBucketOwner\":\"$AWS_ACCOUNT_ID\",\"SourceBucket\":\"arn:aws:s3:::$S3_BUCKET\",\"ManifestOutputLocation\":{\"ExpectedManifestBucketOwner\":\"$AWS_ACCOUNT_ID\",\"Bucket\":\"arn:aws:s3:::$S3_BUCKET\",\"ManifestFormat\":\"S3InventoryReport_CSV_20211130\"},\"Filter\":{},\"EnableManifestOutput\":true}}" \
  --description "Compute SHA256 checksums - $S3_BUCKET" \
  --priority 10 \
  --role-arn "$S3_BATCH_ROLE_ARN" \
  --client-request-token "$(uuidgen)"
```

Use `--no-confirmation-required` instead of `--confirmation-required` if you
want the job to start immediately after creation.

After the job completes, download the completion report folder as-is. Keep the
AWS-created `manifest.json` and referenced `results/*.csv` files together, and
point `sympath` at that completion report `manifest.json`:

```sh
sympath import-s3-checksum-report /path/to/checksum-report/manifest.json
```

S3 report object keys are URL-encoded in AWS CSV output; `sympath` decodes
them during import before storing paths or matching inventory metadata.
Zero-byte S3 folder marker objects whose keys end in `/` are preserved in the
raw S3 report table but skipped from the comparable file inventory, matching
the local scanner's behavior of indexing regular files rather than directories.

For size-aware comparisons, also create a CSV-format S3 Inventory report with
the `Size` and `LastModifiedDate` fields enabled:

```sh
aws s3api put-bucket-inventory-configuration \
  --bucket "$S3_BUCKET" \
  --id sympath-size-inventory \
  --region "$AWS_REGION" \
  --expected-bucket-owner "$AWS_ACCOUNT_ID" \
  --inventory-configuration "{\"Destination\":{\"S3BucketDestination\":{\"AccountId\":\"$AWS_ACCOUNT_ID\",\"Bucket\":\"arn:aws:s3:::$S3_BUCKET\",\"Format\":\"CSV\",\"Prefix\":\"$S3_INVENTORY_PREFIX\"}},\"IsEnabled\":true,\"Id\":\"sympath-size-inventory\",\"IncludedObjectVersions\":\"Current\",\"OptionalFields\":[\"Size\",\"LastModifiedDate\"],\"Schedule\":{\"Frequency\":\"Weekly\"}}"
```

If the inventory destination bucket policy is not already configured, grant S3
permission to write inventory objects under the chosen prefix:

```sh
aws s3api put-bucket-policy \
  --bucket "$S3_BUCKET" \
  --region "$AWS_REGION" \
  --expected-bucket-owner "$AWS_ACCOUNT_ID" \
  --policy "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Sid\":\"AllowS3InventoryDelivery\",\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"s3.amazonaws.com\"},\"Action\":\"s3:PutObject\",\"Resource\":\"arn:aws:s3:::$S3_BUCKET/$S3_INVENTORY_PREFIX/*\",\"Condition\":{\"ArnLike\":{\"aws:SourceArn\":\"arn:aws:s3:::$S3_BUCKET\"},\"StringEquals\":{\"aws:SourceAccount\":\"$AWS_ACCOUNT_ID\",\"s3:x-amz-acl\":\"bucket-owner-full-control\"}}}]}"
```

The first inventory report can take up to 48 hours to appear. After it lands,
download the inventory report folder as-is. Keep the AWS-created
`manifest.json` and referenced `.csv.gz` files together, and pass the inventory
`manifest.json` to enrich imported S3 entries with object sizes and
last-modified timestamps:

```sh
sympath import-s3-checksum-report \
  --inventory-manifest /path/to/inventory/manifest.json \
  /path/to/checksum-report/manifest.json
```

Downloaded S3 Inventory bundles commonly place `manifest.json` in a timestamped
folder and the referenced `.csv.gz` objects in a sibling `data/` directory. Do
not flatten or rename the bundle; that layout is supported.

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
