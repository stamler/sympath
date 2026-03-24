# sympath - Directory Inventory & One-Way Sync Index

## Overview

`sympath` is a cross-platform Go library that inventories a directory
tree and stores file metadata and content hashes in SQLite. It is designed as
the foundation for a high-performance, asynchronous, one-way sync tool in the
spirit of rsync.

The library exposes a single public function:

```go
func InventoryTree(ctx context.Context, db *sql.DB, root string) error
```

It also now includes a basic CLI:

```bash
go run ./cmd/sympath scan /path/to/root
```

or, equivalently:

```bash
go run ./cmd/sympath /path/to/root
```

On each run, the CLI consolidates all `~/.sympath/*.sympath` files into
one surviving database file and then scans into that file. If the
directory is empty, it creates a new random 10-character alphanumeric
`.sympath` filename with `NewRandomSympathFilename()`. Pass `--verbose`
to print startup messages about directory creation, consolidation, and
the final database path in use.

Each call produces a complete snapshot of every regular file under `root`.
Subsequent calls reuse hashes from the previous snapshot when a file's size
and modification time are unchanged, making re-scans of mostly-static trees
very fast.

## Design Principles

- **Publish-on-complete**: A new scan only becomes authoritative after the
  entire pipeline finishes. If the process crashes mid-scan, the previous
  completed scan remains the source of truth.
- **Single snapshot per root**: After a successful scan, the previous scan is
  immediately garbage-collected. The database always holds exactly one
  completed scan per root.
- **Accept live filesystem drift**: The scan is not instantaneous. Files may
  change while the tree is being walked and hashed. The design accepts this
  unavoidable inconsistency while preventing self-inflicted DB inconsistency
  (e.g., partial scans driving delete decisions).
- **Speed over ceremony**: Incremental hashing, worker pools, batch
  transactions, and WAL mode keep throughput high.

## Architecture

### Processing Pipeline

```
Walker (1 goroutine)
  |
  +---> entryCh ---> DB Writer (1 goroutine, priority-drain)
  |
  +---> jobCh ----> Hash Workers (N goroutines) ---> resultCh ---> DB Writer
```

1. **Walker** traverses the directory tree via `filepath.WalkDir`. For each
   regular file it emits a base entry (metadata only) to the DB writer and,
   if hashing is needed, a hash job to the worker pool.
2. **Hash workers** read file content, compute both a full SHA-256 and a fast
   fingerprint in a single pass, then validate that the file did not change
   during reading (stat-before / stat-after).
3. **DB writer** receives base entries and hash results on two channels. It
   always drains pending entries before processing results, guaranteeing that
   the INSERT for a file precedes the UPDATE that enriches it with hashes.

### Incremental Reuse

Before starting a new scan the previous scan's entries are preloaded into an
in-memory map keyed by relative path. During the walk, if a file's `(size,
mtime_ns)` match the previous entry, its fingerprint and SHA-256 are copied
directly into the new scan and the file is not re-read. On a tree where 90%
of files are unchanged, this skips 90% of disk I/O.

### Scan Lifecycle

```
1. Read roots.current_scan_id -> preload previous entries
2. INSERT into scans (status = 'running')
3. Walk tree, emit base entries + hash jobs
4. Workers drain job queue, emit results
5. Writer flushes all inserts and updates
6. Publish (single short transaction):
     a. UPDATE scans SET status = 'complete'
     b. UPSERT roots.current_scan_id
     c. DELETE all other scans for this root (CASCADE deletes entries)
7. On failure: mark scan 'failed', leave previous scan as authoritative
```

## Database Schema

Three tables, one index each:

### `roots`

One row per monitored root directory. Points at the current authoritative scan.

| Column            | Type    | Notes               |
|-------------------|---------|----------------------|
| root              | TEXT PK | Absolute path        |
| current_scan_id   | INTEGER | FK to scans.scan_id  |

### `scans`

One row per scan attempt.

| Column          | Type    | Notes                                  |
|-----------------|---------|----------------------------------------|
| scan_id         | INTEGER PK | Auto-increment (SQLite rowid alias) |
| root            | TEXT    | Absolute path of the scanned directory |
| started_at      | INTEGER | Unix nanoseconds                       |
| finished_at     | INTEGER | Unix nanoseconds (NULL while running)  |
| status          | TEXT    | `'running'`, `'complete'`, or `'failed'` |
| goos            | TEXT    | `runtime.GOOS` at scan time            |
| goarch          | TEXT    | `runtime.GOARCH` at scan time          |
| fs_type         | TEXT    | Best-effort filesystem name (e.g. `apfs`, `ext4`, `NTFS`) |
| case_sensitive  | INTEGER | 1 = case-sensitive, 0 = case-insensitive |

### `entries`

One row per regular file discovered during a scan.

| Column      | Type    | Notes                                          |
|-------------|---------|------------------------------------------------|
| scan_id     | INTEGER | FK to scans (ON DELETE CASCADE)                |
| rel_path    | TEXT    | Forward-slash path relative to root            |
| name        | TEXT    | Base filename                                  |
| ext         | TEXT    | Lowercase extension including dot (e.g. `.txt`)|
| size        | INTEGER | Content size in bytes                          |
| mtime_ns    | INTEGER | Modification time as Unix nanoseconds          |
| fingerprint | TEXT    | Partial SHA-256 hex (first+last 64KB + size)   |
| sha256      | TEXT    | Full content SHA-256 hex                       |
| state       | TEXT    | See Entry States below                         |
| err         | TEXT    | Human-readable error message (NULL if none)    |

Primary key: `(scan_id, rel_path)`.

### Indexes

- `idx_scans_root_status` on `scans(root, status)`
- `idx_entries_scan` on `entries(scan_id)`

### Pragmas

Set once per connection:

- `journal_mode = WAL`
- `synchronous = NORMAL`
- `foreign_keys = ON`
- `busy_timeout = 5000`

## Entry States

Every entry row carries a `state` that indicates how its data was obtained:

| State      | Meaning                                                |
|------------|--------------------------------------------------------|
| `ok`       | File was freshly read and hashed; hashes are valid.    |
| `reused`   | Size and mtime matched the previous scan; hashes were copied without re-reading the file. |
| `unstable` | File changed between the pre-read stat and the post-read stat. Hashes are present but unreliable. Retried once before marking. |
| `vanished` | File existed during discovery but was deleted before the hash worker could open it. No hashes. |
| `error`    | An I/O or permission error prevented reading the file. The `err` column contains the message. |
| `pending`  | Temporary state during scan processing. Should never appear in a completed scan. |

Downstream sync logic should act on `ok` and `reused` entries, and skip or
defer `unstable`, `vanished`, and `error` entries to future runs.

## Fast Fingerprint

The fingerprint is a cheap probabilistic content signature used to detect
whether a file's content has likely changed without reading the entire file.

**Algorithm**: `SHA-256(first_64KB || last_64KB || little_endian_uint64(size))`

- For files smaller than 128KB the first and last regions overlap. The
  fingerprint is still deterministic and content-dependent.
- The fingerprint is computed in the same read pass as the full SHA-256, so it
  adds no extra I/O.
- In V1 both fingerprint and full SHA-256 are always computed together. The
  fingerprint column enables a future optimization: on subsequent scans, if
  metadata changed but the fingerprint matches, the full hash could be
  skipped.

## Cross-Platform Support

| Concern               | macOS                  | Linux                  | Windows                    |
|-----------------------|------------------------|------------------------|-----------------------------|
| Filesystem detection  | `statfs` -> `f_fstypename` | `statfs` -> magic number map | `GetVolumeInformationW`  |
| Case sensitivity      | Probed via temp file   | Probed via temp file   | Volume flags                |
| mtime                 | `FileInfo.ModTime()`   | `FileInfo.ModTime()`   | `FileInfo.ModTime()`        |
| Path separator        | Stored as `/` (ToSlash)| Native `/`             | Stored as `/` (ToSlash)     |
| Symlinks              | Skipped by default     | Skipped by default     | Skipped by default          |
| SQLite driver         | `modernc.org/sqlite` (pure Go, no CGO)                                     |

### Symlink Resolution

On macOS `/var` is a symlink to `/private/var`. SQLite resolves symlinks
internally when reporting `PRAGMA database_list`, so all paths are resolved
through `filepath.EvalSymlinks` to ensure consistent comparison between the
walker, the exclude set, and the database root key.

## File Layout

```
types.go              Shared types: PrevEntry, HashJob, HashResult, baseEntry, VolumeInfo
schema.go             DDL constants, EnsureSchema, ConfigureConnection
fsinfo.go             Case-sensitivity probing (shared across platforms)
fsinfo_darwin.go      macOS filesystem type detection via statfs
fsinfo_linux.go       Linux filesystem type detection via statfs magic numbers
fsinfo_windows.go     Windows filesystem type detection via GetVolumeInformationW
exclude.go            DB file path discovery, symlink-safe path resolution, exclusion logic
hash.go               Fingerprint and SHA-256 computation, hash worker goroutine
walker.go             Directory tree walker, reuse decisions, skip logic
writer.go             Single DB writer goroutine with priority-drain and batch transactions
inventory.go          InventoryTree orchestration, scan lifecycle, publish and garbage collect
```

## Performance Characteristics

- **Batch transactions**: The writer commits every ~5000 operations to keep
  the WAL bounded without per-row commit overhead.
- **Worker pool**: `min(runtime.NumCPU(), 4)` hash workers. Capped at 4 to
  avoid disk thrashing on HDDs while still saturating SHA-256 throughput on
  NVMe.
- **Buffer reuse**: Each worker allocates a single 1MB read buffer at startup
  and reuses it across all files.
- **Preloaded previous scan**: The entire previous scan's metadata is loaded
  into a `map[string]PrevEntry` before the walk begins, making reuse lookups
  O(1).
- **Channel buffers**: 1000-element buffers on all three pipeline channels let
  the walker run ahead of the hash workers.

## Crash Safety

- If the process is interrupted during a scan, the incomplete scan row remains
  with `status = 'running'`. The `roots.current_scan_id` pointer is never
  updated, so the previous completed scan remains authoritative.
- On the next successful run, the publish step's `DELETE FROM scans WHERE
  root=? AND scan_id <> ?` cleans up both the previous completed scan and any
  orphaned running/failed scans in a single operation.
- SQLite's WAL journaling ensures the database file is never structurally
  corrupted by an unclean shutdown.

## Deletion Semantics

Files that were present in the previous scan but are absent from the new
discovery pass simply do not appear in the new scan's entries. Because the
old scan is deleted (CASCADE) after publish, these files vanish from the
database entirely.

This is the correct behavior for one-way sync: the database reflects what the
source tree looks like right now. A downstream sync planner can compare two
databases (source vs. destination) and treat "present in source, absent in
destination" as a copy, and "absent in source, present in destination" as a
delete.

Critically, delete decisions are only derived from completed scans, never
from partial or in-progress scans.

## Open Areas for Future Work

- **Bidirectional sync**: The current design assumes one-way (source of
  truth). Conflict resolution would require additional metadata.
- **Inode / file ID tracking**: Storing `(dev, inode)` on POSIX or the file
  index on Windows would enable rename detection without full rehashing.
- **Permission metadata**: Mode bits, owner, and group are not stored in V1.
  The schema can be extended with `ALTER TABLE ADD COLUMN`.
- **Content-defined chunking**: For very large files, chunked hashing (like
  rsync's rolling checksum) could reduce transfer size.
- **Watch-based incremental updates**: Using FSEvents (macOS), inotify
  (Linux), or ReadDirectoryChangesW (Windows) to detect changes between scans
  instead of re-walking.
