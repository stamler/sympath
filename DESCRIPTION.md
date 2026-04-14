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

On each run, the CLI ensures `~/.sympath` exists, seeds a documented
`~/.sympath/remotes` file with a comment template if one is missing, and
ensures a stable `~/.sympath/machine-id` file exists. When no other scan is
active, startup may fetch remote `~/.sympath/*.sympath` databases listed in
`remotes` and consolidate the local and fetched databases into one surviving
local database file before scanning into it. If another disjoint scan is
already active, the new scan joins the shared DB guard and reuses the current
local database without running a new consolidation pass. If no database exists
yet, startup creates a new random 10-character alphanumeric `.sympath`
filename with `NewRandomSympathFilename()`. Pass `--verbose` to print startup
messages about directory creation, remote fetching, consolidation or database
reuse, and the final database path in use. When stderr is an interactive
terminal, the CLI also renders a live in-place scan progress line with an
ASCII spinner, a bouncing shaded block track, and running file counts.

The `sympath ui` command follows the same local startup discipline, but with a
different lifetime model: it takes the startup lock plus the global DB guard
exclusively, consolidates only local databases, opens the chosen database
read-only while those locks are still held, and then releases both locks
before serving HTTP. That intentionally makes the UI a best-effort snapshot of
the database it opened at startup. Later scans may reconsolidate and publish a
newer database while the UI keeps serving the already-open snapshot.

Each call produces a complete snapshot of every regular file under `root`.
Subsequent calls first reuse hashes from the current authoritative snapshot
and the newest failed interrupted scan for that exact root, then can fall
back to overlapping authoritative scans on the same machine when a file's
size and modification time are unchanged, making re-scans of mostly-static
trees very fast.

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

Before starting a new scan, exact-root reusable entries from the current
authoritative scan and the newest failed interrupted scan are preloaded into
an in-memory candidate map keyed by relative path. During the walk, if a
file's `(size, mtime_ns)` match an exact candidate and all matching
candidates agree on `fingerprint` and `sha256`, those hashes are copied
directly into the new scan and the file is not re-read. On an exact miss, the
walker can lazily consult overlapping authoritative scans from the same
machine by translating their relative paths into the target root's coordinate
system. On a tree where 90% of files are unchanged, this skips 90% of disk
I/O.

### Scan Lifecycle

```
1. Acquire a startup lock and normalize the root
2. Try to acquire the global DB guard exclusively:
     a. if successful, resolve the consolidated DB (including remote fetch and
        consolidation when needed), acquire the per-root scan lock, initialize
        the DB if needed, then downgrade the DB guard to shared mode
     b. if another scan already holds the DB guard shared, join it in shared
        mode, reuse the existing local DB without consolidation, and acquire
        the per-root scan lock
3. Release the startup lock and keep the shared DB guard plus the per-root
   scan lock for the duration of the scan
4. Mark any lingering `scans.status = 'running'` rows for this
   machine/root as `failed`
5. Read roots.current_scan_id plus the newest failed interrupted scan for
   this machine/root -> preload exact reuse candidates
6. INSERT into scans (status = 'running')
7. Walk tree, reusing exact matches first and consulting overlapping
   authoritative roots on same-machine misses; emit base entries + hash jobs
8. Workers drain job queue, emit results
9. Writer flushes all inserts and updates
10. Publish (single short transaction):
     a. UPDATE scans SET status = 'complete'
     b. UPSERT roots.current_scan_id
     c. DELETE all other scans for this root (CASCADE deletes entries)
11. On failure: mark scan 'failed', leave previous scan as authoritative
```

### Locking Model

The concurrency model relies on three distinct locks, each with a different
scope and lifetime:

- **Startup lock**: A short-lived process lock around the startup transition
  where a command decides which database file to use and which longer-lived
  locks it should take. This prevents two processes from simultaneously
  inspecting the lock set and making conflicting startup decisions.
- **Global DB guard**: A lock over the shared `~/.sympath` database set. Active
  scans hold it in shared mode for their full lifetime so the chosen local
  database cannot be consolidated away underneath them. Consolidation paths
  such as scan startup without an already-active scan, and `sympath ui`
  startup, take it exclusively while selecting and opening the database they
  need.
- **Per-root scan lock**: A long-lived lock keyed by `(machine_id, normalized
  root)` and annotated with metadata describing that root. This lock rejects
  overlapping roots on the same machine while still allowing disjoint roots to
  scan concurrently.

| Lock               | Scope                                       | Held By                                                              | Held For                                                                       | Prevents                                                                  |
|--------------------|---------------------------------------------|----------------------------------------------------------------------|--------------------------------------------------------------------------------|---------------------------------------------------------------------------|
| Startup lock       | Whole local `~/.sympath` startup transition | `scan`, `ui`                                                         | Only while choosing the DB and acquiring longer-lived locks                    | Two processes making conflicting startup decisions at the same time       |
| Global DB guard    | Shared local database set in `~/.sympath`   | `scan` in shared mode; startup/consolidation paths in exclusive mode | Shared for the full scan lifetime; exclusive only while selecting/opening a DB | Consolidation replacing or deleting the live DB underneath an active scan |
| Per-root scan lock | One normalized root on one machine          | `scan`                                                               | Full scan lifetime                                                             | Concurrent overlapping scans for the same machine                         |

Together these locks enforce the intended concurrency policy:

- Two disjoint scans can run at the same time.
- Two overlapping scans cannot run at the same time.
- Consolidation cannot replace or delete the live database while a scan is
  actively using it.
- The UI opens a database under startup protection, then intentionally releases
  the locks and serves that already-open database as a best-effort snapshot.

## Database Schema

Four tables are used:

### `metadata`

Small key/value storage used for the local machine identity assigned to
the database currently being scanned into.

### `roots`

One row per monitored root directory per machine. Points at the current
authoritative scan for that machine/path pair.

| Column            | Type    | Notes               |
|-------------------|---------|----------------------|
| machine_id        | TEXT PK | Stable machine identifier |
| root              | TEXT PK | Absolute path        |
| current_scan_id   | INTEGER | FK to scans.scan_id  |

### `scans`

One row per scan attempt.

| Column          | Type    | Notes                                  |
|-----------------|---------|----------------------------------------|
| scan_id         | INTEGER PK | Auto-increment (SQLite rowid alias) |
| machine_id      | TEXT    | Stable machine identifier              |
| hostname        | TEXT    | Human-readable machine hostname        |
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

- `idx_scans_machine_root_status` on `scans(machine_id, root, status)`
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
| `reused`   | Size and mtime matched an exact previous entry or a trusted overlapping authoritative scan; hashes were copied without re-reading the file. |
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
- **Exact-root fast path**: Exact-root candidates from the authoritative scan
  plus the newest failed interrupted scan are loaded into a
  `map[string][]PrevEntry` before the walk begins, making unchanged-file reuse
  O(1) while still allowing conflicting candidates to be rejected safely.
- **Lazy overlap fallback**: Overlapping authoritative scans on the same
  machine are only loaded after an exact-root miss, so ordinary rescans keep
  the old fast path.
- **Channel buffers**: 1000-element buffers on all three pipeline channels let
  the walker run ahead of the hash workers.

## Crash Safety

- If the CLI handles `SIGINT` or `SIGTERM` cleanly during a scan, the in-flight
  scan is marked `failed` with a `finished_at` timestamp. Hard kills or crashes
  may still leave an orphaned `running` row.
- Before a new scan starts hashing, it acquires an overlap-aware per-root scan
  lock. Once that lock is held, any same-root `running` rows are known to be
  orphaned and are immediately adopted to `failed`.
- Interrupted scans stay non-authoritative: `roots.current_scan_id` is never
  updated, so the previous completed scan remains authoritative.
- On the next scan for the same machine/root, the newest failed interrupted
  scan is consulted as an additional exact-match reuse cache for entries whose
  `state IN ('ok', 'reused')` and whose `(rel_path, size, mtime_ns)` still
  match.
- The global DB guard prevents consolidation from renaming or deleting the live
  database while scans are active. Disjoint roots can still scan concurrently
  by sharing that guard in read mode, while overlapping roots remain
  serialized by the per-root scan lock.
- On the next successful run, the publish step's
  `DELETE FROM scans WHERE machine_id=? AND root=? AND scan_id <> ?` cleans up
  both the previous completed scan and any orphaned running/failed scans in a
  single operation.
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

## Database Retention and Space Reuse

The database is designed to avoid unbounded scan-history growth.

- For local scans, a successful publish keeps exactly one completed scan per
  `(machine_id, root)`. Older completed scans for that root are deleted
  immediately, and their `entries` rows are removed via `ON DELETE CASCADE`.
- Failed or interrupted scans are kept only as a temporary reuse source for
  the next scan of the same root. They remain non-authoritative and are
  deleted by the next successful publish for that root.
- In consolidated databases, repeated imports replace the existing
  authoritative snapshot for the same `(machine_id, root)` instead of stacking
  duplicate historical scans.
- If a remote is removed from the aggregator's `remotes` file, that remote's
  previously imported machine data is purged on the next successful
  consolidation run.

This means scan rows are cleaned up logically, but SQLite may still keep the
database file at its current size on disk. Deleted rows free pages for reuse
by future scans, but the file does not necessarily shrink immediately.

`sympath` currently does not run `VACUUM` automatically. That is intentional:
`VACUUM` rewrites the whole database, which can be expensive for large
inventories and is not something we want on the hot path of ordinary scans.
If reclaiming disk space becomes important for long-lived databases, an
explicit maintenance command would be a better fit than vacuuming after every
scan.

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
