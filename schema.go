package inventory

// schema.go defines the SQLite schema and connection configuration.
//
// Three tables are used:
//   - roots:   one row per monitored directory, pointing at the current scan.
//   - scans:   one row per scan attempt, tracking lifecycle and platform metadata.
//   - entries: one row per regular file discovered during a scan.
//
// Foreign key CASCADE on entries ensures that deleting a scan row
// automatically removes all of its entries.

import (
	"context"
	"database/sql"
)

// createTablesSQL contains the DDL for the three core tables and their
// indexes. It is idempotent (IF NOT EXISTS on every statement).
const createTablesSQL = `
CREATE TABLE IF NOT EXISTS roots (
    root TEXT PRIMARY KEY,
    current_scan_id INTEGER
);

CREATE TABLE IF NOT EXISTS scans (
    scan_id INTEGER PRIMARY KEY,
    root TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    status TEXT NOT NULL CHECK(status IN ('running','complete','failed')),
    goos TEXT,
    goarch TEXT,
    fs_type TEXT,
    case_sensitive INTEGER
);

CREATE TABLE IF NOT EXISTS entries (
    scan_id INTEGER NOT NULL,
    rel_path TEXT NOT NULL,
    name TEXT NOT NULL,
    ext TEXT NOT NULL,
    size INTEGER NOT NULL,
    mtime_ns INTEGER NOT NULL,
    fingerprint TEXT,
    sha256 TEXT,
    state TEXT,
    err TEXT,
    PRIMARY KEY (scan_id, rel_path),
    FOREIGN KEY (scan_id) REFERENCES scans(scan_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_scans_root_status ON scans(root, status);
CREATE INDEX IF NOT EXISTS idx_entries_scan ON entries(scan_id);
`

// EnsureSchema creates the roots, scans, and entries tables along with
// their indexes. It is safe to call on every connection; all statements
// use IF NOT EXISTS.
func EnsureSchema(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, createTablesSQL)
	return err
}

// ConfigureConnection sets SQLite pragmas for performance and correctness.
// It should be called once per connection before any other operations:
//   - journal_mode=WAL:       enables concurrent reads during writes
//   - synchronous=NORMAL:     balances durability and speed
//   - foreign_keys=ON:        enforces CASCADE deletes on entries
//   - busy_timeout=5000:      waits up to 5s on lock contention
func ConfigureConnection(ctx context.Context, db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
	}
	for _, p := range pragmas {
		if _, err := db.ExecContext(ctx, p); err != nil {
			return err
		}
	}
	return nil
}
