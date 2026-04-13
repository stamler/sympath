package inventory

// schema.go defines the SQLite schema, compatibility migration, and
// connection configuration for sympath.
//
// The multi-machine startup flow depends on a few guarantees that the
// original single-machine schema did not provide:
//
//   - a scan must identify the machine that produced it
//   - the "current root" pointer must be keyed by machine + path, not
//     just path, so two machines can both scan "/Users/dean"
//   - the database needs a small metadata area for local-machine
//     identity so repeated scans use a stable machine ID
//
// The current schema therefore uses four tables:
//
//   - metadata: key/value settings such as the local machine identity
//   - roots: one row per authoritative machine/path snapshot
//   - scans: one row per scan attempt, tagged with machine metadata
//   - entries: one row per regular file in a scan
//
// Existing databases created before machine-aware consolidation are
// migrated in place. The migration is intentionally conservative:
// current scan rows are preserved, prior entries remain intact, and all
// legacy rows are backfilled with a caller-supplied local machine ID.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const (
	metadataTableName = "metadata"

	metadataLocalMachineIDKey = "local_machine_id"
	metadataLocalHostnameKey  = "local_hostname"
)

// createTablesSQL contains the DDL for the current schema and indexes.
// It is idempotent and safe to run on every opened database.
const createTablesSQL = `
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS roots (
    machine_id TEXT NOT NULL,
    root TEXT NOT NULL,
    current_scan_id INTEGER,
    PRIMARY KEY (machine_id, root)
);

CREATE TABLE IF NOT EXISTS scans (
    scan_id INTEGER PRIMARY KEY,
    machine_id TEXT NOT NULL,
    hostname TEXT NOT NULL,
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

CREATE INDEX IF NOT EXISTS idx_scans_machine_root_status ON scans(machine_id, root, status);
CREATE INDEX IF NOT EXISTS idx_entries_scan ON entries(scan_id);
`

// EnsureSchema creates or upgrades the database schema to the current
// machine-aware layout.
//
// New databases are created directly in the latest shape. Older
// databases are migrated in place if they still use the legacy
// single-machine roots/scans schema. The migration requires the caller
// to provide a stable machine identity so all legacy rows can be
// backfilled deterministically.
func EnsureSchema(ctx context.Context, db *sql.DB, identity MachineIdentity) error {
	if identity.MachineID == "" {
		return fmt.Errorf("machine identity is required")
	}
	if identity.Hostname == "" {
		return fmt.Errorf("hostname is required")
	}
	if err := migrateLegacySchema(ctx, db, identity); err != nil {
		return err
	}
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

func migrateLegacySchema(ctx context.Context, db *sql.DB, identity MachineIdentity) error {
	hasRoots, err := tableExists(ctx, db, "roots")
	if err != nil {
		return err
	}
	hasScans, err := tableExists(ctx, db, "scans")
	if err != nil {
		return err
	}
	if !hasRoots || !hasScans {
		return nil
	}

	rootColumns, err := tableColumns(ctx, db, "roots")
	if err != nil {
		return err
	}
	scanColumns, err := tableColumns(ctx, db, "scans")
	if err != nil {
		return err
	}

	rootsLegacy := rootColumns["machine_id"] == ""
	scansLegacy := scanColumns["machine_id"] == "" || scanColumns["hostname"] == ""
	if !rootsLegacy && !scansLegacy {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if scansLegacy {
		if scanColumns["machine_id"] == "" {
			if _, err := tx.ExecContext(ctx, "ALTER TABLE scans ADD COLUMN machine_id TEXT"); err != nil {
				return err
			}
		}
		if scanColumns["hostname"] == "" {
			if _, err := tx.ExecContext(ctx, "ALTER TABLE scans ADD COLUMN hostname TEXT"); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx,
			"UPDATE scans SET machine_id = ?, hostname = ? WHERE machine_id IS NULL OR machine_id = '' OR hostname IS NULL OR hostname = ''",
			identity.MachineID, identity.Hostname,
		); err != nil {
			return err
		}
	}

	if rootsLegacy {
		if _, err := tx.ExecContext(ctx, `
			CREATE TABLE roots_new (
				machine_id TEXT NOT NULL,
				root TEXT NOT NULL,
				current_scan_id INTEGER,
				PRIMARY KEY (machine_id, root)
			)
		`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO roots_new (machine_id, root, current_scan_id) SELECT ?, root, current_scan_id FROM roots",
			identity.MachineID,
		); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DROP TABLE roots"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "ALTER TABLE roots_new RENAME TO roots"); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS metadata (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)
	`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO metadata (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		metadataLocalMachineIDKey, identity.MachineID,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO metadata (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		metadataLocalHostnameKey, identity.Hostname,
	); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		"CREATE INDEX IF NOT EXISTS idx_scans_machine_root_status ON scans(machine_id, root, status)",
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		"CREATE INDEX IF NOT EXISTS idx_entries_scan ON entries(scan_id)",
	); err != nil {
		return err
	}

	return tx.Commit()
}

func tableExists(ctx context.Context, db *sql.DB, name string) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name = ?",
		name,
	).Scan(&count)
	return count > 0, err
}

func tableColumns(ctx context.Context, db *sql.DB, table string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string]string)
	for rows.Next() {
		var cid int
		var name, colType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultValue, &pk); err != nil {
			return nil, err
		}
		columns[strings.ToLower(name)] = colType
	}
	return columns, rows.Err()
}

// IsMachineAwareInventoryDB reports whether db looks like a sympath
// inventory database that already uses machine-aware roots/scans schema.
//
// This is used by the CLI when inspecting fetched remote databases. It
// intentionally performs a lightweight structural check rather than
// mutating the database during validation.
func IsMachineAwareInventoryDB(ctx context.Context, db *sql.DB) (bool, error) {
	hasRoots, err := tableExists(ctx, db, "roots")
	if err != nil {
		return false, err
	}
	hasScans, err := tableExists(ctx, db, "scans")
	if err != nil {
		return false, err
	}
	hasEntries, err := tableExists(ctx, db, "entries")
	if err != nil {
		return false, err
	}
	if !hasRoots || !hasScans || !hasEntries {
		return false, nil
	}

	rootColumns, err := tableColumns(ctx, db, "roots")
	if err != nil {
		return false, err
	}
	scanColumns, err := tableColumns(ctx, db, "scans")
	if err != nil {
		return false, err
	}

	if rootColumns["machine_id"] == "" {
		return false, nil
	}
	if scanColumns["machine_id"] == "" || scanColumns["hostname"] == "" {
		return false, nil
	}
	return true, nil
}
