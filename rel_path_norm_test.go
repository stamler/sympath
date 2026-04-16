package inventory

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

const machineAwareSchemaWithoutRelPathNorm = `
CREATE TABLE metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE roots (
    machine_id TEXT NOT NULL,
    root TEXT NOT NULL,
    current_scan_id INTEGER,
    PRIMARY KEY (machine_id, root)
);

CREATE TABLE scans (
    scan_id INTEGER PRIMARY KEY,
    machine_id TEXT NOT NULL,
    hostname TEXT NOT NULL,
    root TEXT NOT NULL,
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    status TEXT NOT NULL,
    goos TEXT,
    goarch TEXT,
    fs_type TEXT,
    case_sensitive INTEGER
);

CREATE TABLE entries (
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
    PRIMARY KEY (scan_id, rel_path)
);
`

func TestEnsureRelPathNormBackfill_MigratesSparseValuesOnce(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if _, err := db.Exec(machineAwareSchemaWithoutRelPathNorm); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO scans (scan_id, machine_id, hostname, root, started_at, status) VALUES
			(1, 'machine-a', 'host-a', '/music', 1, 'complete');
		INSERT INTO roots (machine_id, root, current_scan_id) VALUES
			('machine-a', '/music', 1);
		INSERT INTO entries (scan_id, rel_path, name, ext, size, mtime_ns, sha256, state) VALUES
			(1, 'albums/La Vie de rêve/song.flac', 'song.flac', '.flac', 111, 0, 'sha-a', 'ok'),
			(1, 'albums/plain/song.txt', 'song.txt', '.txt', 222, 0, 'sha-b', 'ok');
	`); err != nil {
		t.Fatal(err)
	}

	identity := MachineIdentity{MachineID: "machine-a", Hostname: "host-a"}
	if err := PrepareLocalMachineDB(ctx, db, identity); err != nil {
		t.Fatal(err)
	}
	if err := EnsureRelPathNormBackfill(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := EnsureRelPathNormBackfill(ctx, db); err != nil {
		t.Fatal(err)
	}

	columns, err := tableColumns(ctx, db, "entries")
	if err != nil {
		t.Fatal(err)
	}
	if columns["rel_path_norm"] == "" {
		t.Fatal("expected rel_path_norm column after schema upgrade")
	}

	rows, err := db.Query(`SELECT rel_path, rel_path_norm FROM entries ORDER BY rel_path`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	got := map[string]sql.NullString{}
	for rows.Next() {
		var relPath string
		var relPathNorm sql.NullString
		if err := rows.Scan(&relPath, &relPathNorm); err != nil {
			t.Fatal(err)
		}
		got[relPath] = relPathNorm
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	decomposed := "albums/La Vie de rêve/song.flac"
	if got[decomposed].String != "albums/La Vie de rêve/song.flac" {
		t.Fatalf("expected normalized compare path for %q, got %#v", decomposed, got[decomposed])
	}
	plain := "albums/plain/song.txt"
	if got[plain].Valid {
		t.Fatalf("expected unchanged ASCII path to remain sparse NULL, got %#v", got[plain])
	}

	var marker string
	if err := db.QueryRow(`SELECT value FROM metadata WHERE key = ?`, metadataRelPathNormBackfillV1Key).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if marker != relPathNormBackfillDoneValue {
		t.Fatalf("expected backfill marker %q, got %q", relPathNormBackfillDoneValue, marker)
	}
}

func TestImportCurrentScans_ComputesRelPathNormFromOlderSource(t *testing.T) {
	ctx := context.Background()
	sourceDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer sourceDB.Close()
	if _, err := sourceDB.Exec(machineAwareSchemaWithoutRelPathNorm); err != nil {
		t.Fatal(err)
	}
	if _, err := sourceDB.Exec(`
		INSERT INTO scans (scan_id, machine_id, hostname, root, started_at, status) VALUES
			(1, 'remote-machine', 'remote-host', '/music', 1, 'complete');
		INSERT INTO roots (machine_id, root, current_scan_id) VALUES
			('remote-machine', '/music', 1);
		INSERT INTO entries (scan_id, rel_path, name, ext, size, mtime_ns, sha256, state) VALUES
			(1, 'albums/La Vie de rêve/song.flac', 'song.flac', '.flac', 111, 0, 'sha-a', 'ok');
	`); err != nil {
		t.Fatal(err)
	}

	targetDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer targetDB.Close()
	if err := PrepareLocalMachineDB(ctx, targetDB, MachineIdentity{MachineID: "local-machine", Hostname: "local-host"}); err != nil {
		t.Fatal(err)
	}

	if _, err := ImportCurrentScans(ctx, targetDB, sourceDB); err != nil {
		t.Fatal(err)
	}

	var relPath string
	var relPathNorm sql.NullString
	if err := targetDB.QueryRow(`SELECT rel_path, rel_path_norm FROM entries`).Scan(&relPath, &relPathNorm); err != nil {
		t.Fatal(err)
	}
	if relPath != "albums/La Vie de rêve/song.flac" {
		t.Fatalf("unexpected raw rel_path %q", relPath)
	}
	if relPathNorm.String != "albums/La Vie de rêve/song.flac" {
		t.Fatalf("expected imported normalized compare path, got %#v", relPathNorm)
	}
}
