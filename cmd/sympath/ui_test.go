package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"path/filepath"
	"strings"
	"testing"

	inventory "sympath"

	_ "modernc.org/sqlite"
)

func setupUITestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	identityA := inventory.MachineIdentity{MachineID: "machine-a", Hostname: "host-a"}
	if err := inventory.PrepareLocalMachineDB(context.Background(), db, identityA); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Insert scan for machine-a, root /data/photos
	_, err = db.ExecContext(ctx, `
		INSERT INTO scans (machine_id, hostname, root, started_at, finished_at, status)
		VALUES ('machine-a', 'host-a', '/data/photos', 1000, 2000, 'complete')
	`)
	if err != nil {
		t.Fatal(err)
	}
	var scanA int64
	db.QueryRowContext(ctx, "SELECT last_insert_rowid()").Scan(&scanA)

	_, err = db.ExecContext(ctx, `
		INSERT INTO roots (machine_id, root, current_scan_id) VALUES ('machine-a', '/data/photos', ?)
		ON CONFLICT(machine_id, root) DO UPDATE SET current_scan_id = excluded.current_scan_id
	`, scanA)
	if err != nil {
		t.Fatal(err)
	}

	// Insert scan for machine-b, root /data/photos
	_, err = db.ExecContext(ctx, `
		INSERT INTO scans (machine_id, hostname, root, started_at, finished_at, status)
		VALUES ('machine-b', 'host-b', '/data/photos', 1000, 2000, 'complete')
	`)
	if err != nil {
		t.Fatal(err)
	}
	var scanB int64
	db.QueryRowContext(ctx, "SELECT last_insert_rowid()").Scan(&scanB)

	_, err = db.ExecContext(ctx, `
		INSERT INTO roots (machine_id, root, current_scan_id) VALUES ('machine-b', '/data/photos', ?)
	`, scanB)
	if err != nil {
		t.Fatal(err)
	}

	// Entries for scan A:
	//   identical.txt          - same on both
	//   left-only.txt          - only on A
	//   different.txt          - different content
	//   sub/nested.txt         - same on both (in subdir)
	//   sub/only-a.txt         - only on A (in subdir)
	//   sub/deep/file.txt      - same on both (in deep subdir)
	for _, e := range []struct {
		relPath, name, ext, sha256 string
		size                       int64
	}{
		{"identical.txt", "identical", ".txt", "aaa111", 100},
		{"left-only.txt", "left-only", ".txt", "bbb222", 200},
		{"different.txt", "different", ".txt", "ccc333", 300},
		{"sub/nested.txt", "nested", ".txt", "fff666", 600},
		{"sub/only-a.txt", "only-a", ".txt", "ggg777", 700},
		{"sub/deep/file.txt", "file", ".txt", "hhh888", 800},
	} {
		_, err = db.ExecContext(ctx, `
			INSERT INTO entries (scan_id, rel_path, name, ext, size, mtime_ns, sha256, state)
			VALUES (?, ?, ?, ?, ?, 0, ?, 'ok')
		`, scanA, e.relPath, e.name, e.ext, e.size, e.sha256)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Entries for scan B:
	//   identical.txt          - same on both
	//   right-only.txt         - only on B
	//   different.txt          - different content
	//   sub/nested.txt         - same on both
	//   sub/only-b.txt         - only on B (in subdir)
	//   sub/deep/file.txt      - same on both
	for _, e := range []struct {
		relPath, name, ext, sha256 string
		size                       int64
	}{
		{"identical.txt", "identical", ".txt", "aaa111", 100},
		{"right-only.txt", "right-only", ".txt", "ddd444", 400},
		{"different.txt", "different", ".txt", "eee555", 500},
		{"sub/nested.txt", "nested", ".txt", "fff666", 600},
		{"sub/only-b.txt", "only-b", ".txt", "iii999", 900},
		{"sub/deep/file.txt", "file", ".txt", "hhh888", 800},
	} {
		_, err = db.ExecContext(ctx, `
			INSERT INTO entries (scan_id, rel_path, name, ext, size, mtime_ns, sha256, state)
			VALUES (?, ?, ?, ?, ?, 0, ?, 'ok')
		`, scanB, e.relPath, e.name, e.ext, e.size, e.sha256)
		if err != nil {
			t.Fatal(err)
		}
	}

	return db
}

func resolveUITestScanID(t *testing.T, db *sql.DB, machineID, root string) int64 {
	t.Helper()

	scanID, err := resolveScanID(context.Background(), db, machineID, root)
	if err != nil {
		t.Fatal(err)
	}
	return scanID
}

func insertUITestEntry(t *testing.T, db *sql.DB, scanID int64, relPath string, size int64, sha256 any) {
	t.Helper()

	name := path.Base(relPath)
	ext := path.Ext(name)
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO entries (scan_id, rel_path, name, ext, size, mtime_ns, sha256, state)
		VALUES (?, ?, ?, ?, ?, 0, ?, 'ok')
	`, scanID, relPath, name, ext, size, sha256); err != nil {
		t.Fatal(err)
	}
}

func TestOpenUIReadOnlyDB(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ui.sympath")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := inventory.PrepareLocalMachineDB(context.Background(), db, inventory.MachineIdentity{
		MachineID: "machine-a",
		Hostname:  "host-a",
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	readOnlyDB, err := openUIReadOnlyDB(context.Background(), dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnlyDB.Close()

	var metadataRows int
	if err := readOnlyDB.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM metadata").Scan(&metadataRows); err != nil {
		t.Fatal(err)
	}
	if metadataRows == 0 {
		t.Fatal("expected metadata rows in read-only UI database")
	}
}

func TestRunUIWithIONoInventoryDB(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	err := runUIWithIO(nil, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("expected missing-inventory error")
	}
	if !strings.Contains(err.Error(), "run `sympath scan [ROOT]` first") {
		t.Fatalf("expected friendly scan-first error, got %v", err)
	}
}

func TestHandleRoots(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	req := httptest.NewRequest("GET", "/api/roots", nil)
	rec := httptest.NewRecorder()
	srv.handleRoots(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var roots []rootEntry
	if err := json.Unmarshal(rec.Body.Bytes(), &roots); err != nil {
		t.Fatal(err)
	}

	if len(roots) != 2 {
		t.Fatalf("expected 2 roots, got %d", len(roots))
	}

	machines := map[string]bool{}
	for _, r := range roots {
		machines[r.MachineID] = true
		if r.Root != "/data/photos" {
			t.Fatalf("unexpected root %q", r.Root)
		}
	}
	if !machines["machine-a"] || !machines["machine-b"] {
		t.Fatalf("expected machine-a and machine-b, got %v", machines)
	}
}

func TestHandleDirs(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	req := httptest.NewRequest("GET", "/api/dirs?machine_id=machine-a&root=/data/photos", nil)
	rec := httptest.NewRecorder()
	srv.handleDirs(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var dirs []string
	if err := json.Unmarshal(rec.Body.Bytes(), &dirs); err != nil {
		t.Fatal(err)
	}

	// Should contain "sub" and "sub/deep"
	dirSet := map[string]bool{}
	for _, d := range dirs {
		dirSet[d] = true
	}
	if !dirSet["sub"] {
		t.Fatalf("expected 'sub' in dirs, got %v", dirs)
	}
	if !dirSet["sub/deep"] {
		t.Fatalf("expected 'sub/deep' in dirs, got %v", dirs)
	}
	if len(dirs) != 2 {
		t.Fatalf("expected 2 dirs, got %d: %v", len(dirs), dirs)
	}
}

func TestHandleDirsMissingParams(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	req := httptest.NewRequest("GET", "/api/dirs", nil)
	rec := httptest.NewRecorder()
	srv.handleDirs(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleCompare(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	req := httptest.NewRequest("GET", "/api/compare?left_machine=machine-a&left_root=/data/photos&right_machine=machine-b&right_root=/data/photos", nil)
	rec := httptest.NewRecorder()
	srv.handleCompare(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result compareResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	// identical: identical.txt, sub/nested.txt, sub/deep/file.txt = 3
	if result.IdenticalCount != 3 {
		t.Fatalf("expected 3 identical, got %d", result.IdenticalCount)
	}
	// left-only: left-only.txt, sub/only-a.txt = 2
	if len(result.LeftOnly) != 2 {
		t.Fatalf("expected 2 left-only, got %d", len(result.LeftOnly))
	}
	// right-only: right-only.txt, sub/only-b.txt = 2
	if len(result.RightOnly) != 2 {
		t.Fatalf("expected 2 right-only, got %d", len(result.RightOnly))
	}
	// different: different.txt = 1
	if len(result.Different) != 1 {
		t.Fatalf("expected 1 different, got %d", len(result.Different))
	}
	d := result.Different[0]
	if d.RelPath != "different.txt" {
		t.Fatalf("expected different.txt, got %q", d.RelPath)
	}
	if d.Left.Size != 300 || d.Right.Size != 500 {
		t.Fatalf("expected sizes 300/500, got %d/%d", d.Left.Size, d.Right.Size)
	}
}

func TestHandleCompareWithPrefix(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	// Compare only the "sub" subdirectory on both sides.
	req := httptest.NewRequest("GET", "/api/compare?left_machine=machine-a&left_root=/data/photos&right_machine=machine-b&right_root=/data/photos&left_prefix=sub&right_prefix=sub", nil)
	rec := httptest.NewRecorder()
	srv.handleCompare(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result compareResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	// Within sub/: nested.txt identical, deep/file.txt identical = 2
	if result.IdenticalCount != 2 {
		t.Fatalf("expected 2 identical in sub/, got %d", result.IdenticalCount)
	}
	// left-only in sub/: only-a.txt
	if len(result.LeftOnly) != 1 {
		t.Fatalf("expected 1 left-only in sub/, got %d", len(result.LeftOnly))
	}
	if result.LeftOnly[0].RelPath != "only-a.txt" {
		t.Fatalf("expected only-a.txt (prefix stripped), got %q", result.LeftOnly[0].RelPath)
	}
	// right-only in sub/: only-b.txt
	if len(result.RightOnly) != 1 {
		t.Fatalf("expected 1 right-only in sub/, got %d", len(result.RightOnly))
	}
	if result.RightOnly[0].RelPath != "only-b.txt" {
		t.Fatalf("expected only-b.txt (prefix stripped), got %q", result.RightOnly[0].RelPath)
	}
	// no different files in sub/
	if len(result.Different) != 0 {
		t.Fatalf("expected 0 different in sub/, got %d", len(result.Different))
	}
}

func TestHandleCompareWithLiteralGlobPrefix(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	leftScan := resolveUITestScanID(t, db, "machine-a", "/data/photos")
	rightScan := resolveUITestScanID(t, db, "machine-b", "/data/photos")

	insertUITestEntry(t, db, leftScan, "special[raw]/match.txt", 111, "match-hash")
	insertUITestEntry(t, db, rightScan, "special[raw]/match.txt", 111, "match-hash")
	insertUITestEntry(t, db, leftScan, "specialr/left-only.txt", 222, "left-decoy")
	insertUITestEntry(t, db, rightScan, "specialr/right-only.txt", 333, "right-decoy")

	req := httptest.NewRequest("GET", "/api/compare?left_machine=machine-a&left_root=/data/photos&right_machine=machine-b&right_root=/data/photos&left_prefix=special[raw]&right_prefix=special[raw]", nil)
	rec := httptest.NewRecorder()
	srv.handleCompare(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result compareResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	if result.IdenticalCount != 1 {
		t.Fatalf("expected 1 literal-prefix identical file, got %d", result.IdenticalCount)
	}
	if len(result.LeftOnly) != 0 || len(result.RightOnly) != 0 || len(result.Different) != 0 {
		t.Fatalf("expected only the literal-prefix match, got left=%d right=%d diff=%d",
			len(result.LeftOnly), len(result.RightOnly), len(result.Different))
	}
}

func TestHandleCompareMissingParams(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	req := httptest.NewRequest("GET", "/api/compare?left_machine=machine-a", nil)
	rec := httptest.NewRecorder()
	srv.handleCompare(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHandleCompareIdenticalScan(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	req := httptest.NewRequest("GET", "/api/compare?left_machine=machine-a&left_root=/data/photos&right_machine=machine-a&right_root=/data/photos", nil)
	rec := httptest.NewRecorder()
	srv.handleCompare(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result compareResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	if result.IdenticalCount != 6 {
		t.Fatalf("expected 6 identical (same scan), got %d", result.IdenticalCount)
	}
	if len(result.LeftOnly) != 0 || len(result.RightOnly) != 0 || len(result.Different) != 0 {
		t.Fatalf("expected no differences for same scan, got left=%d right=%d diff=%d",
			len(result.LeftOnly), len(result.RightOnly), len(result.Different))
	}
}

func TestHandleCompareByContent(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	req := httptest.NewRequest("GET", "/api/compare?left_machine=machine-a&left_root=/data/photos&right_machine=machine-b&right_root=/data/photos&by_content=1", nil)
	rec := httptest.NewRecorder()
	srv.handleCompare(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result compareResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	// Content matching: sha256 aaa111, fff666, hhh888 appear on both sides = 3 identical.
	if result.IdenticalCount != 3 {
		t.Fatalf("expected 3 identical by content, got %d", result.IdenticalCount)
	}
	// Left-only by content: bbb222 (left-only.txt), ccc333 (different.txt), ggg777 (sub/only-a.txt) = 3
	if len(result.LeftOnly) != 3 {
		t.Fatalf("expected 3 left-only by content, got %d: %v", len(result.LeftOnly), result.LeftOnly)
	}
	// Right-only by content: ddd444 (right-only.txt), eee555 (different.txt), iii999 (sub/only-b.txt) = 3
	if len(result.RightOnly) != 3 {
		t.Fatalf("expected 3 right-only by content, got %d: %v", len(result.RightOnly), result.RightOnly)
	}
	// No "different" category in content mode.
	if len(result.Different) != 0 {
		t.Fatalf("expected 0 different in content mode, got %d", len(result.Different))
	}
}

func TestHandleCompareIncludesMissingHashesAsDifferent(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	leftScan := resolveUITestScanID(t, db, "machine-a", "/data/photos")
	rightScan := resolveUITestScanID(t, db, "machine-b", "/data/photos")

	insertUITestEntry(t, db, leftScan, "unknown.txt", 123, nil)
	insertUITestEntry(t, db, rightScan, "unknown.txt", 123, "")

	req := httptest.NewRequest("GET", "/api/compare?left_machine=machine-a&left_root=/data/photos&right_machine=machine-b&right_root=/data/photos", nil)
	rec := httptest.NewRecorder()
	srv.handleCompare(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result compareResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	if result.IdenticalCount != 3 {
		t.Fatalf("expected identical count to remain 3, got %d", result.IdenticalCount)
	}
	if len(result.Different) != 2 {
		t.Fatalf("expected 2 different rows including unknown.txt, got %d", len(result.Different))
	}

	foundUnknown := false
	for _, diff := range result.Different {
		if diff.RelPath == "unknown.txt" {
			foundUnknown = true
			if diff.Left.Size != 123 || diff.Right.Size != 123 {
				t.Fatalf("expected unknown.txt sizes to remain visible, got %d/%d", diff.Left.Size, diff.Right.Size)
			}
		}
	}
	if !foundUnknown {
		t.Fatalf("expected unknown.txt in different results, got %v", result.Different)
	}
}

func TestHandleCompareByContentWithPrefix(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	// Left filtered to "sub/", right unfiltered. sub/ on left has:
	//   nested.txt (fff666), only-a.txt (ggg777), deep/file.txt (hhh888)
	// Right (all): identical.txt (aaa111), right-only.txt (ddd444),
	//   different.txt (eee555), sub/nested.txt (fff666),
	//   sub/only-b.txt (iii999), sub/deep/file.txt (hhh888)
	// Content matches: fff666 and hhh888 = 2 identical.
	// Left-only by content: ggg777 = 1
	req := httptest.NewRequest("GET", "/api/compare?left_machine=machine-a&left_root=/data/photos&right_machine=machine-b&right_root=/data/photos&left_prefix=sub&by_content=1", nil)
	rec := httptest.NewRecorder()
	srv.handleCompare(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var result compareResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	if result.IdenticalCount != 2 {
		t.Fatalf("expected 2 identical by content with prefix, got %d", result.IdenticalCount)
	}
	if len(result.LeftOnly) != 1 {
		t.Fatalf("expected 1 left-only by content with prefix, got %d: %v", len(result.LeftOnly), result.LeftOnly)
	}
}
