package inventory

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func openTestDB(t *testing.T, dir string) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(dir, "inventory.sympath")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func createTestTree(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		path := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
}

func countEntries(t *testing.T, db *sql.DB, root string) int {
	t.Helper()
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*) FROM entries e
		JOIN roots r ON r.current_scan_id = e.scan_id
		WHERE r.root = ?`, root).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func countScans(t *testing.T, db *sql.DB, root string) int {
	t.Helper()
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM scans WHERE root = ?", root).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func getEntryState(t *testing.T, db *sql.DB, root, relPath string) string {
	t.Helper()
	var state string
	err := db.QueryRow(`
		SELECT e.state FROM entries e
		JOIN roots r ON r.current_scan_id = e.scan_id
		WHERE r.root = ? AND e.rel_path = ?`, root, relPath).Scan(&state)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func currentScanID(t *testing.T, db *sql.DB, root string) int64 {
	t.Helper()
	var scanID int64
	if err := db.QueryRow("SELECT current_scan_id FROM roots WHERE root = ?", root).Scan(&scanID); err != nil {
		t.Fatal(err)
	}
	return scanID
}

func updateEntryHashes(t *testing.T, db *sql.DB, scanID int64, relPath, fingerprint, sha256 string) {
	t.Helper()
	updateEntryResult(t, db, scanID, relPath, "ok", fingerprint, sha256)
}

func updateEntryResult(t *testing.T, db *sql.DB, scanID int64, relPath, state, fingerprint, sha256 string) {
	t.Helper()
	result, err := db.Exec(
		"UPDATE entries SET state = ?, fingerprint = ?, sha256 = ? WHERE scan_id = ? AND rel_path = ?",
		state, fingerprint, sha256, scanID, relPath,
	)
	if err != nil {
		t.Fatal(err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	if affected != 1 {
		t.Fatalf("expected 1 updated entry for scan %d path %q, got %d", scanID, relPath, affected)
	}
}

func getEntryHashes(t *testing.T, db *sql.DB, root, relPath string) (string, string) {
	t.Helper()
	var fingerprint, sha256 sql.NullString
	err := db.QueryRow(`
		SELECT e.fingerprint, e.sha256 FROM entries e
		JOIN roots r ON r.current_scan_id = e.scan_id
		WHERE r.root = ? AND e.rel_path = ?`, root, relPath,
	).Scan(&fingerprint, &sha256)
	if err != nil {
		t.Fatal(err)
	}
	return fingerprint.String, sha256.String
}

func insertManualScan(t *testing.T, db *sql.DB, machineID, hostname, root, status string) int64 {
	t.Helper()
	result, err := db.Exec(
		"INSERT INTO scans (machine_id, hostname, root, started_at, status) VALUES (?, ?, ?, ?, ?)",
		machineID, hostname, root, time.Now().UnixNano(), status,
	)
	if err != nil {
		t.Fatal(err)
	}
	scanID, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	return scanID
}

func insertManualEntry(t *testing.T, db *sql.DB, scanID int64, relPath string, size, mtimeNS int64, state, fingerprint, sha256 string) {
	t.Helper()
	name := filepath.Base(relPath)
	ext := strings.ToLower(filepath.Ext(name))
	var fingerprintValue any
	if fingerprint != "" {
		fingerprintValue = fingerprint
	}
	var shaValue any
	if sha256 != "" {
		shaValue = sha256
	}
	if _, err := db.Exec(`
		INSERT INTO entries (scan_id, rel_path, name, ext, size, mtime_ns, fingerprint, sha256, state)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, scanID, relPath, name, ext, size, mtimeNS, fingerprintValue, shaValue, state); err != nil {
		t.Fatal(err)
	}
}

func fileSizeAndMtime(t *testing.T, path string) (int64, int64) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size(), info.ModTime().UnixNano()
}

func TestReuseSources_DoesNotLoadOverlapWhenExactMatches(t *testing.T) {
	called := false
	expected := PrevEntry{
		Size:        5,
		MtimeNS:     7,
		Fingerprint: "fp",
		SHA256:      "sha",
	}
	reuse := &reuseSources{
		exact: map[string][]PrevEntry{"file.txt": {expected}},
		loadOverlap: func() (overlapReuseIndex, error) {
			called = true
			return overlapReuseIndex{
				"file.txt": {{PrevEntry: PrevEntry{Size: 5, MtimeNS: 7, Fingerprint: "other-fp", SHA256: "other-sha"}}},
			}, nil
		},
	}

	prev, ok, err := reuse.lookup("file.txt", 5, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected exact-root reuse hit")
	}
	if called {
		t.Fatal("expected exact-root hit to skip overlap loading")
	}
	if prev != expected {
		t.Fatalf("expected %+v, got %+v", expected, prev)
	}
}

func TestReuseSources_LoadsOverlapOnExactMiss(t *testing.T) {
	calls := 0
	expected := PrevEntry{
		Size:        5,
		MtimeNS:     7,
		Fingerprint: "fp",
		SHA256:      "sha",
	}
	reuse := &reuseSources{
		exact: map[string][]PrevEntry{},
		loadOverlap: func() (overlapReuseIndex, error) {
			calls++
			return overlapReuseIndex{
				"file.txt": {{PrevEntry: expected}},
			}, nil
		},
	}

	prev, ok, err := reuse.lookup("file.txt", 5, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected overlap reuse hit")
	}
	if prev != expected {
		t.Fatalf("expected %+v, got %+v", expected, prev)
	}

	prev, ok, err = reuse.lookup("file.txt", 5, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected cached overlap reuse hit")
	}
	if prev != expected {
		t.Fatalf("expected cached %+v, got %+v", expected, prev)
	}
	if calls != 1 {
		t.Fatalf("expected overlap loader to run once, got %d", calls)
	}
}

func TestReuseSources_ConflictingExactMatchesMiss(t *testing.T) {
	reuse := &reuseSources{
		exact: map[string][]PrevEntry{
			"file.txt": {
				{Size: 5, MtimeNS: 7, Fingerprint: "fp-a", SHA256: "sha-a"},
				{Size: 5, MtimeNS: 7, Fingerprint: "fp-b", SHA256: "sha-b"},
			},
		},
	}

	prev, ok, err := reuse.lookup("file.txt", 5, 7)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("expected conflicting exact candidates to miss, got %+v", prev)
	}
}

func TestInventoryTree_FirstScan(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"hello.txt":         "hello world",
		"sub/nested.go":     "package main",
		"sub/deep/file.log": "log entry",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()

	absRoot, _ := resolveAbsPath(scanDir)
	err := InventoryTree(ctx, db, scanDir)
	if err != nil {
		t.Fatalf("InventoryTree failed: %v", err)
	}

	// Verify entry count
	count := countEntries(t, db, absRoot)
	if count != 3 {
		t.Errorf("expected 3 entries, got %d", count)
	}

	// Verify scan count (should be exactly 1)
	scanCount := countScans(t, db, absRoot)
	if scanCount != 1 {
		t.Errorf("expected 1 scan, got %d", scanCount)
	}

	// Verify roots has an entry
	var currentScanID int64
	err = db.QueryRow("SELECT current_scan_id FROM roots WHERE root = ?", absRoot).Scan(&currentScanID)
	if err != nil {
		t.Fatalf("roots entry not found: %v", err)
	}
	if currentScanID == 0 {
		t.Error("current_scan_id should not be 0")
	}

	// Verify scan metadata
	var goos, goarch string
	err = db.QueryRow("SELECT goos, goarch FROM scans WHERE scan_id = ?", currentScanID).Scan(&goos, &goarch)
	if err != nil {
		t.Fatal(err)
	}
	if goos == "" || goarch == "" {
		t.Error("scan metadata goos/goarch should be populated")
	}

	// All entries should have state "ok" (first scan, no previous to reuse)
	for _, rel := range []string{"hello.txt", "sub/nested.go", "sub/deep/file.log"} {
		state := getEntryState(t, db, absRoot, rel)
		if state != "ok" {
			t.Errorf("entry %s: expected state ok, got %s", rel, state)
		}
	}
}

func TestInventoryTree_IdempotentRescan(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"a.txt": "alpha",
		"b.txt": "bravo",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)

	// First scan
	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	// Second scan (no changes)
	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	// All entries should be "reused"
	for _, rel := range []string{"a.txt", "b.txt"} {
		state := getEntryState(t, db, absRoot, rel)
		if state != "reused" {
			t.Errorf("entry %s: expected state reused, got %s", rel, state)
		}
	}

	// Only 1 scan should remain
	scanCount := countScans(t, db, absRoot)
	if scanCount != 1 {
		t.Errorf("expected 1 scan after rescan, got %d", scanCount)
	}
}

func TestInventoryTree_FileModified(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"unchanged.txt": "same content",
		"modified.txt":  "original content",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)

	// First scan
	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	// Modify one file (ensure mtime changes)
	time.Sleep(10 * time.Millisecond)
	modPath := filepath.Join(scanDir, "modified.txt")
	if err := os.WriteFile(modPath, []byte("new content!!!"), 0644); err != nil {
		t.Fatal(err)
	}

	// Second scan
	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	// unchanged.txt should be reused
	state := getEntryState(t, db, absRoot, "unchanged.txt")
	if state != "reused" {
		t.Errorf("unchanged.txt: expected reused, got %s", state)
	}

	// modified.txt should be ok (re-hashed)
	state = getEntryState(t, db, absRoot, "modified.txt")
	if state != "ok" {
		t.Errorf("modified.txt: expected ok, got %s", state)
	}
}

func TestInventoryTree_OverlapReuseParentFromChild(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	childDir := filepath.Join(scanDir, "raw")

	createTestTree(t, scanDir, map[string]string{
		"top.txt":             "top-level",
		"raw/photo.jpg":       "photo",
		"raw/nested/file.txt": "nested",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)

	if err := InventoryTree(ctx, db, childDir); err != nil {
		t.Fatal(err)
	}
	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	for _, rel := range []string{"raw/photo.jpg", "raw/nested/file.txt"} {
		if state := getEntryState(t, db, absRoot, rel); state != "reused" {
			t.Errorf("%s: expected reused from child overlap, got %s", rel, state)
		}
	}
	if state := getEntryState(t, db, absRoot, "top.txt"); state != "ok" {
		t.Errorf("top.txt: expected ok outside overlap, got %s", state)
	}
}

func TestInventoryTree_OverlapReuseChildFromParent(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	childDir := filepath.Join(scanDir, "raw")

	createTestTree(t, scanDir, map[string]string{
		"raw/photo.jpg":       "photo",
		"raw/nested/file.txt": "nested",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absChild, _ := resolveAbsPath(childDir)

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}
	if err := InventoryTree(ctx, db, childDir); err != nil {
		t.Fatal(err)
	}

	for _, rel := range []string{"photo.jpg", "nested/file.txt"} {
		if state := getEntryState(t, db, absChild, rel); state != "reused" {
			t.Errorf("%s: expected reused from parent overlap, got %s", rel, state)
		}
	}
}

func TestInventoryTree_OverlapReuseExactRootStaleOverlapFresh(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	childDir := filepath.Join(scanDir, "raw")

	createTestTree(t, scanDir, map[string]string{
		"top.txt":       "stable",
		"raw/photo.jpg": "old-version",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(filepath.Join(childDir, "photo.jpg"), []byte("new-version"), 0644); err != nil {
		t.Fatal(err)
	}

	if err := InventoryTree(ctx, db, childDir); err != nil {
		t.Fatal(err)
	}
	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	if state := getEntryState(t, db, absRoot, "raw/photo.jpg"); state != "reused" {
		t.Errorf("raw/photo.jpg: expected reused from fresher child overlap, got %s", state)
	}
	if state := getEntryState(t, db, absRoot, "top.txt"); state != "reused" {
		t.Errorf("top.txt: expected exact-root reuse, got %s", state)
	}
}

func TestInventoryTree_OverlapReuseConflictFallsBackToHash(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	targetDir := filepath.Join(scanDir, "raw")
	descendantDir := filepath.Join(targetDir, "nested")

	createTestTree(t, scanDir, map[string]string{
		"raw/nested/file.txt": "shared-content",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absAncestor, _ := resolveAbsPath(scanDir)
	absTarget, _ := resolveAbsPath(targetDir)
	absDescendant, _ := resolveAbsPath(descendantDir)

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}
	if err := InventoryTree(ctx, db, descendantDir); err != nil {
		t.Fatal(err)
	}

	updateEntryHashes(t, db, currentScanID(t, db, absAncestor), "raw/nested/file.txt", strings.Repeat("a", 64), strings.Repeat("b", 64))
	updateEntryHashes(t, db, currentScanID(t, db, absDescendant), "file.txt", strings.Repeat("c", 64), strings.Repeat("d", 64))

	if err := InventoryTree(ctx, db, targetDir); err != nil {
		t.Fatal(err)
	}

	if state := getEntryState(t, db, absTarget, "nested/file.txt"); state != "ok" {
		t.Errorf("nested/file.txt: expected ok after overlap conflict, got %s", state)
	}
}

func TestInventoryTree_OverlapReuseIgnoresUntrustedSourceEntries(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	childDir := filepath.Join(scanDir, "raw")

	createTestTree(t, scanDir, map[string]string{
		"raw/photo.jpg": "photo",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)
	absChild, _ := resolveAbsPath(childDir)

	if err := InventoryTree(ctx, db, childDir); err != nil {
		t.Fatal(err)
	}

	updateEntryResult(t, db, currentScanID(t, db, absChild), "photo.jpg", "error", "", "")

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	if state := getEntryState(t, db, absRoot, "raw/photo.jpg"); state != "ok" {
		t.Errorf("raw/photo.jpg: expected ok when overlap source is untrusted, got %s", state)
	}
}

func TestInventoryTree_OverlapReuseIgnoresNonOverlappingPrefixRoots(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	targetDir := filepath.Join(scanDir, "raw")
	otherDir := filepath.Join(scanDir, "raw-archive")

	createTestTree(t, scanDir, map[string]string{
		"raw/file.txt":         "shared",
		"raw-archive/file.txt": "shared",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absTarget, _ := resolveAbsPath(targetDir)

	if err := InventoryTree(ctx, db, otherDir); err != nil {
		t.Fatal(err)
	}
	if err := InventoryTree(ctx, db, targetDir); err != nil {
		t.Fatal(err)
	}

	if state := getEntryState(t, db, absTarget, "file.txt"); state != "ok" {
		t.Errorf("file.txt: expected ok for non-overlapping prefix root, got %s", state)
	}
}

func TestInventoryTree_FileDeleted(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"keep.txt":   "keeper",
		"remove.txt": "going away",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)

	// First scan
	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}
	if count := countEntries(t, db, absRoot); count != 2 {
		t.Fatalf("expected 2 entries after first scan, got %d", count)
	}

	// Delete a file
	os.Remove(filepath.Join(scanDir, "remove.txt"))

	// Second scan
	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	// Should have 1 entry now
	if count := countEntries(t, db, absRoot); count != 1 {
		t.Errorf("expected 1 entry after deletion, got %d", count)
	}
}

func TestInventoryTree_FileAdded(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"existing.txt": "already here",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)

	// First scan
	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}
	if count := countEntries(t, db, absRoot); count != 1 {
		t.Fatalf("expected 1 entry, got %d", count)
	}

	// Add a file
	createTestTree(t, scanDir, map[string]string{
		"newfile.txt": "brand new",
	})

	// Second scan
	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	if count := countEntries(t, db, absRoot); count != 2 {
		t.Errorf("expected 2 entries after addition, got %d", count)
	}

	state := getEntryState(t, db, absRoot, "newfile.txt")
	if state != "ok" {
		t.Errorf("newfile.txt: expected ok, got %s", state)
	}
}

func TestInventoryTree_EmptyDirectory(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "empty")
	os.MkdirAll(scanDir, 0755)

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	if count := countEntries(t, db, absRoot); count != 0 {
		t.Errorf("expected 0 entries for empty dir, got %d", count)
	}
}

func TestInventoryTree_ZeroByteFile(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"empty.txt": "",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	state := getEntryState(t, db, absRoot, "empty.txt")
	if state != "ok" {
		t.Errorf("empty.txt: expected ok, got %s", state)
	}
}

func TestInventoryTree_UnicodeFilenames(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"日本語.txt":     "japanese",
		"café.txt":    "french",
		"data/🎉.json": `{"party": true}`,
	})

	db := openTestDB(t, dir)
	ctx := context.Background()

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	absRoot, _ := resolveAbsPath(scanDir)
	if count := countEntries(t, db, absRoot); count != 3 {
		t.Errorf("expected 3 entries with unicode names, got %d", count)
	}
}

func TestInventoryTree_NoExtension(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"Makefile":   "all: build",
		".gitignore": "*.o",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	if count := countEntries(t, db, absRoot); count != 2 {
		t.Errorf("expected 2 entries, got %d", count)
	}
}

func TestInventoryTree_DBFileExcluded(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"real.txt": "real file",
	})

	// Put the DB inside the scan directory
	dbPath := filepath.Join(scanDir, "inventory.sympath")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	// Should only have 1 entry (real.txt), not the DB file
	if count := countEntries(t, db, absRoot); count != 1 {
		t.Errorf("expected 1 entry (DB excluded), got %d", count)
	}
}

func TestInventoryTree_OrphanedScanCleanup(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"file.txt": "content",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)

	// Manually create an orphaned "running" scan
	if err := PrepareLocalMachineDB(ctx, db, MachineIdentity{MachineID: "test-machine", Hostname: "test-host"}); err != nil {
		t.Fatal(err)
	}
	_, err := db.ExecContext(ctx,
		"INSERT INTO scans (machine_id, hostname, root, started_at, status) VALUES (?, ?, ?, ?, 'running')",
		"test-machine", "test-host", absRoot, time.Now().UnixNano(),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Run InventoryTree — should complete and clean up the orphan
	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	// Only 1 scan should remain (the completed one)
	scanCount := countScans(t, db, absRoot)
	if scanCount != 1 {
		t.Errorf("expected 1 scan (orphan cleaned up), got %d", scanCount)
	}

	// Verify the remaining scan is complete
	var status string
	err = db.QueryRow("SELECT status FROM scans WHERE root = ?", absRoot).Scan(&status)
	if err != nil {
		t.Fatal(err)
	}
	if status != "complete" {
		t.Errorf("expected scan status complete, got %s", status)
	}
}

func TestInventoryTree_ResumesInterruptedScanEntries(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"resume-ok.txt":     "resume from ok",
		"resume-reused.txt": "resume from reused",
		"skip-pending.txt":  "must hash again",
		"skip-error.txt":    "must hash again too",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)
	identity := MachineIdentity{MachineID: "resume-machine", Hostname: "resume-host"}
	if err := PrepareLocalMachineDB(ctx, db, identity); err != nil {
		t.Fatal(err)
	}

	interruptedScanID := insertManualScan(t, db, identity.MachineID, identity.Hostname, absRoot, "failed")
	for _, tc := range []struct {
		relPath     string
		state       string
		fingerprint string
		sha256      string
	}{
		{relPath: "resume-ok.txt", state: "ok", fingerprint: "resume-ok-fp", sha256: "resume-ok-sha"},
		{relPath: "resume-reused.txt", state: "reused", fingerprint: "resume-reused-fp", sha256: "resume-reused-sha"},
		{relPath: "skip-pending.txt", state: "pending", fingerprint: "pending-fp", sha256: "pending-sha"},
		{relPath: "skip-error.txt", state: "error", fingerprint: "error-fp", sha256: "error-sha"},
	} {
		size, mtimeNS := fileSizeAndMtime(t, filepath.Join(scanDir, tc.relPath))
		insertManualEntry(t, db, interruptedScanID, tc.relPath, size, mtimeNS, tc.state, tc.fingerprint, tc.sha256)
	}

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		relPath     string
		fingerprint string
		sha256      string
	}{
		{relPath: "resume-ok.txt", fingerprint: "resume-ok-fp", sha256: "resume-ok-sha"},
		{relPath: "resume-reused.txt", fingerprint: "resume-reused-fp", sha256: "resume-reused-sha"},
	} {
		if state := getEntryState(t, db, absRoot, tc.relPath); state != "reused" {
			t.Fatalf("%s: expected reused, got %s", tc.relPath, state)
		}
		fingerprint, sha256 := getEntryHashes(t, db, absRoot, tc.relPath)
		if fingerprint != tc.fingerprint || sha256 != tc.sha256 {
			t.Fatalf("%s: expected (%s, %s), got (%s, %s)", tc.relPath, tc.fingerprint, tc.sha256, fingerprint, sha256)
		}
	}

	for _, relPath := range []string{"skip-pending.txt", "skip-error.txt"} {
		if state := getEntryState(t, db, absRoot, relPath); state != "ok" {
			t.Fatalf("%s: expected ok after re-hash, got %s", relPath, state)
		}
	}

	if scanCount := countScans(t, db, absRoot); scanCount != 1 {
		t.Fatalf("expected interrupted scan cleanup to leave 1 scan, got %d", scanCount)
	}
}

func TestInventoryTree_DoesNotResumeRunningScanEntries(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"file.txt": "live running scan should not be reused",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)
	identity := MachineIdentity{MachineID: "resume-machine", Hostname: "resume-host"}
	if err := PrepareLocalMachineDB(ctx, db, identity); err != nil {
		t.Fatal(err)
	}

	runningScanID := insertManualScan(t, db, identity.MachineID, identity.Hostname, absRoot, "running")
	size, mtimeNS := fileSizeAndMtime(t, filepath.Join(scanDir, "file.txt"))
	insertManualEntry(t, db, runningScanID, "file.txt", size, mtimeNS, "ok", "running-fp", "running-sha")

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	if state := getEntryState(t, db, absRoot, "file.txt"); state != "ok" {
		t.Fatalf("file.txt: expected fresh hash instead of running-scan reuse, got %s", state)
	}
	fingerprint, sha256 := getEntryHashes(t, db, absRoot, "file.txt")
	if fingerprint == "running-fp" || sha256 == "running-sha" {
		t.Fatalf("file.txt: unexpectedly reused running scan hashes (%s, %s)", fingerprint, sha256)
	}
}

func TestInventoryTree_InterruptedScanFallsBackToAuthoritativeHashes(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"from-interrupted.txt":   "before change",
		"from-authoritative.txt": "still stable",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)
	identity := MachineIdentity{MachineID: "resume-machine", Hostname: "resume-host"}
	if err := PrepareLocalMachineDB(ctx, db, identity); err != nil {
		t.Fatal(err)
	}

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}
	authoritativeFingerprint, authoritativeSHA := getEntryHashes(t, db, absRoot, "from-authoritative.txt")

	time.Sleep(10 * time.Millisecond)
	if err := os.WriteFile(filepath.Join(scanDir, "from-interrupted.txt"), []byte("after change for interrupted reuse"), 0644); err != nil {
		t.Fatal(err)
	}

	interruptedSize, interruptedMtimeNS := fileSizeAndMtime(t, filepath.Join(scanDir, "from-interrupted.txt"))
	authoritativeSize, authoritativeMtimeNS := fileSizeAndMtime(t, filepath.Join(scanDir, "from-authoritative.txt"))

	interruptedScanID := insertManualScan(t, db, identity.MachineID, identity.Hostname, absRoot, "failed")
	insertManualEntry(t, db, interruptedScanID, "from-interrupted.txt", interruptedSize, interruptedMtimeNS, "ok", "resume-fp", "resume-sha")
	insertManualEntry(t, db, interruptedScanID, "from-authoritative.txt", authoritativeSize+1, authoritativeMtimeNS+1, "ok", "stale-fp", "stale-sha")

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	if state := getEntryState(t, db, absRoot, "from-interrupted.txt"); state != "reused" {
		t.Fatalf("from-interrupted.txt: expected reused, got %s", state)
	}
	fingerprint, sha256 := getEntryHashes(t, db, absRoot, "from-interrupted.txt")
	if fingerprint != "resume-fp" || sha256 != "resume-sha" {
		t.Fatalf("from-interrupted.txt: expected interrupted hashes, got (%s, %s)", fingerprint, sha256)
	}

	if state := getEntryState(t, db, absRoot, "from-authoritative.txt"); state != "reused" {
		t.Fatalf("from-authoritative.txt: expected reused, got %s", state)
	}
	fingerprint, sha256 = getEntryHashes(t, db, absRoot, "from-authoritative.txt")
	if fingerprint != authoritativeFingerprint || sha256 != authoritativeSHA {
		t.Fatalf("from-authoritative.txt: expected authoritative hashes (%s, %s), got (%s, %s)", authoritativeFingerprint, authoritativeSHA, fingerprint, sha256)
	}
	if fingerprint == "stale-fp" || sha256 == "stale-sha" {
		t.Fatal("from-authoritative.txt: unexpectedly reused stale interrupted hashes")
	}

	if scanCount := countScans(t, db, absRoot); scanCount != 1 {
		t.Fatalf("expected successful publish to keep one complete scan, got %d", scanCount)
	}
}

func TestInventoryTree_ConflictingExactReuseCandidatesTriggerFreshHash(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"file.txt": "authoritative content",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)
	identity := MachineIdentity{MachineID: "resume-machine", Hostname: "resume-host"}
	if err := PrepareLocalMachineDB(ctx, db, identity); err != nil {
		t.Fatal(err)
	}

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}
	authoritativeScanID := currentScanID(t, db, absRoot)
	updateEntryHashes(t, db, authoritativeScanID, "file.txt", "authoritative-fp", "authoritative-sha")

	size, mtimeNS := fileSizeAndMtime(t, filepath.Join(scanDir, "file.txt"))
	interruptedScanID := insertManualScan(t, db, identity.MachineID, identity.Hostname, absRoot, "failed")
	insertManualEntry(t, db, interruptedScanID, "file.txt", size, mtimeNS, "ok", "interrupted-fp", "interrupted-sha")

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	if state := getEntryState(t, db, absRoot, "file.txt"); state != "ok" {
		t.Fatalf("file.txt: expected fresh hash after conflicting exact candidates, got %s", state)
	}
	fingerprint, sha256 := getEntryHashes(t, db, absRoot, "file.txt")
	if fingerprint == "authoritative-fp" || sha256 == "authoritative-sha" || fingerprint == "interrupted-fp" || sha256 == "interrupted-sha" {
		t.Fatalf("file.txt: unexpectedly reused conflicting exact hashes (%s, %s)", fingerprint, sha256)
	}
}

func TestInventoryTree_CancellationMarksScanFailed(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"file.txt": "content",
	})

	db := openTestDB(t, dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	prevHook := afterCreateScanHook
	afterCreateScanHook = func(int64) {
		cancel()
	}
	t.Cleanup(func() {
		afterCreateScanHook = prevHook
	})

	absRoot, _ := resolveAbsPath(scanDir)
	err := InventoryTree(ctx, db, scanDir)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}

	var status string
	var finishedAt sql.NullInt64
	err = db.QueryRow("SELECT status, finished_at FROM scans WHERE root = ? ORDER BY scan_id DESC LIMIT 1", absRoot).Scan(&status, &finishedAt)
	if err != nil {
		t.Fatal(err)
	}
	if status != "failed" {
		t.Fatalf("expected failed scan after cancellation, got %s", status)
	}
	if !finishedAt.Valid {
		t.Fatal("expected failed scan to record finished_at")
	}
}

func TestInventoryTree_SHAAndFingerprintPopulated(t *testing.T) {
	dir := t.TempDir()
	scanDir := filepath.Join(dir, "data")
	os.MkdirAll(scanDir, 0755)

	createTestTree(t, scanDir, map[string]string{
		"test.txt": "test content for hash verification",
	})

	db := openTestDB(t, dir)
	ctx := context.Background()
	absRoot, _ := resolveAbsPath(scanDir)

	if err := InventoryTree(ctx, db, scanDir); err != nil {
		t.Fatal(err)
	}

	var sha, fp sql.NullString
	err := db.QueryRow(`
		SELECT e.sha256, e.fingerprint FROM entries e
		JOIN roots r ON r.current_scan_id = e.scan_id
		WHERE r.root = ? AND e.rel_path = 'test.txt'`, absRoot).Scan(&sha, &fp)
	if err != nil {
		t.Fatal(err)
	}

	if !sha.Valid || sha.String == "" {
		t.Error("sha256 should be populated")
	}
	if !fp.Valid || fp.String == "" {
		t.Error("fingerprint should be populated")
	}

	// SHA-256 should be a 64-char hex string
	if len(sha.String) != 64 {
		t.Errorf("sha256 should be 64 chars, got %d", len(sha.String))
	}
	if len(fp.String) != 64 {
		t.Errorf("fingerprint should be 64 chars, got %d", len(fp.String))
	}
}
