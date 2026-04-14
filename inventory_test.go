package inventory

import (
	"context"
	"database/sql"
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

func TestReuseSources_DoesNotLoadOverlapWhenExactMatches(t *testing.T) {
	called := false
	expected := PrevEntry{
		Size:        5,
		MtimeNS:     7,
		Fingerprint: "fp",
		SHA256:      "sha",
	}
	reuse := &reuseSources{
		exact: map[string]PrevEntry{"file.txt": expected},
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
		exact: map[string]PrevEntry{},
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
