package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

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

func compareHasEntry(entries []fileEntry, relPath string) bool {
	for _, entry := range entries {
		if entry.RelPath == relPath {
			return true
		}
	}
	return false
}

func compareHasDiff(diffs []fileDiffPair, relPath string) bool {
	for _, diff := range diffs {
		if diff.RelPath == relPath {
			return true
		}
	}
	return false
}

func TestEntryCTEsBranching(t *testing.T) {
	tests := []struct {
		name           string
		leftPrefix     string
		rightPrefix    string
		ignoreCommonOS bool
		wantArgs       []any
		wantSQL        []string
		wantNoSQL      []string
	}{
		{
			name:           "empty prefixes keep raw paths",
			leftPrefix:     "",
			rightPrefix:    "",
			ignoreCommonOS: false,
			wantArgs:       []any{int64(11), int64(22)},
			wantSQL: []string{
				"SELECT rel_path AS join_path, rel_path, size, sha256",
			},
			wantNoSQL: []string{
				"x'FF'",
				"SUBSTR(rel_path, ?) AS join_path",
			},
		},
		{
			name:           "equal non-empty prefixes join on raw path",
			leftPrefix:     "sub",
			rightPrefix:    "sub",
			ignoreCommonOS: false,
			wantArgs:       []any{5, int64(11), "sub/", "sub/", 5, int64(22), "sub/", "sub/"},
			wantSQL: []string{
				"rel_path AS join_path,",
				"rel_path < ? || x'FF'",
			},
			wantNoSQL: []string{
				"SUBSTR(rel_path, ?) AS join_path",
			},
		},
		{
			name:           "different non-empty prefixes join on trimmed path",
			leftPrefix:     "left",
			rightPrefix:    "right",
			ignoreCommonOS: false,
			wantArgs:       []any{6, 6, int64(11), "left/", "left/", 7, 7, int64(22), "right/", "right/"},
			wantSQL: []string{
				"SUBSTR(rel_path, ?) AS join_path",
				"rel_path < ? || x'FF'",
			},
		},
		{
			name:           "ignore common os metadata filters both sides",
			leftPrefix:     "",
			rightPrefix:    "",
			ignoreCommonOS: true,
			wantArgs: []any{
				int64(11), ".ds_store", "thumbs.db", "ehthumbs.db", "desktop.ini", ".directory",
				int64(22), ".ds_store", "thumbs.db", "ehthumbs.db", "desktop.ini", ".directory",
			},
			wantSQL: []string{
				"LOWER(name) NOT IN (?, ?, ?, ?, ?)",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cte, args := entryCTEs(11, 22, tc.leftPrefix, tc.rightPrefix, tc.ignoreCommonOS)

			if !reflect.DeepEqual(args, tc.wantArgs) {
				t.Fatalf("unexpected args:\n got: %#v\nwant: %#v", args, tc.wantArgs)
			}
			for _, fragment := range tc.wantSQL {
				if !strings.Contains(cte, fragment) {
					t.Fatalf("expected SQL fragment %q in:\n%s", fragment, cte)
				}
			}
			for _, fragment := range tc.wantNoSQL {
				if strings.Contains(cte, fragment) {
					t.Fatalf("did not expect SQL fragment %q in:\n%s", fragment, cte)
				}
			}
		})
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

func TestRunUIWithIO_WaitsForActiveScanToReleaseDBGuard(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	stateDir, err := sympathStateDir()
	if err != nil {
		t.Fatal(err)
	}
	activeRoot := filepath.Clean(filepath.Join(string(filepath.Separator), "active", "root"))
	helperCmd, helperStdin := startActiveScanHelper(t, stateDir, "machine-a", activeRoot)
	defer func() {
		_ = helperStdin.Close()
		_ = helperCmd.Wait()
	}()

	resultCh := make(chan error, 1)
	go func() {
		resultCh <- runUIWithIO(nil, io.Discard, io.Discard)
	}()

	select {
	case err := <-resultCh:
		t.Fatalf("expected UI startup to wait for active scan, returned early with %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	if err := helperStdin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := helperCmd.Wait(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("expected missing-inventory error after active scan released")
		}
		if !strings.Contains(err.Error(), "run `sympath scan [ROOT]` first") {
			t.Fatalf("expected friendly scan-first error after release, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected UI startup to finish after active scan released")
	}
}

func TestRunUIWithIO_OpensDatabaseBeforeReleasingStartupProtection(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	startup := seedRunDBForScanTests(t)
	stateDir, err := sympathStateDir()
	if err != nil {
		t.Fatal(err)
	}

	sentinel := errors.New("stop after verified UI open")
	prevOpen := openUIReadOnlyDBForRunUI
	openUIReadOnlyDBForRunUI = func(ctx context.Context, dbPath string) (*sql.DB, error) {
		t.Helper()

		if dbPath != startup.DBPath {
			t.Fatalf("expected UI to open %q, got %q", startup.DBPath, dbPath)
		}
		state, err := probeSharedDBGuardDuringUIOpen(stateDir)
		if err != nil {
			t.Fatal(err)
		}
		if state != "blocked" {
			t.Fatalf("expected DB guard probe to block during UI open, got %q", state)
		}
		return nil, sentinel
	}
	t.Cleanup(func() { openUIReadOnlyDBForRunUI = prevOpen })

	err = runUIWithIO(nil, io.Discard, io.Discard)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error after probe, got %v", err)
	}
}

func TestRunUIWithIO_DefaultLogsOnlyMergeProgress(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	stateDir, err := sympathStateDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(stateDir, 0755); err != nil {
		t.Fatal(err)
	}
	rootA := filepath.Join(t.TempDir(), "ui-root-a")
	rootB := filepath.Join(t.TempDir(), "ui-root-b")
	dbA := filepath.Join(stateDir, "aaa.sympath")
	dbB := filepath.Join(stateDir, "bbb.sympath")

	writeTree(t, rootA, map[string]string{"a.txt": "alpha"})
	writeTree(t, rootB, map[string]string{"b.txt": "bravo"})
	scanIntoDB(t, dbA, rootA)
	scanIntoDB(t, dbB, rootB)

	sentinel := errors.New("stop after logged UI open")
	prevOpen := openUIReadOnlyDBForRunUI
	openUIReadOnlyDBForRunUI = func(ctx context.Context, dbPath string) (*sql.DB, error) {
		t.Helper()
		return nil, sentinel
	}
	t.Cleanup(func() { openUIReadOnlyDBForRunUI = prevOpen })

	var stderr bytes.Buffer
	err = runUIWithIO(nil, io.Discard, &stderr)
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error after opening log, got %v", err)
	}

	logOut := stderr.String()
	for _, want := range []string{
		"INFO: Merging: local 1/2: aaa.sympath",
		"INFO: Merging: local 2/2: bbb.sympath",
	} {
		if !strings.Contains(logOut, want) {
			t.Fatalf("expected log containing %q, got:\n%s", want, logOut)
		}
	}
	for _, unwanted := range []string{
		"Preparing UI startup",
		"Consolidating local databases for UI startup",
		"Opening database snapshot:",
		"Database:",
	} {
		if strings.Contains(logOut, unwanted) {
			t.Fatalf("did not expect setup chatter %q, got:\n%s", unwanted, logOut)
		}
	}
}

func TestHandleStatusUpdateAvailable(t *testing.T) {
	prev := version
	version = "v1.2.3"
	t.Cleanup(func() { version = prev })

	srv := &uiServer{
		updates: updateChecker{
			stateDir: func() (string, error) { return t.TempDir(), nil },
			now:      time.Now,
			fetchLatest: func(context.Context) (latestRelease, error) {
				return latestRelease{
					Version: "v1.2.4",
					URL:     "https://example.com/releases/v1.2.4",
				}, nil
			},
		},
	}

	req := httptest.NewRequest("GET", "/api/status", nil)
	rec := httptest.NewRecorder()
	srv.handleStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var status updateStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if !status.Supported || !status.UpdateAvailable || status.LatestVersion != "v1.2.4" {
		t.Fatalf("expected update-available status, got %+v", status)
	}
}

func TestHandleStatusUpToDate(t *testing.T) {
	prev := version
	version = "v1.2.3"
	t.Cleanup(func() { version = prev })

	srv := &uiServer{
		updates: updateChecker{
			stateDir: func() (string, error) { return t.TempDir(), nil },
			now:      time.Now,
			fetchLatest: func(context.Context) (latestRelease, error) {
				return latestRelease{
					Version: "v1.2.3",
					URL:     "https://example.com/releases/v1.2.3",
				}, nil
			},
		},
	}

	req := httptest.NewRequest("GET", "/api/status", nil)
	rec := httptest.NewRecorder()
	srv.handleStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var status updateStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if !status.Supported || status.UpdateAvailable || status.Unavailable {
		t.Fatalf("expected up-to-date status, got %+v", status)
	}
}

func TestHandleStatusUnsupportedBuild(t *testing.T) {
	prev := version
	version = "dev"
	t.Cleanup(func() { version = prev })

	srv := &uiServer{
		updates: updateChecker{
			stateDir: func() (string, error) { return t.TempDir(), nil },
			now:      time.Now,
			fetchLatest: func(context.Context) (latestRelease, error) {
				t.Fatal("expected unsupported build to skip live fetch")
				return latestRelease{}, nil
			},
		},
	}

	req := httptest.NewRequest("GET", "/api/status", nil)
	rec := httptest.NewRecorder()
	srv.handleStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var status updateStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.Supported || status.UpdateAvailable || status.Unavailable {
		t.Fatalf("expected unsupported-build status, got %+v", status)
	}
}

func TestHandleStatusUnavailable(t *testing.T) {
	prev := version
	version = "v1.2.3"
	t.Cleanup(func() { version = prev })

	srv := &uiServer{
		updates: updateChecker{
			stateDir: func() (string, error) { return t.TempDir(), nil },
			now:      time.Now,
			fetchLatest: func(context.Context) (latestRelease, error) {
				return latestRelease{}, context.DeadlineExceeded
			},
		},
	}

	req := httptest.NewRequest("GET", "/api/status", nil)
	rec := httptest.NewRecorder()
	srv.handleStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var status updateStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if !status.Supported || !status.Unavailable {
		t.Fatalf("expected unavailable status, got %+v", status)
	}
}

func TestEmbeddedUIIncludesStatusMountAndExplicitCompareAction(t *testing.T) {
	content, err := fs.ReadFile(uiStaticFS, "ui_static/index.html")
	if err != nil {
		t.Fatal(err)
	}

	html := string(content)
	if !strings.Contains(html, "id=\"app-status\"") {
		t.Fatalf("expected status mount in embedded UI, got:\n%s", html)
	}
	if !strings.Contains(html, "fetch('/api/status')") {
		t.Fatalf("expected status fetch path in embedded UI, got:\n%s", html)
	}
	if !strings.Contains(html, "id=\"compare-button\"") {
		t.Fatalf("expected explicit compare button in embedded UI, got:\n%s", html)
	}
	if !strings.Contains(html, "id=\"ignore-common-os\" checked") {
		t.Fatalf("expected checked common OS ignore toggle in embedded UI, got:\n%s", html)
	}
	if !strings.Contains(html, "compareButton.addEventListener('click', compare)") {
		t.Fatalf("expected compare button click handler in embedded UI, got:\n%s", html)
	}
	if !strings.Contains(html, "ignoreCommonOSBox.addEventListener('change', markCompareDirty)") {
		t.Fatalf("expected common OS ignore toggle to dirty compare state, got:\n%s", html)
	}
	if !strings.Contains(html, "params.set('ignore_common_os', '1')") {
		t.Fatalf("expected compare requests to include ignore_common_os flag, got:\n%s", html)
	}
	if strings.Contains(html, "setTimeout(compare, 400)") {
		t.Fatalf("expected compare to be manually triggered, found auto-compare debounce in embedded UI:\n%s", html)
	}
	if !strings.Contains(html, ".replace(/'/g, '&#39;')") {
		t.Fatalf("expected single-quote escaping in attribute helper, got:\n%s", html)
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

func TestHandleCompareWithDifferentPrefixes(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	leftScan := resolveUITestScanID(t, db, "machine-a", "/data/photos")
	rightScan := resolveUITestScanID(t, db, "machine-b", "/data/photos")

	insertUITestEntry(t, db, leftScan, "leftprefix/shared.txt", 111, "shared-hash")
	insertUITestEntry(t, db, rightScan, "rightprefix/shared.txt", 111, "shared-hash")
	insertUITestEntry(t, db, leftScan, "leftprefix/different.txt", 222, "left-diff")
	insertUITestEntry(t, db, rightScan, "rightprefix/different.txt", 333, "right-diff")
	insertUITestEntry(t, db, leftScan, "leftprefix/left-only.txt", 444, "left-only")
	insertUITestEntry(t, db, rightScan, "rightprefix/right-only.txt", 555, "right-only")

	req := httptest.NewRequest("GET", "/api/compare?left_machine=machine-a&left_root=/data/photos&right_machine=machine-b&right_root=/data/photos&left_prefix=leftprefix&right_prefix=rightprefix", nil)
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
		t.Fatalf("expected 1 identical across different prefixes, got %d", result.IdenticalCount)
	}
	if len(result.LeftOnly) != 1 || result.LeftOnly[0].RelPath != "left-only.txt" {
		t.Fatalf("expected left-only.txt as the only left-only file, got %v", result.LeftOnly)
	}
	if len(result.RightOnly) != 1 || result.RightOnly[0].RelPath != "right-only.txt" {
		t.Fatalf("expected right-only.txt as the only right-only file, got %v", result.RightOnly)
	}
	if len(result.Different) != 1 {
		t.Fatalf("expected 1 different file across different prefixes, got %d", len(result.Different))
	}
	if result.Different[0].RelPath != "different.txt" {
		t.Fatalf("expected different.txt with prefixes stripped, got %q", result.Different[0].RelPath)
	}
}

func TestHandleCompareWithUnicodePrefixes(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	leftScan := resolveUITestScanID(t, db, "machine-a", "/data/photos")
	rightScan := resolveUITestScanID(t, db, "machine-b", "/data/photos")

	insertUITestEntry(t, db, leftScan, "Música/shared.txt", 111, "shared-hash")
	insertUITestEntry(t, db, rightScan, "Éxitos/shared.txt", 111, "shared-hash")
	insertUITestEntry(t, db, leftScan, "Música/different.txt", 222, "left-diff")
	insertUITestEntry(t, db, rightScan, "Éxitos/different.txt", 333, "right-diff")
	insertUITestEntry(t, db, leftScan, "Música/left-only.txt", 444, "left-only")
	insertUITestEntry(t, db, rightScan, "Éxitos/right-only.txt", 555, "right-only")

	req := httptest.NewRequest("GET", "/api/compare?left_machine=machine-a&left_root=/data/photos&right_machine=machine-b&right_root=/data/photos&left_prefix=Música&right_prefix=Éxitos", nil)
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
		t.Fatalf("expected 1 identical across Unicode prefixes, got %d", result.IdenticalCount)
	}
	if len(result.LeftOnly) != 1 || result.LeftOnly[0].RelPath != "left-only.txt" {
		t.Fatalf("expected left-only.txt as the only left-only file, got %v", result.LeftOnly)
	}
	if len(result.RightOnly) != 1 || result.RightOnly[0].RelPath != "right-only.txt" {
		t.Fatalf("expected right-only.txt as the only right-only file, got %v", result.RightOnly)
	}
	if len(result.Different) != 1 || result.Different[0].RelPath != "different.txt" {
		t.Fatalf("expected different.txt as the only different file, got %v", result.Different)
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

func TestHandleCompareWithoutIgnoreCommonOSIncludesMetadataFiles(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	leftScan := resolveUITestScanID(t, db, "machine-a", "/data/photos")
	rightScan := resolveUITestScanID(t, db, "machine-b", "/data/photos")

	insertUITestEntry(t, db, leftScan, ".DS_Store", 10, "left-ds")
	insertUITestEntry(t, db, rightScan, "Thumbs.db", 20, "right-thumbs")
	insertUITestEntry(t, db, leftScan, "Desktop.ini", 30, "left-desktop")
	insertUITestEntry(t, db, rightScan, "Desktop.ini", 40, "right-desktop")

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

	if !compareHasEntry(result.LeftOnly, ".DS_Store") {
		t.Fatalf("expected .DS_Store in left-only results, got %v", result.LeftOnly)
	}
	if !compareHasEntry(result.RightOnly, "Thumbs.db") {
		t.Fatalf("expected Thumbs.db in right-only results, got %v", result.RightOnly)
	}
	if !compareHasDiff(result.Different, "Desktop.ini") {
		t.Fatalf("expected Desktop.ini in different results, got %v", result.Different)
	}
}

func TestHandleCompareIgnoreCommonOSFiltersPathResults(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	leftScan := resolveUITestScanID(t, db, "machine-a", "/data/photos")
	rightScan := resolveUITestScanID(t, db, "machine-b", "/data/photos")

	insertUITestEntry(t, db, leftScan, ".DS_Store", 10, "left-ds")
	insertUITestEntry(t, db, rightScan, "THUMBS.DB", 20, "right-thumbs")
	insertUITestEntry(t, db, leftScan, "Desktop.ini", 30, "left-desktop")
	insertUITestEntry(t, db, rightScan, "Desktop.ini", 40, "right-desktop")

	req := httptest.NewRequest("GET", "/api/compare?left_machine=machine-a&left_root=/data/photos&right_machine=machine-b&right_root=/data/photos&ignore_common_os=1", nil)
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
		t.Fatalf("expected baseline identical count with ignored metadata, got %d", result.IdenticalCount)
	}
	if len(result.LeftOnly) != 2 {
		t.Fatalf("expected baseline left-only count with ignored metadata, got %d: %v", len(result.LeftOnly), result.LeftOnly)
	}
	if len(result.RightOnly) != 2 {
		t.Fatalf("expected baseline right-only count with ignored metadata, got %d: %v", len(result.RightOnly), result.RightOnly)
	}
	if len(result.Different) != 1 {
		t.Fatalf("expected baseline different count with ignored metadata, got %d: %v", len(result.Different), result.Different)
	}
	if compareHasEntry(result.LeftOnly, ".DS_Store") || compareHasEntry(result.RightOnly, "THUMBS.DB") || compareHasDiff(result.Different, "Desktop.ini") {
		t.Fatalf("expected ignored metadata to be removed, got left=%v right=%v diff=%v", result.LeftOnly, result.RightOnly, result.Different)
	}
}

func TestHandleCompareIgnoreCommonOSFiltersContentResults(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	leftScan := resolveUITestScanID(t, db, "machine-a", "/data/photos")
	rightScan := resolveUITestScanID(t, db, "machine-b", "/data/photos")

	insertUITestEntry(t, db, leftScan, ".DS_Store", 10, "shared-ds")
	insertUITestEntry(t, db, rightScan, ".DS_Store", 10, "shared-ds")
	insertUITestEntry(t, db, leftScan, "Thumbs.db", 20, "left-thumbs")
	insertUITestEntry(t, db, rightScan, "Desktop.ini", 30, "right-desktop")

	req := httptest.NewRequest("GET", "/api/compare?left_machine=machine-a&left_root=/data/photos&right_machine=machine-b&right_root=/data/photos&by_content=1&ignore_common_os=1", nil)
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
		t.Fatalf("expected baseline identical-by-content count with ignored metadata, got %d", result.IdenticalCount)
	}
	if len(result.LeftOnly) != 3 {
		t.Fatalf("expected baseline left-only-by-content count with ignored metadata, got %d: %v", len(result.LeftOnly), result.LeftOnly)
	}
	if len(result.RightOnly) != 3 {
		t.Fatalf("expected baseline right-only-by-content count with ignored metadata, got %d: %v", len(result.RightOnly), result.RightOnly)
	}
	if len(result.Different) != 0 {
		t.Fatalf("expected no different bucket in content mode, got %v", result.Different)
	}
	if compareHasEntry(result.LeftOnly, "Thumbs.db") || compareHasEntry(result.RightOnly, "Desktop.ini") {
		t.Fatalf("expected ignored metadata to be removed from content compare, got left=%v right=%v", result.LeftOnly, result.RightOnly)
	}
}

func TestHandleCompareIgnoreCommonOSFiltersPrefixResults(t *testing.T) {
	db := setupUITestDB(t)
	srv := &uiServer{db: db}

	leftScan := resolveUITestScanID(t, db, "machine-a", "/data/photos")
	rightScan := resolveUITestScanID(t, db, "machine-b", "/data/photos")

	insertUITestEntry(t, db, leftScan, "sub/.directory", 10, "left-directory")
	insertUITestEntry(t, db, rightScan, "sub/Desktop.ini", 20, "right-desktop")
	insertUITestEntry(t, db, leftScan, "sub/Thumbs.db", 30, "left-thumbs")
	insertUITestEntry(t, db, rightScan, "sub/Thumbs.db", 40, "right-thumbs")

	req := httptest.NewRequest("GET", "/api/compare?left_machine=machine-a&left_root=/data/photos&right_machine=machine-b&right_root=/data/photos&left_prefix=sub&right_prefix=sub&ignore_common_os=1", nil)
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
		t.Fatalf("expected baseline identical count in sub/ with ignored metadata, got %d", result.IdenticalCount)
	}
	if len(result.LeftOnly) != 1 || result.LeftOnly[0].RelPath != "only-a.txt" {
		t.Fatalf("expected only-a.txt as the sole left-only result in sub/, got %v", result.LeftOnly)
	}
	if len(result.RightOnly) != 1 || result.RightOnly[0].RelPath != "only-b.txt" {
		t.Fatalf("expected only-b.txt as the sole right-only result in sub/, got %v", result.RightOnly)
	}
	if len(result.Different) != 0 {
		t.Fatalf("expected no different results in sub/ after ignoring metadata, got %v", result.Different)
	}
}

func probeSharedDBGuardDuringUIOpen(stateDir string) (string, error) {
	cmd := exec.Command(os.Args[0], "-test.run=TestScanLockHelperProcess")
	cmd.Env = append(os.Environ(),
		scanLockHelperEnv+"=1",
		"SCAN_LOCK_STATE_DIR="+stateDir,
		scanLockHelperModeEnv+"=probe-shared-db-guard-blocked",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("probe shared db guard: %w (%s)", err, strings.TrimSpace(string(output)))
	}
	return strings.TrimSpace(string(output)), nil
}
