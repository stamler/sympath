package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	inventory "sympath"

	_ "modernc.org/sqlite"
)

type fakeRemoteTransport struct{}

func (fakeRemoteTransport) LocateRemoteDB(context.Context, string) (string, error) {
	return "", errors.New("unexpected remote lookup")
}

func (fakeRemoteTransport) FetchRemoteDB(context.Context, string, string, string) error {
	return errors.New("unexpected remote fetch")
}

type mapRemoteTransport struct {
	paths map[string]string
}

func (m mapRemoteTransport) LocateRemoteDB(_ context.Context, target string) (string, error) {
	path, ok := m.paths[target]
	if !ok {
		return "", fmt.Errorf("unknown remote %s", target)
	}
	return path, nil
}

func (m mapRemoteTransport) FetchRemoteDB(_ context.Context, _ string, remotePath, localPath string) error {
	data, err := os.ReadFile(remotePath)
	if err != nil {
		return err
	}
	return os.WriteFile(localPath, data, 0644)
}

func TestConsolidateSympathDir_NoFilesReturnsRandomPath(t *testing.T) {
	dir := t.TempDir()

	path, err := consolidateSympathDir(context.Background(), dir, testMachineIdentity(), fakeRemoteTransport{}, verboseLogger{})
	if err != nil {
		t.Fatal(err)
	}

	if filepath.Dir(path) != dir {
		t.Fatalf("expected path in %q, got %q", dir, path)
	}
	if filepath.Ext(path) != ".sympath" {
		t.Fatalf("expected .sympath extension, got %q", path)
	}
}

func TestConsolidateSympathDir_OneFileReusesPath(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(t.TempDir(), "root")
	dbPath := filepath.Join(dir, "only.sympath")

	writeTree(t, root, map[string]string{"file.txt": "content"})
	scanIntoDB(t, dbPath, root)

	path, err := consolidateSympathDir(context.Background(), dir, testMachineIdentity(), fakeRemoteTransport{}, verboseLogger{})
	if err != nil {
		t.Fatal(err)
	}

	if path != dbPath {
		t.Fatalf("expected %q, got %q", dbPath, path)
	}
}

func TestConsolidateSympathDir_MergesDifferentRootsAndDeletesArtifacts(t *testing.T) {
	dir := t.TempDir()
	rootA := filepath.Join(t.TempDir(), "rootA")
	rootB := filepath.Join(t.TempDir(), "rootB")
	dbA := filepath.Join(dir, "aaa.sympath")
	dbB := filepath.Join(dir, "bbb.sympath")

	writeTree(t, rootA, map[string]string{"a.txt": "alpha"})
	writeTree(t, rootB, map[string]string{"b.txt": "bravo"})
	scanIntoDB(t, dbA, rootA)
	scanIntoDB(t, dbB, rootB)

	for _, artifact := range []string{dbB + "-wal", dbB + "-shm", dbB + "-journal"} {
		if err := os.WriteFile(artifact, []byte("sidecar"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	var logs bytes.Buffer
	path, err := consolidateSympathDir(context.Background(), dir, testMachineIdentity(), fakeRemoteTransport{}, newVerboseLogger(&logs, true))
	if err != nil {
		t.Fatal(err)
	}

	if path == dbA || path == dbB {
		t.Fatalf("expected a freshly written survivor path, got %q", path)
	}
	if _, err := os.Stat(dbB); !os.IsNotExist(err) {
		t.Fatalf("expected source database %q to be deleted, stat err=%v", dbB, err)
	}
	for _, artifact := range []string{dbB + "-wal", dbB + "-shm", dbB + "-journal"} {
		if _, err := os.Stat(artifact); !os.IsNotExist(err) {
			t.Fatalf("expected artifact %q to be deleted, stat err=%v", artifact, err)
		}
	}
	logOut := logs.String()
	if !strings.Contains(logOut, "Consolidating 2 local database file(s) and 0 fetched remote file(s)") {
		t.Fatalf("expected consolidation message, got:\n%s", logOut)
	}
	if !strings.Contains(logOut, "Consolidated database: ") {
		t.Fatalf("expected final database message, got:\n%s", logOut)
	}
	if !strings.Contains(logOut, "Removed merged artifact: "+dbB) {
		t.Fatalf("expected artifact removal message, got:\n%s", logOut)
	}

	db := openInventoryDB(t, path)
	defer db.Close()

	normA, err := normalizePath(rootA)
	if err != nil {
		t.Fatal(err)
	}
	normB, err := normalizePath(rootB)
	if err != nil {
		t.Fatal(err)
	}

	if got := countRoots(t, db); got != 2 {
		t.Fatalf("expected 2 roots after consolidation, got %d", got)
	}
	if got := countEntriesForRoot(t, db, normA); got != 1 {
		t.Fatalf("expected 1 entry for rootA, got %d", got)
	}
	if got := countEntriesForRoot(t, db, normB); got != 1 {
		t.Fatalf("expected 1 entry for rootB, got %d", got)
	}
}

func TestConsolidateSympathDir_NewestScanWinsForSameRoot(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(t.TempDir(), "shared")
	dbA := filepath.Join(dir, "aaa.sympath")
	dbB := filepath.Join(dir, "bbb.sympath")

	writeTree(t, root, map[string]string{"file.txt": "old"})
	scanIntoDB(t, dbA, root)

	time.Sleep(10 * time.Millisecond)
	writeTree(t, root, map[string]string{"file.txt": "newer content"})
	scanIntoDB(t, dbB, root)

	path, err := consolidateSympathDir(context.Background(), dir, testMachineIdentity(), fakeRemoteTransport{}, verboseLogger{})
	if err != nil {
		t.Fatal(err)
	}
	if path == dbA || path == dbB {
		t.Fatalf("expected consolidation to produce a fresh survivor path, got %q", path)
	}

	db := openInventoryDB(t, path)
	defer db.Close()

	normRoot, err := normalizePath(root)
	if err != nil {
		t.Fatal(err)
	}

	if got := countRoots(t, db); got != 1 {
		t.Fatalf("expected 1 root after consolidation, got %d", got)
	}
	if got := entrySizeForRoot(t, db, normRoot, "file.txt"); got != int64(len("newer content")) {
		t.Fatalf("expected newer entry size %d, got %d", len("newer content"), got)
	}
}

func TestConsolidateSympathDir_CorruptDBLeavesFilesIntact(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(t.TempDir(), "root")
	dbA := filepath.Join(dir, "aaa.sympath")
	dbB := filepath.Join(dir, "bbb.sympath")

	writeTree(t, root, map[string]string{"file.txt": "content"})
	scanIntoDB(t, dbA, root)
	if err := os.WriteFile(dbB, []byte("not a sqlite database"), 0644); err != nil {
		t.Fatal(err)
	}

	if _, err := consolidateSympathDir(context.Background(), dir, testMachineIdentity(), fakeRemoteTransport{}, verboseLogger{}); err == nil {
		t.Fatal("expected consolidation to fail for corrupt database")
	}

	if _, err := os.Stat(dbA); err != nil {
		t.Fatalf("expected survivor candidate %q to remain, got %v", dbA, err)
	}
	if _, err := os.Stat(dbB); err != nil {
		t.Fatalf("expected corrupt source %q to remain, got %v", dbB, err)
	}

	db := openInventoryDB(t, dbA)
	defer db.Close()
	if got := countRoots(t, db); got != 1 {
		t.Fatalf("expected original survivor contents to remain, got %d roots", got)
	}
}

func TestResolveRunDBPath_SeedsRemotesFileWithDocumentation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	startup, err := resolveRunDBPath(context.Background(), fakeRemoteTransport{}, verboseLogger{})
	if err != nil {
		t.Fatal(err)
	}

	remotesPath := filepath.Join(home, ".sympath", remotesFileName)
	data, err := os.ReadFile(remotesPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)

	if !strings.Contains(text, "One remote target per line.") {
		t.Fatalf("expected remotes file to document one-target-per-line syntax, got:\n%s", text)
	}
	if !strings.Contains(text, "WHAT YOU MAY NOT INCLUDE") {
		t.Fatalf("expected remotes file to include the detailed restrictions block, got:\n%s", text)
	}
	if !strings.Contains(text, "If a remote fetch fails, sympath warns and continues") {
		t.Fatalf("expected remotes file to document fetch failure behavior, got:\n%s", text)
	}
	if !strings.Contains(text, "Each remote is fetched as the SSH login account in this file.") {
		t.Fatalf("expected remotes file to document remote account behavior, got:\n%s", text)
	}
	if _, err := os.Stat(filepath.Join(home, ".sympath", machineIDFileName)); err != nil {
		t.Fatalf("expected machine-id file to be created, got %v", err)
	}
	if filepath.Dir(startup.DBPath) != filepath.Join(home, ".sympath") {
		t.Fatalf("expected DB path inside ~/.sympath, got %q", startup.DBPath)
	}
}

func TestLoadRemoteTargets_AcceptsRootAccountTargets(t *testing.T) {
	path := filepath.Join(t.TempDir(), remotesFileName)
	if err := os.WriteFile(path, []byte("root@fileserver\n"), 0644); err != nil {
		t.Fatal(err)
	}

	remotes, err := loadRemoteTargets(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(remotes) != 1 || remotes[0] != "root@fileserver" {
		t.Fatalf("expected root account target to parse, got %#v", remotes)
	}
}

func TestConsolidateSympathDir_FetchesAndImportsRemoteDB(t *testing.T) {
	dir := t.TempDir()
	localRoot := filepath.Join(t.TempDir(), "local-root")
	remoteRoot := filepath.Join(t.TempDir(), "remote-root")
	localDB := filepath.Join(dir, "local.sympath")
	remoteDB := filepath.Join(t.TempDir(), "remote.sympath")

	writeTree(t, localRoot, map[string]string{"local.txt": "local"})
	writeTree(t, remoteRoot, map[string]string{"remote.txt": "remote"})
	scanIntoDBWithIdentity(t, localDB, localRoot, testMachineIdentity())
	scanIntoDBWithIdentity(t, remoteDB, remoteRoot, inventory.MachineIdentity{
		MachineID: "remote-machine",
		Hostname:  "remote-host",
	})

	if err := os.WriteFile(filepath.Join(dir, remotesFileName), []byte("remote-a\n"), 0644); err != nil {
		t.Fatal(err)
	}

	path, err := consolidateSympathDir(
		context.Background(),
		dir,
		testMachineIdentity(),
		mapRemoteTransport{paths: map[string]string{"remote-a": remoteDB}},
		verboseLogger{},
	)
	if err != nil {
		t.Fatal(err)
	}

	db := openInventoryDB(t, path)
	defer db.Close()

	if got := countRoots(t, db); got != 2 {
		t.Fatalf("expected 2 roots after importing remote data, got %d", got)
	}
	if got := countDistinctMachineIDs(t, db); got != 2 {
		t.Fatalf("expected 2 machine IDs after importing remote data, got %d", got)
	}
}

func TestConsolidateSympathDir_LogsRemoteFetchAndMergeProgress(t *testing.T) {
	dir := t.TempDir()
	localRoot := filepath.Join(t.TempDir(), "local-root")
	remoteRoot := filepath.Join(t.TempDir(), "remote-root")
	localDB := filepath.Join(dir, "local.sympath")
	remoteDB := filepath.Join(t.TempDir(), "remote.sympath")

	writeTree(t, localRoot, map[string]string{"local.txt": "local"})
	writeTree(t, remoteRoot, map[string]string{"remote.txt": "remote"})
	scanIntoDBWithIdentity(t, localDB, localRoot, testMachineIdentity())
	scanIntoDBWithIdentity(t, remoteDB, remoteRoot, inventory.MachineIdentity{
		MachineID: "remote-machine",
		Hostname:  "remote-host",
	})

	if err := os.WriteFile(filepath.Join(dir, remotesFileName), []byte("remote-a\n"), 0644); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	if _, err := consolidateSympathDir(
		context.Background(),
		dir,
		testMachineIdentity(),
		mapRemoteTransport{paths: map[string]string{"remote-a": remoteDB}},
		newScanLogger(&logs, false),
	); err != nil {
		t.Fatal(err)
	}

	logOut := logs.String()
	for _, want := range []string{
		"INFO: Fetching 1/1: remote-a",
		"INFO: Merging: local 1/2: local.sympath",
		"INFO: Merging: remote 2/2: remote-a",
	} {
		if !strings.Contains(logOut, want) {
			t.Fatalf("expected log containing %q, got:\n%s", want, logOut)
		}
	}
	for _, unwanted := range []string{
		"Starting remote fetch",
		"Fetched remote database",
		"Remote fetch complete",
		"Consolidated database:",
	} {
		if strings.Contains(logOut, unwanted) {
			t.Fatalf("did not expect setup/completion log %q, got:\n%s", unwanted, logOut)
		}
	}
}

func TestConsolidateSympathDir_LocalOnlyTransportKeepsSkipNarrationQuietByDefault(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(t.TempDir(), "local-root")
	dbPath := filepath.Join(dir, "local.sympath")

	writeTree(t, root, map[string]string{"local.txt": "local"})
	scanIntoDB(t, dbPath, root)

	if err := os.WriteFile(filepath.Join(dir, remotesFileName), []byte("remote-a\n"), 0644); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	path, err := consolidateSympathDir(
		context.Background(),
		dir,
		testMachineIdentity(),
		localOnlyTransport{},
		newUILogger(&logs),
	)
	if err != nil {
		t.Fatal(err)
	}
	if path != dbPath {
		t.Fatalf("expected UI-mode consolidation to reuse %q, got %q", dbPath, path)
	}

	logOut := logs.String()
	if strings.Contains(logOut, "Skipping fetch for 1 configured remote database(s): UI startup uses local databases only") {
		t.Fatalf("expected UI skip narration to stay quiet by default, got:\n%s", logOut)
	}
	if strings.Contains(logOut, "WARN: Remote remote-a skipped") {
		t.Fatalf("expected UI mode to avoid per-remote warnings, got:\n%s", logOut)
	}
}

func TestConsolidateSympathDir_RemovingRemotePurgesItsMachineData(t *testing.T) {
	dir := t.TempDir()
	localRoot := filepath.Join(t.TempDir(), "local-root")
	remoteRoot := filepath.Join(t.TempDir(), "remote-root")
	localDB := filepath.Join(dir, "local.sympath")
	remoteDB := filepath.Join(t.TempDir(), "remote.sympath")

	writeTree(t, localRoot, map[string]string{"local.txt": "local"})
	writeTree(t, remoteRoot, map[string]string{"remote.txt": "remote"})
	scanIntoDBWithIdentity(t, localDB, localRoot, testMachineIdentity())
	scanIntoDBWithIdentity(t, remoteDB, remoteRoot, inventory.MachineIdentity{
		MachineID: "remote-machine",
		Hostname:  "remote-host",
	})

	remotesPath := filepath.Join(dir, remotesFileName)
	if err := os.WriteFile(remotesPath, []byte("remote-a\n"), 0644); err != nil {
		t.Fatal(err)
	}
	firstPath, err := consolidateSympathDir(
		context.Background(),
		dir,
		testMachineIdentity(),
		mapRemoteTransport{paths: map[string]string{"remote-a": remoteDB}},
		verboseLogger{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, remotesStateName)); err != nil {
		t.Fatalf("expected remote state file after initial import, got %v", err)
	}

	if err := os.WriteFile(remotesPath, []byte(remotesFileTemplate), 0644); err != nil {
		t.Fatal(err)
	}
	secondPath, err := consolidateSympathDir(
		context.Background(),
		dir,
		testMachineIdentity(),
		fakeRemoteTransport{},
		verboseLogger{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if firstPath == secondPath {
		t.Fatalf("expected a fresh survivor path after purge, got %q", secondPath)
	}

	db := openInventoryDB(t, secondPath)
	defer db.Close()

	if got := countRoots(t, db); got != 1 {
		t.Fatalf("expected only the local root after removing the remote, got %d", got)
	}
	if got := countDistinctMachineIDs(t, db); got != 1 {
		t.Fatalf("expected only the local machine after removing the remote, got %d", got)
	}
}

func TestBuildRemoteShellCommand_QuotesScriptForSSH(t *testing.T) {
	script := `printf '%s\n' "path with spaces; and quote ' inside"`

	output, err := exec.Command("sh", "-c", buildRemoteShellCommand(script)).CombinedOutput()
	if err != nil {
		t.Fatalf("expected quoted command to execute, got %v with output %q", err, string(output))
	}

	if got, want := string(output), "path with spaces; and quote ' inside\n"; got != want {
		t.Fatalf("expected output %q, got %q", want, got)
	}
}

func TestBuildRemoteLookupArgs_UsesConnectTimeout(t *testing.T) {
	got := buildRemoteLookupArgs("remote-a")

	want := []string{
		"-o", "ConnectTimeout=10",
		"remote-a",
		buildRemoteShellCommand(remoteDBLookupScript),
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d args, got %d: %#v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("arg %d: expected %q, got %q", i, want[i], got[i])
		}
	}
}

func TestBuildRemoteCopyArgs_UsesConnectTimeoutWithoutExtraFlags(t *testing.T) {
	got := buildRemoteCopyArgs("remote-a", "/home/dean/.sympath/current.sympath", "/tmp/fetch.sympath")

	want := []string{
		"-q",
		"-o", "ConnectTimeout=10",
		"remote-a:/home/dean/.sympath/current.sympath",
		"/tmp/fetch.sympath",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d args, got %d: %#v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("arg %d: expected %q, got %q", i, want[i], got[i])
		}
	}
}

func TestParseRemoteDBPath(t *testing.T) {
	tests := []struct {
		name    string
		output  string
		want    string
		wantErr string
	}{
		{
			name:   "single path",
			output: "/root/.sympath/current.sympath\n",
			want:   "/root/.sympath/current.sympath",
		},
		{
			name:    "empty output",
			output:  "",
			wantErr: "no remote ~/.sympath/*.sympath file found",
		},
		{
			name: "mixed output",
			output: strings.Join([]string{
				"Last login: Mon Apr 13 21:23:23 2026",
				"/root/.sympath/older.sympath",
				"warning banner",
				"/root/.sympath/newest.sympath",
				"",
			}, "\n"),
			want: "/root/.sympath/newest.sympath",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseRemoteDBPath([]byte(tc.output))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error %q, got nil", tc.wantErr)
				}
				if err.Error() != tc.wantErr {
					t.Fatalf("expected error %q, got %q", tc.wantErr, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected %q, got %q", tc.want, got)
			}
		})
	}
}

func TestFormatCommandError_PreservesCommandOutput(t *testing.T) {
	output, err := exec.Command("sh", "-c", "printf 'banner\\n'; printf >&2 'permission denied\\n'; exit 255").CombinedOutput()
	if err == nil {
		t.Fatal("expected command failure")
	}

	got := formatCommandError("locate remote database via ssh", err, output).Error()
	if !strings.Contains(got, "locate remote database via ssh") {
		t.Fatalf("expected action prefix in %q", got)
	}
	if !strings.Contains(got, "exit status 255") {
		t.Fatalf("expected exit status in %q", got)
	}
	if !strings.Contains(got, "permission denied") {
		t.Fatalf("expected stderr text in %q", got)
	}
}

func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()

	for relPath, content := range files {
		path := filepath.Join(root, filepath.FromSlash(relPath))
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
}

func scanIntoDB(t *testing.T, dbPath, root string) {
	t.Helper()
	scanIntoDBWithIdentity(t, dbPath, root, testMachineIdentity())
}

func scanIntoDBWithIdentity(t *testing.T, dbPath, root string, identity inventory.MachineIdentity) {
	t.Helper()

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := inventory.PrepareLocalMachineDB(context.Background(), db, identity); err != nil {
		t.Fatal(err)
	}

	if err := inventory.InventoryTree(context.Background(), db, root); err != nil {
		t.Fatalf("InventoryTree(%q, %q) failed: %v", dbPath, root, err)
	}
}

func openInventoryDB(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if err := inventory.ConfigureConnection(context.Background(), db); err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func countRoots(t *testing.T, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM roots").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func countDistinctMachineIDs(t *testing.T, db *sql.DB) int {
	t.Helper()

	var count int
	if err := db.QueryRow("SELECT COUNT(DISTINCT machine_id) FROM roots").Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func countEntriesForRoot(t *testing.T, db *sql.DB, root string) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`
		SELECT COUNT(*) FROM entries e
		JOIN roots r ON r.current_scan_id = e.scan_id
		WHERE r.root = ?
	`, root).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func entrySizeForRoot(t *testing.T, db *sql.DB, root, relPath string) int64 {
	t.Helper()

	var size int64
	if err := db.QueryRow(`
		SELECT e.size FROM entries e
		JOIN roots r ON r.current_scan_id = e.scan_id
		WHERE r.root = ? AND e.rel_path = ?
	`, root, relPath).Scan(&size); err != nil {
		t.Fatal(err)
	}
	return size
}

func testMachineIdentity() inventory.MachineIdentity {
	return inventory.MachineIdentity{
		MachineID: "local-test-machine",
		Hostname:  "local-test-host",
	}
}
