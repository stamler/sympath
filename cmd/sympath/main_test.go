package main

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	inventory "sympath"
)

const crashRecoveryHelperEnv = "GO_WANT_CRASH_RECOVERY_HELPER"

func TestRunScan_RejectsDBFlag(t *testing.T) {
	err := runScanWithIO([]string{"--db", "ignored.sympath"}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("expected --db to be rejected")
	}
	if !strings.Contains(err.Error(), "flag provided but not defined: -db") {
		t.Fatalf("expected unknown flag error, got %v", err)
	}
}

func TestPrintUsage_DoesNotMentionDBFlag(t *testing.T) {
	var buf bytes.Buffer
	printUsage(&buf)

	out := buf.String()
	if strings.Contains(out, "--db") {
		t.Fatalf("expected usage to omit --db, got:\n%s", out)
	}
	if !strings.Contains(out, "~/.sympath/*.sympath") {
		t.Fatalf("expected usage to mention consolidation, got:\n%s", out)
	}
	if !strings.Contains(out, "--verbose") {
		t.Fatalf("expected usage to mention --verbose, got:\n%s", out)
	}
	if !strings.Contains(out, "sympath version") {
		t.Fatalf("expected usage to mention version command, got:\n%s", out)
	}
	if !strings.Contains(out, "sympath update") {
		t.Fatalf("expected usage to mention update command, got:\n%s", out)
	}
	if !strings.Contains(out, "sympath update-check") {
		t.Fatalf("expected usage to mention update-check command, got:\n%s", out)
	}
}

func TestRunScan_VerboseReportsDatabaseSetup(t *testing.T) {
	home := t.TempDir()
	root := filepath.Join(t.TempDir(), "scan-root")
	t.Setenv("HOME", home)

	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runScanWithIO([]string{"--verbose", root}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}

	errOut := stderr.String()
	if !strings.Contains(errOut, "Created database directory:") {
		t.Fatalf("expected directory creation message, got:\n%s", errOut)
	}
	if !strings.Contains(errOut, "Created new database path:") {
		t.Fatalf("expected database creation message, got:\n%s", errOut)
	}
	if !strings.Contains(errOut, "Using database:") {
		t.Fatalf("expected final database message, got:\n%s", errOut)
	}
	if !strings.Contains(stdout.String(), "Scan complete") {
		t.Fatalf("expected scan summary on stdout, got:\n%s", stdout.String())
	}
}

func TestRunScan_VerboseReportsInterruptedResumeSource(t *testing.T) {
	home := t.TempDir()
	root := filepath.Join(t.TempDir(), "scan-root")
	t.Setenv("HOME", home)

	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(root, "file.txt")
	if err := os.WriteFile(filePath, []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}

	startup, err := resolveRunDBPath(context.Background(), fakeRemoteTransport{}, verboseLogger{})
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", startup.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := inventory.PrepareLocalMachineDB(context.Background(), db, startup.Identity); err != nil {
		t.Fatal(err)
	}

	normalizedRoot, err := normalizePath(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.Exec(
		"INSERT INTO scans (machine_id, hostname, root, started_at, status) VALUES (?, ?, ?, ?, 'failed')",
		startup.Identity.MachineID, startup.Identity.Hostname, normalizedRoot, time.Now().UnixNano(),
	)
	if err != nil {
		t.Fatal(err)
	}
	scanID, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO entries (scan_id, rel_path, name, ext, size, mtime_ns, fingerprint, sha256, state)
		VALUES (?, 'file.txt', 'file.txt', '.txt', ?, ?, 'resume-fp', 'resume-sha', 'ok')
	`, scanID, info.Size(), info.ModTime().UnixNano()); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runScanWithIO([]string{"--verbose", root}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}

	errOut := stderr.String()
	if !strings.Contains(errOut, "Interrupted resume source: scan") {
		t.Fatalf("expected interrupted resume log, got:\n%s", errOut)
	}
	if !strings.Contains(errOut, "(1 reusable entries)") {
		t.Fatalf("expected reusable entry count in log, got:\n%s", errOut)
	}
}

func TestRunScan_VerboseAdoptsStaleRunningScanAsResumeSource(t *testing.T) {
	home := t.TempDir()
	root := filepath.Join(t.TempDir(), "scan-root")
	t.Setenv("HOME", home)

	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	filePath := filepath.Join(root, "file.txt")
	if err := os.WriteFile(filePath, []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}

	startup, err := resolveRunDBPath(context.Background(), fakeRemoteTransport{}, verboseLogger{})
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", startup.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := inventory.PrepareLocalMachineDB(context.Background(), db, startup.Identity); err != nil {
		t.Fatal(err)
	}

	normalizedRoot, err := normalizePath(root)
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.Exec(
		"INSERT INTO scans (machine_id, hostname, root, started_at, status) VALUES (?, ?, ?, ?, 'running')",
		startup.Identity.MachineID, startup.Identity.Hostname, normalizedRoot, time.Now().UnixNano(),
	)
	if err != nil {
		t.Fatal(err)
	}
	scanID, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		INSERT INTO entries (scan_id, rel_path, name, ext, size, mtime_ns, fingerprint, sha256, state)
		VALUES (?, 'file.txt', 'file.txt', '.txt', ?, ?, 'running-fp', 'running-sha', 'ok')
	`, scanID, info.Size(), info.ModTime().UnixNano()); err != nil {
		t.Fatal(err)
	}

	stateDir, err := sympathStateDir()
	if err != nil {
		t.Fatal(err)
	}
	if err := ensureSympathDir(stateDir, verboseLogger{}); err != nil {
		t.Fatal(err)
	}
	locksDir, err := ensureScanLocksDir(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := scanRootLockPath(locksDir, startup.Identity.MachineID, normalizedRoot)
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeScanLockMetadata(lockFile, scanLockMetadata{
		MachineID: startup.Identity.MachineID,
		Root:      normalizedRoot,
	}); err != nil {
		_ = lockFile.Close()
		t.Fatal(err)
	}
	if err := lockFile.Close(); err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runScanWithIO([]string{"--verbose", root}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}

	errOut := stderr.String()
	if !strings.Contains(errOut, "Interrupted resume source: scan") {
		t.Fatalf("expected stale running scan to be adopted as a resume source, got:\n%s", errOut)
	}
	if !strings.Contains(errOut, "(1 reusable entries)") {
		t.Fatalf("expected reusable entry count in log, got:\n%s", errOut)
	}
	if !strings.Contains(stdout.String(), "Scan complete") {
		t.Fatalf("expected scan summary on stdout, got:\n%s", stdout.String())
	}

	var state string
	var fingerprint string
	var sha256 string
	if err := db.QueryRow(`
		SELECT e.state, e.fingerprint, e.sha256
		FROM entries e
		JOIN roots r ON r.current_scan_id = e.scan_id
		WHERE r.machine_id = ? AND r.root = ? AND e.rel_path = 'file.txt'
	`, startup.Identity.MachineID, normalizedRoot).Scan(&state, &fingerprint, &sha256); err != nil {
		t.Fatal(err)
	}
	if state != "reused" {
		t.Fatalf("expected reused entry state, got %q", state)
	}
	if fingerprint != "running-fp" || sha256 != "running-sha" {
		t.Fatalf("expected stale running scan hashes to be reused, got (%q, %q)", fingerprint, sha256)
	}
}

func TestRunScan_UsesInjectedScanContext(t *testing.T) {
	home := t.TempDir()
	root := filepath.Join(t.TempDir(), "scan-root")
	t.Setenv("HOME", home)

	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	prevNewScanContext := newScanContext
	prevInventoryScan := inventoryScanWithProgress
	t.Cleanup(func() {
		newScanContext = prevNewScanContext
		inventoryScanWithProgress = prevInventoryScan
		cancel()
	})

	newScanContext = func() (context.Context, context.CancelFunc) {
		return ctx, cancel
	}

	called := false
	inventoryScanWithProgress = func(scanCtx context.Context, db *sql.DB, root string, progress *inventory.ScanProgress) error {
		called = true
		cancel()
		<-scanCtx.Done()
		return scanCtx.Err()
	}

	err := runScanWithIO([]string{root}, io.Discard, io.Discard)
	if !called {
		t.Fatal("expected injected scan function to be called")
	}
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("expected context cancellation error, got %v", err)
	}
}

func TestRunScan_AllowsConcurrentDisjointRootWhileAnotherScanIsActive(t *testing.T) {
	home := t.TempDir()
	rootA := filepath.Join(t.TempDir(), "root-a")
	rootB := filepath.Join(t.TempDir(), "root-b")
	t.Setenv("HOME", home)

	if err := os.MkdirAll(rootA, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(rootB, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootA, "a.txt"), []byte("alpha"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootB, "b.txt"), []byte("beta"), 0644); err != nil {
		t.Fatal(err)
	}

	startup := seedRunDBForScanTests(t)
	stateDir, err := sympathStateDir()
	if err != nil {
		t.Fatal(err)
	}
	normalizedRootA, err := normalizePath(rootA)
	if err != nil {
		t.Fatal(err)
	}
	normalizedRootB, err := normalizePath(rootB)
	if err != nil {
		t.Fatal(err)
	}

	helperCmd, helperStdin := startActiveScanHelper(t, stateDir, startup.Identity.MachineID, normalizedRootA)
	defer func() {
		_ = helperStdin.Close()
		_ = helperCmd.Wait()
	}()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runScanWithIO([]string{"--verbose", rootB}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}

	errOut := stderr.String()
	if !strings.Contains(errOut, "Using existing database while another scan is active: "+startup.DBPath) {
		t.Fatalf("expected concurrent disjoint scan to reuse active database, got:\n%s", errOut)
	}
	if !strings.Contains(stdout.String(), "Scan complete") {
		t.Fatalf("expected scan summary on stdout, got:\n%s", stdout.String())
	}

	db, err := sql.Open("sqlite", startup.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var rootCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM roots
		WHERE machine_id = ? AND root = ?
	`, startup.Identity.MachineID, normalizedRootB).Scan(&rootCount); err != nil {
		t.Fatal(err)
	}
	if rootCount != 1 {
		t.Fatalf("expected disjoint concurrent scan to publish one root row, got %d", rootCount)
	}

	localDBs, err := listSympathDBs(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(localDBs) != 1 || localDBs[0] != startup.DBPath {
		t.Fatalf("expected active scan to preserve the existing database path %q, got %v", startup.DBPath, localDBs)
	}
}

func TestRunScan_RejectsConcurrentOverlappingRootAtCLILevel(t *testing.T) {
	home := t.TempDir()
	root := filepath.Join(t.TempDir(), "root")
	overlapRoot := filepath.Join(root, "nested")
	t.Setenv("HOME", home)

	if err := os.MkdirAll(overlapRoot, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a.txt"), []byte("alpha"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(overlapRoot, "b.txt"), []byte("beta"), 0644); err != nil {
		t.Fatal(err)
	}

	startup := seedRunDBForScanTests(t)
	stateDir, err := sympathStateDir()
	if err != nil {
		t.Fatal(err)
	}
	normalizedRoot, err := normalizePath(root)
	if err != nil {
		t.Fatal(err)
	}
	normalizedOverlapRoot, err := normalizePath(overlapRoot)
	if err != nil {
		t.Fatal(err)
	}

	helperCmd, helperStdin := startActiveScanHelper(t, stateDir, startup.Identity.MachineID, normalizedRoot)
	defer func() {
		_ = helperStdin.Close()
		_ = helperCmd.Wait()
	}()

	err = runScanWithIO([]string{overlapRoot}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("expected overlapping root scan to be rejected")
	}
	if !strings.Contains(err.Error(), "overlapping root") {
		t.Fatalf("expected overlapping-root error, got %v", err)
	}

	db, err := sql.Open("sqlite", startup.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var scanCount int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM scans
		WHERE machine_id = ? AND root = ?
	`, startup.Identity.MachineID, normalizedOverlapRoot).Scan(&scanCount); err != nil {
		t.Fatal(err)
	}
	if scanCount != 0 {
		t.Fatalf("expected overlapping root rejection before scan creation, got %d scan rows", scanCount)
	}
}

func TestRunScan_HardKillRecoveryReusesOrphanedRunningScan(t *testing.T) {
	home := t.TempDir()
	root := filepath.Join(t.TempDir(), "scan-root")
	t.Setenv("HOME", home)

	if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "file.txt"), []byte("content"), 0644); err != nil {
		t.Fatal(err)
	}

	startup := seedRunDBForScanTests(t)
	normalizedRoot, err := normalizePath(root)
	if err != nil {
		t.Fatal(err)
	}

	helperCmd, helperStdin := startCrashRecoveryHelper(t, root)
	defer func() {
		_ = helperStdin.Close()
		_ = helperCmd.Wait()
	}()

	if err := helperCmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := helperCmd.Wait(); err == nil {
		t.Fatal("expected helper to exit via hard kill")
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runScanWithIO([]string{"--verbose", root}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}

	errOut := stderr.String()
	if !strings.Contains(errOut, "Interrupted resume source: scan") {
		t.Fatalf("expected orphaned running scan to be resumed, got:\n%s", errOut)
	}
	if !strings.Contains(stdout.String(), "Scan complete") {
		t.Fatalf("expected scan summary on stdout, got:\n%s", stdout.String())
	}

	db, err := sql.Open("sqlite", startup.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var state string
	var fingerprint string
	var sha256 string
	if err := db.QueryRow(`
		SELECT e.state, e.fingerprint, e.sha256
		FROM entries e
		JOIN roots r ON r.current_scan_id = e.scan_id
		WHERE r.machine_id = ? AND r.root = ? AND e.rel_path = 'file.txt'
	`, startup.Identity.MachineID, normalizedRoot).Scan(&state, &fingerprint, &sha256); err != nil {
		t.Fatal(err)
	}
	if state != "reused" {
		t.Fatalf("expected reused entry state after hard-kill recovery, got %q", state)
	}
	if fingerprint != "crash-fp" || sha256 != "crash-sha" {
		t.Fatalf("expected hard-killed scan hashes to be reused, got (%q, %q)", fingerprint, sha256)
	}
}

func TestRunWithIO_VersionCommandPrintsBuildVersion(t *testing.T) {
	prev := version
	version = "v1.2.3"
	t.Cleanup(func() { version = prev })

	var stdout bytes.Buffer
	if err := runWithIO([]string{"version"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}

	if got := stdout.String(); got != "v1.2.3\n" {
		t.Fatalf("expected version output, got %q", got)
	}
}

func TestRunWithIO_VersionFlagPrintsBuildVersion(t *testing.T) {
	prev := version
	version = "v9.9.9"
	t.Cleanup(func() { version = prev })

	var stdout bytes.Buffer
	if err := runWithIO([]string{"--version"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}

	if got := stdout.String(); got != "v9.9.9\n" {
		t.Fatalf("expected version output, got %q", got)
	}
}

func TestRunWithIO_VersionCommandPrintsUpdateNoticeOnStderr(t *testing.T) {
	prev := version
	version = "v1.2.3"
	t.Cleanup(func() { version = prev })

	home := t.TempDir()
	t.Setenv("HOME", home)
	writeUpdateCacheForTest(t, filepath.Join(home, ".sympath"), updateCache{
		CheckedAt:     time.Now().UTC(),
		LatestVersion: "v1.2.4",
		ReleaseURL:    "https://example.com/releases/v1.2.4",
	})

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithIO([]string{"version"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}

	if got := stdout.String(); got != "v1.2.3\n" {
		t.Fatalf("expected version stdout only, got %q", got)
	}
	if !strings.Contains(stderr.String(), "Update available: v1.2.4 (current v1.2.3)") {
		t.Fatalf("expected update notice on stderr, got %q", stderr.String())
	}
}

func TestRunWithIO_VersionCommandSkipsNoticeWhenUpToDate(t *testing.T) {
	prev := version
	version = "v1.2.3"
	t.Cleanup(func() { version = prev })

	home := t.TempDir()
	t.Setenv("HOME", home)
	writeUpdateCacheForTest(t, filepath.Join(home, ".sympath"), updateCache{
		CheckedAt:     time.Now().UTC(),
		LatestVersion: "v1.2.3",
		ReleaseURL:    "https://example.com/releases/v1.2.3",
	})

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithIO([]string{"version"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}

	if got := stdout.String(); got != "v1.2.3\n" {
		t.Fatalf("expected version stdout only, got %q", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr notice, got %q", stderr.String())
	}
}

func TestRunWithIO_VersionCommandSkipsNoticeForUnsupportedBuild(t *testing.T) {
	prev := version
	version = "dev"
	t.Cleanup(func() { version = prev })

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithIO([]string{"version"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}

	if got := stdout.String(); got != "dev\n" {
		t.Fatalf("expected version stdout only, got %q", got)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr notice, got %q", stderr.String())
	}
}

func TestRunWithIO_UpdateCheckCommandSuccess(t *testing.T) {
	prevVersion := version
	version = "v1.2.3"
	t.Cleanup(func() { version = prevVersion })

	prevChecker := newUpdateChecker
	newUpdateChecker = func() updateChecker {
		return updateChecker{
			stateDir: func() (string, error) { return t.TempDir(), nil },
			now:      time.Now,
			fetchLatest: func(context.Context) (latestRelease, error) {
				return latestRelease{
					Version: "v1.2.5",
					URL:     "https://example.com/releases/v1.2.5",
				}, nil
			},
		}
	}
	t.Cleanup(func() { newUpdateChecker = prevChecker })

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithIO([]string{"update-check"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}

	out := stdout.String()
	if !strings.Contains(out, "Update available: v1.2.5 (current v1.2.3)") {
		t.Fatalf("expected update-check output, got %q", out)
	}
	if !strings.Contains(out, "https://example.com/releases/v1.2.5") {
		t.Fatalf("expected release URL in output, got %q", out)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr output, got %q", stderr.String())
	}
}

func TestRunWithIO_UpdateCheckCommandFailure(t *testing.T) {
	prevVersion := version
	version = "v1.2.3"
	t.Cleanup(func() { version = prevVersion })

	prevChecker := newUpdateChecker
	newUpdateChecker = func() updateChecker {
		return updateChecker{
			stateDir: func() (string, error) { return t.TempDir(), nil },
			now:      time.Now,
			fetchLatest: func(context.Context) (latestRelease, error) {
				return latestRelease{}, errors.New("network unavailable")
			},
		}
	}
	t.Cleanup(func() { newUpdateChecker = prevChecker })

	var stdout bytes.Buffer
	err := runWithIO([]string{"update-check"}, &stdout, io.Discard)
	if err == nil {
		t.Fatal("expected update-check command to fail")
	}
	if !strings.Contains(err.Error(), "live check failed") {
		t.Fatalf("expected live check failure, got %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("expected no stdout on failure, got %q", stdout.String())
	}
}

func TestRunWithIO_UpdateCommandUpToDate(t *testing.T) {
	prevVersion := version
	version = "v1.2.3"
	t.Cleanup(func() { version = prevVersion })

	prevChecker := newUpdateChecker
	newUpdateChecker = func() updateChecker {
		return updateChecker{
			stateDir: func() (string, error) { return t.TempDir(), nil },
			now:      time.Now,
			fetchLatest: func(context.Context) (latestRelease, error) {
				return latestRelease{
					Version: "v1.2.3",
					URL:     "https://example.com/releases/v1.2.3",
				}, nil
			},
		}
	}
	t.Cleanup(func() { newUpdateChecker = prevChecker })

	prevInstall := installManagedRelease
	installManagedRelease = func(context.Context, string) (installedRelease, error) {
		t.Fatal("expected no install when already up to date")
		return installedRelease{}, nil
	}
	t.Cleanup(func() { installManagedRelease = prevInstall })

	var stdout bytes.Buffer
	if err := runWithIO([]string{"update"}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}

	if got := stdout.String(); !strings.Contains(got, "v1.2.3 is up to date") {
		t.Fatalf("expected up-to-date message, got %q", got)
	}
}

func TestRunWithIO_UpdateCommandInstallsAvailableRelease(t *testing.T) {
	prevVersion := version
	version = "v1.2.3"
	t.Cleanup(func() { version = prevVersion })

	stateDir := t.TempDir()
	prevChecker := newUpdateChecker
	newUpdateChecker = func() updateChecker {
		return updateChecker{
			stateDir: func() (string, error) { return stateDir, nil },
			now:      time.Now,
			fetchLatest: func(context.Context) (latestRelease, error) {
				return latestRelease{
					Version: "v1.2.5",
					URL:     "https://example.com/releases/v1.2.5",
				}, nil
			},
		}
	}
	t.Cleanup(func() { newUpdateChecker = prevChecker })

	capturedVersion := ""
	prevInstall := installManagedRelease
	installManagedRelease = func(_ context.Context, targetVersion string) (installedRelease, error) {
		capturedVersion = targetVersion
		return installedRelease{
			Version:    "v1.2.5",
			TargetPath: "/tmp/sympath",
			ReleaseURL: "https://example.com/releases/v1.2.5",
		}, nil
	}
	t.Cleanup(func() { installManagedRelease = prevInstall })

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := runWithIO([]string{"update"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}

	if capturedVersion != "v1.2.5" {
		t.Fatalf("expected installer target version v1.2.5, got %q", capturedVersion)
	}
	out := stdout.String()
	if !strings.Contains(out, "Updating sympath from v1.2.3 to v1.2.5") {
		t.Fatalf("expected preflight output, got %q", out)
	}
	if !strings.Contains(out, "Installed sympath v1.2.5 to /tmp/sympath") {
		t.Fatalf("expected install success output, got %q", out)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr output, got %q", stderr.String())
	}

	cachePath := filepath.Join(stateDir, updateCheckCacheName)
	cacheData, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("expected cache file after update, got %v", err)
	}
	if !strings.Contains(string(cacheData), "v1.2.5") {
		t.Fatalf("expected cache to mention installed version, got %q", string(cacheData))
	}
}

func TestRunWithIO_UpdateCommandUsesRequestedInstallVersion(t *testing.T) {
	prevVersion := version
	version = "v1.2.3"
	t.Cleanup(func() { version = prevVersion })
	t.Setenv(installVersionEnv, "v1.2.4")

	prevChecker := newUpdateChecker
	newUpdateChecker = func() updateChecker {
		return updateChecker{
			stateDir: func() (string, error) { return t.TempDir(), nil },
			now:      time.Now,
			fetchLatest: func(context.Context) (latestRelease, error) {
				t.Fatal("expected pinned install version to skip live release check")
				return latestRelease{}, nil
			},
		}
	}
	t.Cleanup(func() { newUpdateChecker = prevChecker })

	capturedVersion := ""
	prevInstall := installManagedRelease
	installManagedRelease = func(_ context.Context, targetVersion string) (installedRelease, error) {
		capturedVersion = targetVersion
		return installedRelease{
			Version:    "v1.2.4",
			TargetPath: "/tmp/sympath",
			ReleaseURL: "https://example.com/releases/v1.2.4",
		}, nil
	}
	t.Cleanup(func() { installManagedRelease = prevInstall })

	if err := runWithIO([]string{"update"}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	if capturedVersion != "v1.2.4" {
		t.Fatalf("expected explicit install version v1.2.4, got %q", capturedVersion)
	}
}

func TestRunWithIO_UpdateCommandPinnedVersionDoesNotFailOnLiveCheck(t *testing.T) {
	prevVersion := version
	version = "v1.2.3"
	t.Cleanup(func() { version = prevVersion })
	t.Setenv(installVersionEnv, "v1.2.4")

	prevChecker := newUpdateChecker
	newUpdateChecker = func() updateChecker {
		return updateChecker{
			stateDir: func() (string, error) { return t.TempDir(), nil },
			now:      time.Now,
			fetchLatest: func(context.Context) (latestRelease, error) {
				return latestRelease{}, errors.New("network unavailable")
			},
		}
	}
	t.Cleanup(func() { newUpdateChecker = prevChecker })

	prevInstall := installManagedRelease
	installManagedRelease = func(_ context.Context, targetVersion string) (installedRelease, error) {
		return installedRelease{
			Version:    targetVersion,
			TargetPath: "/tmp/sympath",
			ReleaseURL: "https://example.com/releases/" + targetVersion,
		}, nil
	}
	t.Cleanup(func() { installManagedRelease = prevInstall })

	if err := runWithIO([]string{"update"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("expected pinned update to proceed without live check, got %v", err)
	}
}

func TestRunWithIO_UpdateCommandRejectsUnsupportedBuild(t *testing.T) {
	prevVersion := version
	version = "dev"
	t.Cleanup(func() { version = prevVersion })

	prevInstall := installManagedRelease
	installManagedRelease = func(context.Context, string) (installedRelease, error) {
		t.Fatal("expected no install for unsupported build")
		return installedRelease{}, nil
	}
	t.Cleanup(func() { installManagedRelease = prevInstall })

	err := runWithIO([]string{"update"}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("expected unsupported build to fail")
	}
	if !strings.Contains(err.Error(), "sympath update is unavailable for build dev") {
		t.Fatalf("expected unsupported-build error, got %v", err)
	}
}

func TestRunWithIO_UpdateCommandReturnsInstallerFailure(t *testing.T) {
	prevVersion := version
	version = "v1.2.3"
	t.Cleanup(func() { version = prevVersion })

	prevChecker := newUpdateChecker
	newUpdateChecker = func() updateChecker {
		return updateChecker{
			stateDir: func() (string, error) { return t.TempDir(), nil },
			now:      time.Now,
			fetchLatest: func(context.Context) (latestRelease, error) {
				return latestRelease{
					Version: "v1.2.5",
					URL:     "https://example.com/releases/v1.2.5",
				}, nil
			},
		}
	}
	t.Cleanup(func() { newUpdateChecker = prevChecker })

	prevInstall := installManagedRelease
	installManagedRelease = func(context.Context, string) (installedRelease, error) {
		return installedRelease{}, errors.New("checksum verification failed")
	}
	t.Cleanup(func() { installManagedRelease = prevInstall })

	err := runWithIO([]string{"update"}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("expected update to fail")
	}
	if !strings.Contains(err.Error(), "checksum verification failed") {
		t.Fatalf("expected installer failure, got %v", err)
	}
}

func TestEmitAutoUpdateNoticeNilUpdatesIsNoOp(t *testing.T) {
	var stderr bytes.Buffer
	if err := emitAutoUpdateNotice(&stderr, nil); err != nil {
		t.Fatal(err)
	}
	if stderr.Len() != 0 {
		t.Fatalf("expected no stderr output, got %q", stderr.String())
	}
}

func writeUpdateCacheForTest(t *testing.T, stateDir string, cache updateCache) {
	t.Helper()

	checker := updateChecker{
		stateDir: func() (string, error) { return stateDir, nil },
		now:      time.Now,
	}
	if err := checker.writeCache(cache); err != nil {
		t.Fatal(err)
	}
}

func seedRunDBForScanTests(t *testing.T) startupState {
	t.Helper()

	startup, err := resolveRunDBPath(context.Background(), fakeRemoteTransport{}, verboseLogger{})
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite", startup.DBPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := inventory.PrepareLocalMachineDB(context.Background(), db, startup.Identity); err != nil {
		t.Fatal(err)
	}

	return startup
}

func TestCrashRecoveryHelperProcess(t *testing.T) {
	if os.Getenv(crashRecoveryHelperEnv) != "1" {
		return
	}

	root := os.Getenv("CRASH_HELPER_ROOT")

	stateDir, err := sympathStateDir()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	startupLock, err := acquireScanStartupLock(context.Background(), stateDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	exclusiveDBGuard, err := acquireScanDBGuardLockExclusive(context.Background(), stateDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(4)
	}
	startup, err := resolveRunDBPath(context.Background(), fakeRemoteTransport{}, verboseLogger{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(5)
	}
	normalizedRoot, err := normalizePath(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(6)
	}
	rootLock, err := acquireScanRootLock(stateDir, startup.Identity.MachineID, normalizedRoot)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(7)
	}

	db, err := sql.Open("sqlite", startup.DBPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(8)
	}
	if err := inventory.PrepareLocalMachineDB(context.Background(), db, startup.Identity); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(9)
	}
	if err := exclusiveDBGuard.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(10)
	}
	dbGuard, err := acquireScanDBGuardLockShared(context.Background(), stateDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(11)
	}
	if err := startupLock.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(12)
	}

	info, err := os.Stat(filepath.Join(root, "file.txt"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(13)
	}

	result, err := db.Exec(
		"INSERT INTO scans (machine_id, hostname, root, started_at, status) VALUES (?, ?, ?, ?, 'running')",
		startup.Identity.MachineID, startup.Identity.Hostname, normalizedRoot, time.Now().UnixNano(),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(14)
	}
	scanID, err := result.LastInsertId()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(15)
	}
	if _, err := db.Exec(`
		INSERT INTO entries (scan_id, rel_path, name, ext, size, mtime_ns, fingerprint, sha256, state)
		VALUES (?, 'file.txt', 'file.txt', '.txt', ?, ?, 'crash-fp', 'crash-sha', 'ok')
	`, scanID, info.Size(), info.ModTime().UnixNano()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(16)
	}

	fmt.Fprintln(os.Stdout, "ready")
	_ = os.Stdout.Sync()
	_, _ = io.ReadAll(os.Stdin)
	_ = db.Close()
	_ = dbGuard.Close()
	_ = rootLock.Close()
	os.Exit(0)
}

func startCrashRecoveryHelper(t *testing.T, root string) (*exec.Cmd, io.WriteCloser) {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=TestCrashRecoveryHelperProcess")
	cmd.Env = append(os.Environ(),
		crashRecoveryHelperEnv+"=1",
		"CRASH_HELPER_ROOT="+root,
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	lineBuf := make([]byte, 6)
	if _, err := io.ReadFull(stdout, lineBuf); err != nil {
		_ = stdin.Close()
		_ = cmd.Wait()
		t.Fatalf("wait for crash helper readiness: %v (stderr: %s)", err, stderr.String())
	}
	if string(lineBuf) != "ready\n" {
		_ = stdin.Close()
		_ = cmd.Wait()
		t.Fatalf("unexpected crash helper output %q (stderr: %s)", string(lineBuf), stderr.String())
	}

	return cmd, stdin
}
