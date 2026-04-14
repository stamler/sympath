package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
