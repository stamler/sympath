package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
