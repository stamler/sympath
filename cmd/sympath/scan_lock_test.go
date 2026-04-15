// Package main tests the scan locking layer by exercising it across real
// processes rather than only within a single Go test goroutine.
//
// The production design depends on kernel-managed file locks, so the important
// behaviors are inherently process-oriented: one process must be able to hold a
// root lock while another process attempts startup, inspects the lock metadata,
// and decides whether the roots overlap. These tests use a helper subprocess to
// model that situation faithfully.
//
// The coverage here focuses on the policy that scan_lock.go implements:
// overlapping roots must block each other, disjoint roots should be allowed to
// proceed, the shared DB guard must admit multiple concurrent holders, and the
// startup/root lock sequence must be usable from independent sympath
// processes.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	scanLockHelperEnv     = "GO_WANT_SCAN_LOCK_HELPER"
	scanLockHelperModeEnv = "SCAN_LOCK_MODE"
)

func TestScanLockHelperProcess(t *testing.T) {
	if os.Getenv(scanLockHelperEnv) != "1" {
		return
	}

	stateDir := os.Getenv("SCAN_LOCK_STATE_DIR")
	machineID := os.Getenv("SCAN_LOCK_MACHINE_ID")
	root := os.Getenv("SCAN_LOCK_ROOT")
	mode := os.Getenv(scanLockHelperModeEnv)

	var err error
	var dbGuard *fileLock
	if mode == "shared-db-guard" {
		dbGuard, err = acquireScanDBGuardLockShared(context.Background(), stateDir)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		fmt.Fprintln(os.Stdout, "ready")
		_ = os.Stdout.Sync()
		_, _ = io.ReadAll(os.Stdin)
		_ = dbGuard.Close()
		os.Exit(0)
	}
	if mode == "probe-shared-db-guard-blocked" {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		dbGuard, err = acquireScanDBGuardLockShared(ctx, stateDir)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stdout, "blocked")
			os.Exit(0)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(7)
		}
		_ = dbGuard.Close()
		fmt.Fprintln(os.Stdout, "acquired")
		os.Exit(0)
	}

	startupLock, err := acquireScanStartupLock(context.Background(), stateDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	if mode == "startup-lock" {
		fmt.Fprintln(os.Stdout, "ready")
		_ = os.Stdout.Sync()
		_, _ = io.ReadAll(os.Stdin)
		_ = startupLock.Close()
		os.Exit(0)
	}
	if mode == "active-scan" {
		dbGuard, err = acquireScanDBGuardLockShared(context.Background(), stateDir)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(6)
		}
	}
	rootLock, err := acquireScanRootLock(stateDir, machineID, root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(4)
	}
	if err := startupLock.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(5)
	}

	fmt.Fprintln(os.Stdout, "ready")
	_ = os.Stdout.Sync()
	_, _ = io.ReadAll(os.Stdin)
	_ = rootLock.Close()
	if dbGuard != nil {
		_ = dbGuard.Close()
	}
	os.Exit(0)
}

func TestAcquireScanRootLock_RejectsActiveOverlappingRootAndAllowsDisjoint(t *testing.T) {
	stateDir := t.TempDir()
	machineID := "machine-a"
	root := filepath.Clean(filepath.Join(string(filepath.Separator), "data", "photos"))

	helperCmd, helperStdin := startScanLockHelper(t, stateDir, machineID, root, "")
	defer func() {
		_ = helperStdin.Close()
		_ = helperCmd.Wait()
	}()

	startupLock, err := acquireScanStartupLock(context.Background(), stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer startupLock.Close()

	overlapRoot := filepath.Join(root, "raw")
	if _, err := acquireScanRootLock(stateDir, machineID, overlapRoot); err == nil {
		t.Fatalf("expected overlapping root %q to be rejected", overlapRoot)
	} else if !strings.Contains(err.Error(), "overlapping root") {
		t.Fatalf("expected overlapping-root error, got %v", err)
	}

	disjointRoot := filepath.Clean(filepath.Join(string(filepath.Separator), "data", "music"))
	disjointLock, err := acquireScanRootLock(stateDir, machineID, disjointRoot)
	if err != nil {
		t.Fatalf("expected disjoint root %q to be allowed, got %v", disjointRoot, err)
	}
	defer disjointLock.Close()
}

func TestScanRootLock_CloseLeavesStablePathAndStaysInactive(t *testing.T) {
	stateDir := t.TempDir()
	machineID := "machine-a"
	root := filepath.Clean(filepath.Join(string(filepath.Separator), "data", "photos"))

	startupLock, err := acquireScanStartupLock(context.Background(), stateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer startupLock.Close()

	lock, err := acquireScanRootLock(stateDir, machineID, root)
	if err != nil {
		t.Fatal(err)
	}
	lockPath := lock.path
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("expected closed root lock to leave a stale lock path, got %v", err)
	}

	meta, active, err := inspectScanLock(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if active {
		t.Fatalf("expected stale root lock to be inactive, got %+v", meta)
	}
	if _, err := os.Stat(lockPath); err != nil {
		t.Fatalf("expected stale root lock path to stay in place, got %v", err)
	}

	reacquired, err := acquireScanRootLock(stateDir, machineID, root)
	if err != nil {
		t.Fatalf("expected reacquiring stale root lock %q to succeed, got %v", root, err)
	}
	defer reacquired.Close()
	if reacquired.path != lockPath {
		t.Fatalf("expected reacquired lock to reuse path %q, got %q", lockPath, reacquired.path)
	}
}

func TestAcquireScanDBGuardLockShared_AllowsConcurrentHolders(t *testing.T) {
	stateDir := t.TempDir()

	helperOne, stdinOne := startScanLockHelper(t, stateDir, "", "", "shared-db-guard")
	defer func() {
		_ = stdinOne.Close()
		_ = helperOne.Wait()
	}()

	helperTwo, stdinTwo := startScanLockHelper(t, stateDir, "", "", "shared-db-guard")
	defer func() {
		_ = stdinTwo.Close()
		_ = helperTwo.Wait()
	}()

	if _, ok, err := tryAcquireScanDBGuardLockExclusive(stateDir); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected exclusive DB guard acquisition to fail while shared holders are active")
	}
}

func startActiveScanHelper(t *testing.T, stateDir, machineID, root string) (*exec.Cmd, io.WriteCloser) {
	t.Helper()
	return startScanLockHelper(t, stateDir, machineID, root, "active-scan")
}

func startStartupLockHelper(t *testing.T, stateDir string) (*exec.Cmd, io.WriteCloser) {
	t.Helper()
	return startScanLockHelper(t, stateDir, "", "", "startup-lock")
}

func startScanLockHelper(t *testing.T, stateDir, machineID, root, mode string) (*exec.Cmd, io.WriteCloser) {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=TestScanLockHelperProcess")
	cmd.Env = append(os.Environ(),
		scanLockHelperEnv+"=1",
		"SCAN_LOCK_STATE_DIR="+stateDir,
		"SCAN_LOCK_MACHINE_ID="+machineID,
		"SCAN_LOCK_ROOT="+root,
	)
	if mode != "" {
		cmd.Env = append(cmd.Env, scanLockHelperModeEnv+"="+mode)
	}

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

	reader := bufio.NewReader(stdout)
	line, err := reader.ReadString('\n')
	if err != nil {
		_ = stdin.Close()
		_ = cmd.Wait()
		t.Fatalf("wait for helper readiness: %v (stderr: %s)", err, stderr.String())
	}
	if strings.TrimSpace(line) != "ready" {
		_ = stdin.Close()
		_ = cmd.Wait()
		t.Fatalf("unexpected helper output %q (stderr: %s)", line, stderr.String())
	}

	return cmd, stdin
}
