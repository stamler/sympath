// Package main contains sympath's CLI commands and the process-level
// coordination they need around the shared local SQLite database.
//
// This file implements the scan locking model that makes interrupted-scan
// resume safe without giving up concurrency for disjoint roots.
//
// The locking behavior has two layers:
//
//  1. A short-lived startup lock serializes the part of command startup that
//     decides which root lock to take and coordinates the transition from
//     consolidation into an active scan. That prevents two scan processes from
//     racing while they inspect the existing lock set.
//  2. A long-lived global DB guard stays held in shared mode for the duration
//     of every active scan, while consolidation takes it exclusively. That lets
//     multiple disjoint scans run concurrently without allowing a new
//     consolidation pass to replace the live database file underneath them.
//  3. A long-lived per-root lock stays held for the duration of a scan. The
//     lock metadata records the machine ID and normalized root path so a later
//     process can detect active overlapping scans and reject only those while
//     still allowing disjoint roots to proceed concurrently.
//
// Once a process owns the root lock for a machine/root pair, any lingering
// SQLite rows with scans.status = 'running' for that exact machine/root can be
// treated as orphaned. A live same-root scan would still be holding the root
// lock, so the next process would not have been able to acquire it. That lets
// the caller safely adopt stale running rows to failed and then reuse their
// hashes through the existing failed-scan resume path.
//
// The implementation uses ordinary files plus OS advisory locks instead of a
// JSON "lock file" protocol. The on-disk JSON is only descriptive metadata for
// overlap checks and stale-lock detection; the actual mutual exclusion comes from
// the kernel file lock. Root lock files intentionally keep a stable pathname on
// normal exit so a successor cannot lose its rendezvous point to an unlink race.
// Crashes and clean exits both leave unlocked stale files behind, and later
// scans detect that those files are inactive, ignore their old metadata, and
// overwrite the same stable path on the next acquisition.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	scanLocksDirName    = "scan-locks"
	scanStartupLockName = ".startup.lock"
	scanDBGuardLockName = ".db.guard.lock"
	scanLockRetryDelay  = 50 * time.Millisecond
)

type fileLockMode int

const (
	fileLockModeShared fileLockMode = iota + 1
	fileLockModeExclusive
)

type fileLock struct {
	file *os.File
}

type scanLock struct {
	lock *fileLock
	path string
}

type scanLockMetadata struct {
	MachineID string `json:"machine_id"`
	Root      string `json:"root"`
}

func acquireScanStartupLock(ctx context.Context, stateDir string) (*fileLock, error) {
	locksDir, err := ensureScanLocksDir(stateDir)
	if err != nil {
		return nil, err
	}
	return acquireBlockingFileLock(ctx, filepath.Join(locksDir, scanStartupLockName), fileLockModeExclusive)
}

func acquireScanDBGuardLockShared(ctx context.Context, stateDir string) (*fileLock, error) {
	locksDir, err := ensureScanLocksDir(stateDir)
	if err != nil {
		return nil, err
	}
	return acquireBlockingFileLock(ctx, filepath.Join(locksDir, scanDBGuardLockName), fileLockModeShared)
}

func acquireScanDBGuardLockExclusive(ctx context.Context, stateDir string) (*fileLock, error) {
	locksDir, err := ensureScanLocksDir(stateDir)
	if err != nil {
		return nil, err
	}
	return acquireBlockingFileLock(ctx, filepath.Join(locksDir, scanDBGuardLockName), fileLockModeExclusive)
}

func tryAcquireScanDBGuardLockExclusive(stateDir string) (*fileLock, bool, error) {
	locksDir, err := ensureScanLocksDir(stateDir)
	if err != nil {
		return nil, false, err
	}
	return tryAcquireFileLock(filepath.Join(locksDir, scanDBGuardLockName), fileLockModeExclusive)
}

func acquireBlockingFileLock(ctx context.Context, path string, mode fileLockMode) (*fileLock, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	for {
		locked, err := tryLockFile(file, mode)
		if err != nil {
			file.Close()
			return nil, err
		}
		if locked {
			return &fileLock{file: file}, nil
		}
		select {
		case <-ctx.Done():
			file.Close()
			return nil, ctx.Err()
		case <-time.After(scanLockRetryDelay):
		}
	}
}

func tryAcquireFileLock(path string, mode fileLockMode) (*fileLock, bool, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, false, err
	}
	locked, err := tryLockFile(file, mode)
	if err != nil {
		file.Close()
		return nil, false, err
	}
	if !locked {
		file.Close()
		return nil, false, nil
	}
	return &fileLock{file: file}, true, nil
}

func (l *fileLock) Close() error {
	if l == nil || l.file == nil {
		return nil
	}
	unlockErr := unlockFile(l.file)
	closeErr := l.file.Close()
	l.file = nil
	return errors.Join(unlockErr, closeErr)
}

func acquireScanRootLock(stateDir, machineID, root string) (*scanLock, error) {
	locksDir, err := ensureScanLocksDir(stateDir)
	if err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(locksDir)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Name() == scanStartupLockName || entry.Name() == scanDBGuardLockName {
			continue
		}
		path := filepath.Join(locksDir, entry.Name())
		meta, active, err := inspectScanLock(path)
		if err != nil {
			return nil, err
		}
		if active && meta.MachineID == machineID && rootsOverlap(meta.Root, root) {
			return nil, fmt.Errorf("scan already in progress for overlapping root %q", meta.Root)
		}
	}

	lockPath := scanRootLockPath(locksDir, machineID, root)
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	locked, err := tryLockFile(file, fileLockModeExclusive)
	if err != nil {
		file.Close()
		return nil, err
	}
	if !locked {
		file.Close()
		return nil, fmt.Errorf("scan already in progress for %q", root)
	}

	meta := scanLockMetadata{MachineID: machineID, Root: root}
	if err := writeScanLockMetadata(file, meta); err != nil {
		unlockFile(file)
		file.Close()
		return nil, err
	}
	return &scanLock{
		lock: &fileLock{file: file},
		path: lockPath,
	}, nil
}

func (l *scanLock) Close() error {
	if l == nil {
		return nil
	}
	return l.lock.Close()
}

func ensureScanLocksDir(stateDir string) (string, error) {
	locksDir := filepath.Join(stateDir, scanLocksDirName)
	if err := os.MkdirAll(locksDir, 0755); err != nil {
		return "", err
	}
	return locksDir, nil
}

func scanRootLockPath(locksDir, machineID, root string) string {
	sum := sha256.Sum256([]byte(machineID + "\n" + root))
	return filepath.Join(locksDir, hex.EncodeToString(sum[:])+".lock")
}

func inspectScanLock(path string) (scanLockMetadata, bool, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return scanLockMetadata{}, false, nil
		}
		return scanLockMetadata{}, false, err
	}

	locked, err := tryLockFile(file, fileLockModeExclusive)
	if err != nil {
		file.Close()
		return scanLockMetadata{}, false, err
	}
	if locked {
		lock := &fileLock{file: file}
		_ = lock.Close()
		return scanLockMetadata{}, false, nil
	}

	meta, err := readScanLockMetadata(file)
	file.Close()
	if err != nil {
		return scanLockMetadata{}, false, fmt.Errorf("read active scan lock %q: %w", path, err)
	}
	return meta, true, nil
}

func writeScanLockMetadata(file *os.File, meta scanLockMetadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if err := file.Truncate(0); err != nil {
		return err
	}
	if _, err := file.Seek(0, 0); err != nil {
		return err
	}
	if _, err := file.Write(data); err != nil {
		return err
	}
	return file.Sync()
}

func readScanLockMetadata(file *os.File) (scanLockMetadata, error) {
	if _, err := file.Seek(0, 0); err != nil {
		return scanLockMetadata{}, err
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return scanLockMetadata{}, err
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return scanLockMetadata{}, errors.New("scan lock metadata is empty")
	}

	var meta scanLockMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return scanLockMetadata{}, err
	}
	if meta.Root == "" {
		return scanLockMetadata{}, errors.New("scan lock root is empty")
	}
	if meta.MachineID == "" {
		return scanLockMetadata{}, errors.New("scan lock machine_id is empty")
	}
	return meta, nil
}

// rootsOverlap assumes both inputs were already normalized with normalizePath,
// so filepath.Rel can safely answer the ancestor/descendant question using the
// host OS's path semantics.
func rootsOverlap(a, b string) bool {
	return isSameOrDescendant(a, b) || isSameOrDescendant(b, a)
}

func isSameOrDescendant(base, candidate string) bool {
	rel, err := filepath.Rel(base, candidate)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return false
	}
	return true
}
