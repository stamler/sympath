//go:build windows

// Package main contains the CLI's platform-specific helpers for process-wide
// scan coordination.
//
// This file supplies the Windows implementation of the low-level file-lock
// primitives used by scan_lock.go. The higher-level scan lock code relies on a
// non-blocking exclusive lock probe so it can distinguish a live process that
// still owns a root lock from an orphaned file left behind by a crash.
//
// Windows does not expose flock(2), so the implementation uses LockFileEx with
// LOCKFILE_EXCLUSIVE_LOCK and LOCKFILE_FAIL_IMMEDIATELY. A successful call means
// this process owns the lock. ERROR_LOCK_VIOLATION and
// ERROR_SHARING_VIOLATION mean another process still holds it, which the caller
// treats as an active scan.
//
// scan_lock.go stores JSON metadata at the beginning of each lock file so other
// processes can inspect active root locks and decide whether their roots
// overlap. Locking byte 0 would make those reads fail on Windows because the
// locked byte range itself becomes unreadable to other processes. To keep the
// metadata readable while still getting kernel-enforced mutual exclusion, the
// Windows implementation locks a single byte at a high offset that the metadata
// never reaches.
package main

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

const windowsFileLockOffset = 1 << 20

func tryLockFile(file *os.File, mode fileLockMode) (bool, error) {
	flags := uint32(windows.LOCKFILE_FAIL_IMMEDIATELY)
	if mode == fileLockModeExclusive {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}
	err := windows.LockFileEx(
		windows.Handle(file.Fd()),
		flags,
		0,
		1,
		0,
		fileLockOverlapped(),
	)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, windows.ERROR_LOCK_VIOLATION) || errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
		return false, nil
	}
	return false, err
}

func unlockFile(file *os.File) error {
	return windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, fileLockOverlapped())
}

func fileLockOverlapped() *windows.Overlapped {
	return &windows.Overlapped{Offset: windowsFileLockOffset}
}
