//go:build !windows

// Package main contains the CLI's platform-specific helpers for process-wide
// scan coordination.
//
// This file supplies the non-Windows implementation of the low-level file-lock
// primitives used by scan_lock.go. The higher-level scan lock code needs a
// non-blocking "try lock" operation so it can:
//
//  1. wait with context cancellation when acquiring the startup lock,
//  2. probe existing per-root lock files to decide whether they are active or
//     stale, and
//  3. keep the actual overlap policy independent from OS-specific syscalls.
//
// On Unix-like systems that behavior is implemented with flock(2). A successful
// LOCK_EX|LOCK_NB means this process now owns the advisory lock. EWOULDBLOCK
// and EAGAIN mean some other live process still owns it, so the caller should
// treat the lock as active rather than stale. Unlocking delegates to LOCK_UN.
package main

import (
	"os"

	"golang.org/x/sys/unix"
)

func tryLockFile(file *os.File, mode fileLockMode) (bool, error) {
	lockOp := unix.LOCK_SH
	if mode == fileLockModeExclusive {
		lockOp = unix.LOCK_EX
	}
	err := unix.Flock(int(file.Fd()), lockOp|unix.LOCK_NB)
	if err == nil {
		return true, nil
	}
	if err == unix.EWOULDBLOCK || err == unix.EAGAIN {
		return false, nil
	}
	return false, err
}

func unlockFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
