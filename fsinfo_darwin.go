//go:build darwin

package inventory

// fsinfo_darwin.go detects filesystem metadata on macOS.
// It reads the filesystem type name from the statfs Fstypename field
// (e.g., "apfs", "hfs") and probes case sensitivity with a temp file
// because both APFS and HFS+ can be either case-sensitive or
// case-insensitive depending on how the volume was formatted.

import (
	"syscall"
	"unsafe"
)

// detectVolumeInfo returns filesystem type and case sensitivity for the
// given path on macOS. It uses syscall.Statfs to read the f_fstypename
// field, then probes case sensitivity via probeCaseSensitivity.
func detectVolumeInfo(root string) VolumeInfo {
	var info VolumeInfo

	var stat syscall.Statfs_t
	if err := syscall.Statfs(root, &stat); err != nil {
		return info
	}

	// f_fstypename is a [16]int8 on darwin
	nameBytes := (*[16]byte)(unsafe.Pointer(&stat.Fstypename))
	n := 0
	for n < 16 && nameBytes[n] != 0 {
		n++
	}
	info.FSType = string(nameBytes[:n])

	// Case sensitivity: check the volume flags.
	// MNT_CPROTECT is not useful here. Instead, check if the filesystem
	// is known to be case-insensitive by default.
	// APFS and HFS+ are case-insensitive by default on macOS,
	// but case-sensitive variants exist. The only reliable way is to probe.
	info.CaseSensitive = probeCaseSensitivity(root)

	return info
}
