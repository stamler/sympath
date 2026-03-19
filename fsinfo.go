package inventory

// fsinfo.go provides cross-platform filesystem metadata detection.
//
// The platform-specific implementations live in fsinfo_darwin.go,
// fsinfo_linux.go, and fsinfo_windows.go. Each implements a
// detectVolumeInfo(root string) VolumeInfo function selected at
// compile time via build tags.
//
// This file contains the public wrapper and the shared
// probeCaseSensitivity function used by all platforms.

import (
	"os"
	"path/filepath"
	"strings"
)

// DetectVolumeInfo returns filesystem type and case sensitivity for root.
// The filesystem type is detected via OS-specific syscalls (statfs on
// POSIX, GetVolumeInformationW on Windows). Case sensitivity is probed
// by creating a temporary file and checking whether its upper-cased
// name resolves to the same file.
func DetectVolumeInfo(root string) VolumeInfo {
	return detectVolumeInfo(root)
}

// probeCaseSensitivity creates a temporary probe file in root, then
// attempts to stat the upper-cased version of its name. If the
// upper-cased name resolves, the filesystem is case-insensitive.
// If not, it is case-sensitive. Falls back to assuming case-sensitive
// if the probe file cannot be created (e.g., read-only filesystem).
func probeCaseSensitivity(root string) bool {
	f, err := os.CreateTemp(root, ".case_probe_")
	if err != nil {
		// Can't probe; assume case-sensitive as the safer default
		return true
	}
	probePath := f.Name()
	f.Close()
	defer os.Remove(probePath)

	// Try to stat the uppercase version of the probe filename
	base := filepath.Base(probePath)
	upperPath := filepath.Join(root, strings.ToUpper(base))

	_, err = os.Stat(upperPath)
	if err != nil {
		// Upper case name doesn't resolve → case-sensitive
		return true
	}
	// Upper case name resolves → case-insensitive
	return false
}
