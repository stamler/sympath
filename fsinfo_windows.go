//go:build windows

package inventory

// fsinfo_windows.go detects filesystem metadata on Windows.
// It calls GetVolumeInformationW to retrieve the filesystem name
// (e.g., "NTFS", "ReFS") and the FILE_CASE_SENSITIVE_SEARCH flag.
// Unlike POSIX systems, Windows reports case sensitivity through
// volume flags rather than requiring a probe.

import (
	"path/filepath"
	"syscall"
	"unsafe"
)

// detectVolumeInfo returns filesystem type and case sensitivity for the
// given path on Windows. It extracts the volume root (e.g., "C:\") from
// the path and calls GetVolumeInformationW for the FS name and flags.
func detectVolumeInfo(root string) VolumeInfo {
	var info VolumeInfo

	// Get the volume root (e.g., "C:\\")
	volRoot := filepath.VolumeName(root)
	if volRoot == "" {
		return info
	}
	volRoot += `\`

	volRootPtr, err := syscall.UTF16PtrFromString(volRoot)
	if err != nil {
		return info
	}

	var (
		fsNameBuf [256]uint16
		flags     uint32
	)

	kernel32 := syscall.MustLoadDLL("kernel32.dll")
	proc := kernel32.MustFindProc("GetVolumeInformationW")

	ret, _, _ := proc.Call(
		uintptr(unsafe.Pointer(volRootPtr)),
		0, 0, 0, 0,
		uintptr(unsafe.Pointer(&flags)),
		uintptr(unsafe.Pointer(&fsNameBuf[0])),
		uintptr(len(fsNameBuf)),
	)
	if ret == 0 {
		return info
	}

	info.FSType = syscall.UTF16ToString(fsNameBuf[:])

	// FILE_CASE_SENSITIVE_SEARCH = 0x00000001
	info.CaseSensitive = flags&0x00000001 != 0

	return info
}
