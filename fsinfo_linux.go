//go:build linux

package inventory

// fsinfo_linux.go detects filesystem metadata on Linux.
// It maps the statfs f_type magic number to a human-readable name
// and probes case sensitivity via a temporary file. Most native Linux
// filesystems are case-sensitive, but mounted CIFS/SMB or FUSE shares
// may not be.

import (
	"syscall"
)

// fsMagicNames maps well-known Linux filesystem magic numbers (from
// the statfs(2) man page and /usr/include/linux/magic.h) to
// human-readable names.
var fsMagicNames = map[int64]string{
	0xEF53:     "ext4",     // also ext2/ext3
	0x58465342: "xfs",
	0x9123683E: "btrfs",
	0x01021994: "tmpfs",
	0x28cd3d45: "cramfs",
	0x3153464a: "jfs",
	0x52654973: "reiserfs",
	0x6969:     "nfs",
	0xFF534D42: "cifs",
	0x65735546: "fuse",
	0xF2F52010: "f2fs",
	0xCAFE0001: "bcachefs",
}

// detectVolumeInfo returns filesystem type and case sensitivity for the
// given path on Linux. It uses syscall.Statfs to read the f_type magic
// number and maps it to a name via fsMagicNames.
func detectVolumeInfo(root string) VolumeInfo {
	var info VolumeInfo

	var stat syscall.Statfs_t
	if err := syscall.Statfs(root, &stat); err != nil {
		return info
	}

	if name, ok := fsMagicNames[stat.Type]; ok {
		info.FSType = name
	}

	// Linux filesystems are almost always case-sensitive.
	// Notable exception: mounted CIFS/SMB shares, but probing is the only
	// reliable way to detect those edge cases.
	info.CaseSensitive = probeCaseSensitivity(root)

	return info
}
