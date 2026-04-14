// Package inventory provides a high-performance, cross-platform directory
// tree inventory function backed by SQLite. It walks a directory tree,
// computes content hashes, and stores the results as a single authoritative
// snapshot using publish-on-complete semantics.
//
// The primary entry point is [InventoryTree].
package inventory

// MachineIdentity identifies the machine producing local scan rows.
//
// MachineID is the stable, persistent identifier used for database keys
// and cross-machine consolidation. Hostname is display metadata that
// helps operators understand which machine produced a scan, but it is
// not used as the authoritative identity because hostnames can change.
type MachineIdentity struct {
	MachineID string
	Hostname  string
}

// PrevEntry holds the subset of a reusable scan entry needed for reuse
// decisions during the walk phase. If a file's Size and MtimeNS match
// an exact previous entry or a trusted overlapping authoritative entry,
// its Fingerprint and SHA256 are copied forward without re-reading the
// file.
type PrevEntry struct {
	Size        int64
	MtimeNS     int64
	Fingerprint string
	SHA256      string
}

// HashJob represents a file that needs its content hashed. It is sent
// from the walker goroutine to the pool of hash workers via the job
// channel. AbsPath is the symlink-resolved absolute path used for
// opening the file; RelPath is the forward-slash relative path used
// as the database key.
type HashJob struct {
	AbsPath string
	RelPath string
	Size    int64
	MtimeNS int64
}

// HashResult carries the output of a hash worker back to the DB writer.
// State indicates the outcome:
//   - "ok":       hashes are valid
//   - "unstable": file changed during read (retried once before marking)
//   - "vanished": file was deleted before the worker could open it
//   - "error":    an I/O or permission error occurred
type HashResult struct {
	RelPath     string
	Fingerprint string
	SHA256      string
	State       string
	Err         string
}

// baseEntry is the initial metadata record sent from the walker to the
// DB writer. It is inserted into the entries table immediately. If the
// entry's State is "pending", a corresponding HashJob has been enqueued
// and a later HashResult will update the row with hashes. If the State
// is "reused", Fingerprint and SHA256 are already populated from the
// previous scan. If the State is "error", ErrMsg describes the problem.
type baseEntry struct {
	RelPath     string
	Name        string
	Ext         string
	Size        int64
	MtimeNS     int64
	State       string // "reused", "pending", "error"
	Fingerprint string // populated when State is "reused"
	SHA256      string // populated when State is "reused"
	ErrMsg      string // populated when State is "error"
}

// VolumeInfo holds filesystem metadata detected for a root path.
// FSType is a best-effort name like "apfs", "ext4", or "NTFS".
// CaseSensitive indicates whether the filesystem distinguishes
// filenames that differ only in case.
type VolumeInfo struct {
	FSType        string
	CaseSensitive bool
}
