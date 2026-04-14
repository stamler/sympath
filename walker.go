package inventory

// walker.go traverses the directory tree and feeds the inventory pipeline.
//
// The walker is the first stage of the three-stage pipeline
// (walker → hash workers → writer). It emits two streams:
//
//   - entryCh: a baseEntry for every regular file, containing metadata
//     (rel_path, name, ext, size, mtime_ns) and an initial state.
//   - jobCh: a HashJob for every file whose content needs hashing.
//
// For each file, the walker first checks exact-root reuse and then any
// overlapping-root reuse candidates. If a matching candidate is found,
// the file is emitted as state="reused" with the stored hashes copied in,
// and no HashJob is enqueued — skipping the expensive I/O + SHA-256.
//
// Files that fail the reuse check are emitted as state="pending" and a
// corresponding HashJob is sent to the hash workers.
//
// Symlinks, non-regular files (devices, sockets, FIFOs), and excluded
// files (the SQLite database and its companions) are silently skipped.
// Directory-level permission errors cause the subtree to be skipped;
// file-level permission errors produce an entry with state="error".
//
// Relative paths stored in entries use forward slashes (filepath.ToSlash)
// for cross-platform consistency.

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"
)

// runWalker walks the directory tree rooted at root using filepath.WalkDir,
// emitting a baseEntry on entryCh for every regular file encountered and a
// HashJob on jobCh for files that need content hashing.
//
// The reuse decision compares each file's (size, mtime_ns) against the
// exact-root previous scan and then any overlapping authoritative scans.
// Matching files are emitted as state="reused" with hashes copied from a
// reuse source; non-matching files are emitted as
// state="pending" with a HashJob enqueued.
//
// The caller must close entryCh and jobCh after runWalker returns.
func runWalker(
	ctx context.Context,
	root string,
	reuse *reuseSources,
	excludeSet map[string]struct{},
	entryCh chan<- baseEntry,
	jobCh chan<- HashJob,
	progress *ScanProgress,
) error {
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Handle walk errors (permission denied, etc.)
		if walkErr != nil {
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			// File-level error: record it and continue
			rel, relErr := filepath.Rel(root, path)
			if relErr == nil {
				rel = filepath.ToSlash(rel)
				entry := baseEntry{
					RelPath: rel,
					Name:    filepath.Base(rel),
					Ext:     strings.ToLower(filepath.Ext(filepath.Base(rel))),
					State:   "error",
					ErrMsg:  walkErr.Error(),
				}
				entryCh <- entry
				progress.noteDiscovered(entry.State)
			}
			return nil
		}

		// Skip directories (we recurse into them but don't emit entries)
		if d.IsDir() {
			return nil
		}

		// Skip symlinks
		if d.Type()&fs.ModeSymlink != 0 {
			return nil
		}

		// Get file info
		info, err := d.Info()
		if err != nil {
			return nil
		}

		// Skip non-regular files (devices, sockets, etc.)
		if !info.Mode().IsRegular() {
			return nil
		}

		// Skip excluded files (DB + WAL + SHM)
		absPath, _ := resolveAbsPath(path)
		if shouldExclude(absPath, excludeSet) {
			return nil
		}

		rel, err := filepath.Rel(root, path)
		if err != nil {
			return nil
		}
		rel = filepath.ToSlash(rel)

		name := filepath.Base(rel)
		ext := strings.ToLower(filepath.Ext(name))
		size := info.Size()
		mtimeNS := info.ModTime().UnixNano()

		// Check if we can reuse hashes from an exact or overlapping scan.
		prev, ok, err := reuse.lookup(rel, size, mtimeNS)
		if err != nil {
			return err
		}
		if ok {
			entry := baseEntry{
				RelPath:     rel,
				Name:        name,
				Ext:         ext,
				Size:        size,
				MtimeNS:     mtimeNS,
				State:       "reused",
				Fingerprint: prev.Fingerprint,
				SHA256:      prev.SHA256,
			}
			entryCh <- entry
			progress.noteDiscovered(entry.State)
			return nil
		}

		// File needs hashing
		entry := baseEntry{
			RelPath: rel,
			Name:    name,
			Ext:     ext,
			Size:    size,
			MtimeNS: mtimeNS,
			State:   "pending",
		}
		entryCh <- entry
		progress.noteDiscovered(entry.State)

		jobCh <- HashJob{
			AbsPath: absPath,
			RelPath: rel,
			Size:    size,
			MtimeNS: mtimeNS,
		}

		return nil
	})
}
