// Package inventory contains the scan-time reuse logic that lets a new scan
// skip rehashing files whose content is already known.
//
// This file implements the two-layer reuse lookup used by the walker:
//
//  1. Exact-root reuse: before a scan starts, inventory.go preloads reusable
//     entries from the current authoritative scan for the same root plus the
//     newest failed interrupted scan for that root. Those entries are grouped
//     by relative path because multiple candidates can now exist for the same
//     file path. During lookup, a candidate is reusable only when its size and
//     mtime_ns match the file currently being walked.
//  2. Overlap reuse: if the exact-root lookup misses, the walker can lazily
//     consult other authoritative scans from the same machine whose roots
//     overlap the target root. Ancestor roots are translated by stripping the
//     target root's prefix from the source relative path; descendant roots are
//     translated by prepending the descendant root's relative prefix.
//
// The safety rule for both layers is the same: matching metadata is necessary
// but not sufficient when multiple candidates exist. If more than one candidate
// matches rel_path + size + mtime_ns and those candidates disagree on
// fingerprint or SHA-256, lookup intentionally returns a miss so the file is
// hashed fresh. That keeps reuse deterministic and prevents a failed
// interrupted scan from overriding a conflicting authoritative result simply by
// being merged first.
//
// The overlap index is loaded lazily because ordinary rescans usually find most
// reuse opportunities in the exact-root cache. Avoiding the overlap query on
// the common path keeps startup fast while still allowing same-machine
// ancestor/descendant scans to rescue reuse on exact misses.
package inventory

import (
	"context"
	"database/sql"
	"path"
	"path/filepath"
	"strings"
)

// reuseSources is stateful and not safe for concurrent use.
// The walker consults it from a single goroutine.
type reuseSources struct {
	exact         map[string][]PrevEntry
	overlap       overlapReuseIndex
	loadOverlap   func() (overlapReuseIndex, error)
	overlapLoaded bool
}

func (s *reuseSources) lookup(relPath string, size, mtimeNS int64) (PrevEntry, bool, error) {
	var matched PrevEntry
	found := false
	for _, prev := range s.exact[relPath] {
		if prev.Size != size || prev.MtimeNS != mtimeNS {
			continue
		}
		if !found {
			matched = prev
			found = true
			continue
		}
		if matched.Fingerprint != prev.Fingerprint || matched.SHA256 != prev.SHA256 {
			return PrevEntry{}, false, nil
		}
	}
	if found {
		return matched, true, nil
	}

	if !s.overlapLoaded {
		s.overlapLoaded = true
		if s.loadOverlap != nil {
			index, err := s.loadOverlap()
			if err != nil {
				return PrevEntry{}, false, err
			}
			s.overlap = index
		}
		if s.overlap == nil {
			s.overlap = make(overlapReuseIndex)
		}
	}

	prev, ok := s.overlap.lookup(relPath, size, mtimeNS)
	return prev, ok, nil
}

type overlapReuseCandidate struct {
	PrevEntry
}

type overlapReuseIndex map[string][]overlapReuseCandidate

func (idx overlapReuseIndex) lookup(relPath string, size, mtimeNS int64) (PrevEntry, bool) {
	candidates := idx[relPath]
	if len(candidates) == 0 {
		return PrevEntry{}, false
	}

	var matched PrevEntry
	found := false
	for _, candidate := range candidates {
		if candidate.Size != size || candidate.MtimeNS != mtimeNS {
			continue
		}
		if !found {
			matched = candidate.PrevEntry
			found = true
			continue
		}
		if matched.Fingerprint != candidate.Fingerprint || matched.SHA256 != candidate.SHA256 {
			return PrevEntry{}, false
		}
	}

	return matched, found
}

type overlapRootKind int

const (
	overlapRootAncestor overlapRootKind = iota + 1
	overlapRootDescendant
)

type overlapRoot struct {
	SourceRoot   string
	ScanID       int64
	Kind         overlapRootKind
	SourceSubdir string
	TargetPrefix string
}

type rowScanner interface {
	Scan(dest ...any) error
}

func loadOverlapReuseIndex(ctx context.Context, db *sql.DB, machineID, targetRoot string) (overlapReuseIndex, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT root, current_scan_id
		FROM roots
		WHERE machine_id = ? AND root <> ? AND current_scan_id IS NOT NULL
	`, machineID, targetRoot)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	index := make(overlapReuseIndex)
	for rows.Next() {
		var sourceRoot string
		var scanID int64
		if err := rows.Scan(&sourceRoot, &scanID); err != nil {
			return nil, err
		}

		overlap, ok := classifyOverlapRoot(targetRoot, sourceRoot, scanID)
		if !ok {
			continue
		}
		if err := loadOverlapRootEntries(ctx, db, index, overlap); err != nil {
			return nil, err
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return index, nil
}

func classifyOverlapRoot(targetRoot, sourceRoot string, scanID int64) (overlapRoot, bool) {
	if rel, ok := descendantRelativePath(sourceRoot, targetRoot); ok {
		if rel == "" {
			return overlapRoot{}, false
		}
		return overlapRoot{
			SourceRoot:   sourceRoot,
			ScanID:       scanID,
			Kind:         overlapRootAncestor,
			SourceSubdir: rel,
		}, true
	}

	if rel, ok := descendantRelativePath(targetRoot, sourceRoot); ok {
		if rel == "" {
			return overlapRoot{}, false
		}
		return overlapRoot{
			SourceRoot:   sourceRoot,
			ScanID:       scanID,
			Kind:         overlapRootDescendant,
			TargetPrefix: rel,
		}, true
	}

	return overlapRoot{}, false
}

func descendantRelativePath(base, candidate string) (string, bool) {
	rel, err := filepath.Rel(base, candidate)
	if err != nil {
		return "", false
	}
	if rel == "." {
		return "", true
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", false
	}
	return filepath.ToSlash(rel), true
}

func loadOverlapRootEntries(ctx context.Context, db *sql.DB, index overlapReuseIndex, overlap overlapRoot) error {
	switch overlap.Kind {
	case overlapRootAncestor:
		return loadAncestorOverlapEntries(ctx, db, index, overlap)
	case overlapRootDescendant:
		return loadDescendantOverlapEntries(ctx, db, index, overlap)
	default:
		return nil
	}
}

func loadAncestorOverlapEntries(ctx context.Context, db *sql.DB, index overlapReuseIndex, overlap overlapRoot) error {
	rows, err := db.QueryContext(ctx, `
		SELECT rel_path, size, mtime_ns, fingerprint, sha256
		FROM entries
		WHERE scan_id = ?
		  AND state IN ('ok', 'reused')
		  AND NULLIF(fingerprint, '') IS NOT NULL
		  AND NULLIF(sha256, '') IS NOT NULL
		  AND substr(rel_path, 1, length(?) + 1) = ? || '/'
	`, overlap.ScanID, overlap.SourceSubdir, overlap.SourceSubdir)
	if err != nil {
		return err
	}
	defer rows.Close()

	prefix := overlap.SourceSubdir + "/"
	for rows.Next() {
		relPath, prev, err := scanPrevEntry(rows)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(relPath, prefix) {
			continue
		}
		targetRelPath := strings.TrimPrefix(relPath, prefix)
		index[targetRelPath] = append(index[targetRelPath], overlapReuseCandidate{
			PrevEntry: prev,
		})
	}

	return rows.Err()
}

func loadDescendantOverlapEntries(ctx context.Context, db *sql.DB, index overlapReuseIndex, overlap overlapRoot) error {
	rows, err := db.QueryContext(ctx, `
		SELECT rel_path, size, mtime_ns, fingerprint, sha256
		FROM entries
		WHERE scan_id = ?
		  AND state IN ('ok', 'reused')
		  AND NULLIF(fingerprint, '') IS NOT NULL
		  AND NULLIF(sha256, '') IS NOT NULL
	`, overlap.ScanID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		relPath, prev, err := scanPrevEntry(rows)
		if err != nil {
			return err
		}
		targetRelPath := path.Join(overlap.TargetPrefix, relPath)
		index[targetRelPath] = append(index[targetRelPath], overlapReuseCandidate{
			PrevEntry: prev,
		})
	}

	return rows.Err()
}

func scanPrevEntry(scanner rowScanner) (string, PrevEntry, error) {
	var relPath string
	var prev PrevEntry
	var fp, hash sql.NullString
	if err := scanner.Scan(&relPath, &prev.Size, &prev.MtimeNS, &fp, &hash); err != nil {
		return "", PrevEntry{}, err
	}
	if fp.Valid {
		prev.Fingerprint = fp.String
	}
	if hash.Valid {
		prev.SHA256 = hash.String
	}
	return relPath, prev, nil
}
