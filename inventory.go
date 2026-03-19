package inventory

// inventory.go contains the public API and orchestration logic for the
// directory inventory pipeline.
//
// The sole public entry point is InventoryTree, which scans a directory
// tree, computes content hashes for every regular file, and stores the
// results in a SQLite database. Each call produces a new scan that
// becomes authoritative only after the entire pipeline completes
// (publish-on-complete semantics).
//
// Orchestration proceeds in these phases:
//
//  1. Setup: resolve root path, configure DB connection, ensure schema,
//     detect DB file for exclusion, load previous scan entries, detect
//     volume info (filesystem type and case sensitivity).
//  2. Pipeline: launch walker, N hash workers, and writer goroutines
//     connected by buffered channels.
//  3. Coordination: walker closes entryCh + jobCh → workers drain and
//     exit via WaitGroup → resultCh closed → writer drains and returns.
//  4. Publish or fail: on success, an atomic transaction marks the scan
//     complete, updates the roots pointer, and deletes old scans
//     (CASCADE deletes their entries). On failure, the scan is marked
//     "failed" and the previous scan remains authoritative.

import (
	"context"
	"database/sql"
	"runtime"
	"sync"
	"time"
)

// InventoryTree scans the directory tree rooted at root and populates the
// SQLite database db with file metadata and content hashes for every
// regular file found.
//
// It maintains a single authoritative snapshot per root using
// publish-on-complete semantics: the new scan replaces the previous one
// only after the entire pipeline finishes successfully. A crash or error
// leaves the previous scan intact.
//
// Subsequent calls reuse fingerprints and SHA-256 hashes from the
// previous scan when a file's (size, mtime_ns) are unchanged, making
// re-scans of mostly-static trees very fast.
//
// The pipeline uses min(NumCPU, 4) hash workers to avoid disk thrashing
// while still saturating SHA-256 throughput on fast storage.
func InventoryTree(ctx context.Context, db *sql.DB, root string) error {
	// Normalize root path
	absRoot, err := resolveAbsPath(root)
	if err != nil {
		return err
	}

	// Configure connection and ensure schema
	if err := ConfigureConnection(ctx, db); err != nil {
		return err
	}
	if err := EnsureSchema(ctx, db); err != nil {
		return err
	}

	// Determine DB file path for exclusion
	dbPath, err := getDBPath(ctx, db)
	if err != nil {
		return err
	}
	excludeSet := makeExcludeSet(dbPath)

	// Load previous scan entries for reuse
	prevScanID, err := getCurrentScanID(ctx, db, absRoot)
	if err != nil {
		return err
	}
	prevEntries, err := loadPreviousEntries(ctx, db, prevScanID)
	if err != nil {
		return err
	}

	// Detect volume info
	volInfo := DetectVolumeInfo(absRoot)

	// Create new scan
	scanID, err := createScan(ctx, db, absRoot, volInfo)
	if err != nil {
		return err
	}

	// Determine worker count
	numWorkers := runtime.NumCPU()
	if numWorkers > 4 {
		numWorkers = 4
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	// Pipeline channels
	entryCh := make(chan baseEntry, 1000)
	jobCh := make(chan HashJob, 1000)
	resultCh := make(chan HashResult, 1000)

	// Error collection
	var walkerErr error
	var writerErr error

	// Start hash workers
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			hashWorker(ctx, jobCh, resultCh)
		}()
	}

	// Start walker
	go func() {
		walkerErr = runWalker(ctx, absRoot, prevEntries, excludeSet, entryCh, jobCh)
		close(entryCh)
		close(jobCh)
	}()

	// Close resultCh when all workers are done
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	// Run writer in the current goroutine
	writerErr = runWriter(ctx, db, scanID, entryCh, resultCh)

	// Determine outcome
	if walkerErr != nil || writerErr != nil {
		_ = failScan(ctx, db, scanID)
		if walkerErr != nil {
			return walkerErr
		}
		return writerErr
	}

	// Publish the scan
	return publishScan(ctx, db, scanID, absRoot)
}

// getCurrentScanID returns the current_scan_id for a root from the roots
// table, or 0 if no previous scan exists. A zero return value signals to
// the caller that no previous entries are available for reuse.
func getCurrentScanID(ctx context.Context, db *sql.DB, root string) (int64, error) {
	var scanID sql.NullInt64
	err := db.QueryRowContext(ctx,
		"SELECT current_scan_id FROM roots WHERE root = ?", root,
	).Scan(&scanID)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !scanID.Valid {
		return 0, nil
	}
	return scanID.Int64, nil
}

// loadPreviousEntries preloads all entries from the previous scan into a
// map keyed by rel_path. The walker uses this map for O(1) reuse lookups.
// If scanID is 0 (no previous scan), an empty map is returned.
func loadPreviousEntries(ctx context.Context, db *sql.DB, scanID int64) (map[string]PrevEntry, error) {
	if scanID == 0 {
		return make(map[string]PrevEntry), nil
	}

	rows, err := db.QueryContext(ctx,
		"SELECT rel_path, size, mtime_ns, fingerprint, sha256 FROM entries WHERE scan_id = ?",
		scanID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make(map[string]PrevEntry)
	for rows.Next() {
		var relPath string
		var pe PrevEntry
		var fp, hash sql.NullString
		if err := rows.Scan(&relPath, &pe.Size, &pe.MtimeNS, &fp, &hash); err != nil {
			return nil, err
		}
		if fp.Valid {
			pe.Fingerprint = fp.String
		}
		if hash.Valid {
			pe.SHA256 = hash.String
		}
		entries[relPath] = pe
	}
	return entries, rows.Err()
}

// createScan inserts a new scan row with status="running" and returns
// its auto-generated scan_id. The row records the root path, start
// timestamp, runtime platform (GOOS, GOARCH), filesystem type, and
// case sensitivity flag from the detected VolumeInfo.
func createScan(ctx context.Context, db *sql.DB, root string, vol VolumeInfo) (int64, error) {
	caseSensitive := 0
	if vol.CaseSensitive {
		caseSensitive = 1
	}
	result, err := db.ExecContext(ctx,
		`INSERT INTO scans (root, started_at, status, goos, goarch, fs_type, case_sensitive)
		 VALUES (?, ?, 'running', ?, ?, ?, ?)`,
		root, time.Now().UnixNano(),
		runtime.GOOS, runtime.GOARCH, vol.FSType, caseSensitive,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// publishScan atomically promotes a completed scan to authoritative
// status. In a single transaction it: (1) marks the scan as
// status="complete" with a finished_at timestamp, (2) upserts the
// roots table to point current_scan_id at this scan, and (3) deletes
// all other scans for the same root (CASCADE deletes their entries).
// This is the commit point — if it succeeds, the new scan is live.
func publishScan(ctx context.Context, db *sql.DB, scanID int64, root string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx,
		"UPDATE scans SET status='complete', finished_at=? WHERE scan_id=?",
		time.Now().UnixNano(), scanID,
	); err != nil {
		tx.Rollback()
		return err
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO roots (root, current_scan_id) VALUES (?, ?)
		 ON CONFLICT(root) DO UPDATE SET current_scan_id=excluded.current_scan_id`,
		root, scanID,
	); err != nil {
		tx.Rollback()
		return err
	}

	// Delete all other scans for this root (CASCADE deletes their entries)
	if _, err := tx.ExecContext(ctx,
		"DELETE FROM scans WHERE root=? AND scan_id <> ?",
		root, scanID,
	); err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}

// failScan marks a scan as status="failed" with a finished_at timestamp
// without updating the roots pointer. The previous scan (if any) remains
// authoritative. Failed scans are garbage-collected on the next
// successful publish for the same root.
func failScan(ctx context.Context, db *sql.DB, scanID int64) error {
	_, err := db.ExecContext(ctx,
		"UPDATE scans SET status='failed', finished_at=? WHERE scan_id=?",
		time.Now().UnixNano(), scanID,
	)
	return err
}
