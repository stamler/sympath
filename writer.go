package inventory

// writer.go is the final stage of the inventory pipeline: a single
// goroutine that persists base entries and hash results to SQLite.
//
// The writer consumes two channels:
//
//   - entryCh (baseEntry): metadata rows emitted by the walker. Each
//     entry is INSERTed into the entries table with its initial state
//     ("pending", "reused", or "error").
//   - resultCh (HashResult): hash computation outcomes from the workers.
//     Each result UPDATEs the corresponding entry's fingerprint, sha256,
//     state, and err columns.
//
// Ordering invariant: an UPDATE can only succeed if the row's INSERT
// has already been committed. Because the walker and hash workers run
// concurrently, a result may arrive before its base entry in the
// select loop. The writer enforces correct ordering with a
// priority-drain pattern: it always drains all available entries from
// entryCh before processing any result from resultCh.
//
// To keep the WAL bounded, operations are batched into transactions of
// ~5000 rows. The commitBatch closure commits the current transaction,
// begins a new one, and re-prepares both statements.

import (
	"context"
	"database/sql"
)

// batchSize is the number of INSERT/UPDATE operations accumulated
// before the writer commits the current transaction and starts a new
// one. This keeps the SQLite WAL bounded while amortizing commit cost.
const batchSize = 5000

// insertSQL is the prepared statement template for inserting a base
// entry row into the entries table.
const insertSQL = `
	INSERT INTO entries (scan_id, rel_path, rel_path_norm, name, ext, size, mtime_ns, fingerprint, sha256, state, err)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`

// updateSQL is the prepared statement template for updating an entry's
// hash columns after a hash worker completes.
const updateSQL = `
	UPDATE entries SET fingerprint=?, sha256=?, state=?, err=?
	WHERE scan_id=? AND rel_path=?
`

// runWriter is the single database writer goroutine. It receives base
// entries from the walker via entryCh and hash results from workers via
// resultCh, persisting them to the entries table in batched transactions.
//
// The writer uses a priority-drain pattern: it always consumes all
// available entries from entryCh before processing any result from
// resultCh. This guarantees that the INSERT for a given rel_path is
// committed before the corresponding UPDATE arrives.
//
// Operations are batched into transactions of ~batchSize (5000) rows.
// A final commit flushes any remaining buffered operations. On error
// or context cancellation, the current transaction is rolled back.
func runWriter(
	ctx context.Context,
	db *sql.DB,
	scanID int64,
	entryCh <-chan baseEntry,
	resultCh <-chan HashResult,
) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	insertStmt, err := tx.PrepareContext(ctx, insertSQL)
	if err != nil {
		tx.Rollback()
		return err
	}

	updateStmt, err := tx.PrepareContext(ctx, updateSQL)
	if err != nil {
		insertStmt.Close()
		tx.Rollback()
		return err
	}

	ops := 0

	commitBatch := func() error {
		insertStmt.Close()
		updateStmt.Close()
		if err := tx.Commit(); err != nil {
			return err
		}
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		insertStmt, err = tx.PrepareContext(ctx, insertSQL)
		if err != nil {
			tx.Rollback()
			return err
		}
		updateStmt, err = tx.PrepareContext(ctx, updateSQL)
		if err != nil {
			insertStmt.Close()
			tx.Rollback()
			return err
		}
		ops = 0
		return nil
	}

	doInsert := func(entry baseEntry) error {
		var errStr *string
		if entry.ErrMsg != "" {
			errStr = &entry.ErrMsg
		}
		var fp, hash *string
		if entry.Fingerprint != "" {
			fp = &entry.Fingerprint
		}
		if entry.SHA256 != "" {
			hash = &entry.SHA256
		}
		var relPathNorm *string
		if entry.RelPathNorm != "" {
			relPathNorm = &entry.RelPathNorm
		}
		if _, err := insertStmt.ExecContext(ctx,
			scanID, entry.RelPath, relPathNorm, entry.Name, entry.Ext,
			entry.Size, entry.MtimeNS, fp, hash, entry.State, errStr,
		); err != nil {
			return err
		}
		ops++
		if ops >= batchSize {
			return commitBatch()
		}
		return nil
	}

	doUpdate := func(result HashResult) error {
		var errStr *string
		if result.Err != "" {
			errStr = &result.Err
		}
		var fp, hash *string
		if result.Fingerprint != "" {
			fp = &result.Fingerprint
		}
		if result.SHA256 != "" {
			hash = &result.SHA256
		}
		if _, err := updateStmt.ExecContext(ctx,
			fp, hash, result.State, errStr,
			scanID, result.RelPath,
		); err != nil {
			return err
		}
		ops++
		if ops >= batchSize {
			return commitBatch()
		}
		return nil
	}

	entryClosed := false
	resultClosed := false

	for !entryClosed || !resultClosed {
		// Priority: drain all pending entries before processing results.
		// This guarantees INSERT precedes UPDATE for the same rel_path.
		if !entryClosed {
			drained := false
			for !drained {
				select {
				case entry, ok := <-entryCh:
					if !ok {
						entryClosed = true
						drained = true
						continue
					}
					if err := doInsert(entry); err != nil {
						tx.Rollback()
						return err
					}
				default:
					drained = true
				}
			}
		}

		// If both channels are closed, we're done
		if entryClosed && resultClosed {
			break
		}

		// Process one result (or wait for more entries/results)
		if entryClosed {
			// Only results remaining
			select {
			case result, ok := <-resultCh:
				if !ok {
					resultClosed = true
					continue
				}
				if err := doUpdate(result); err != nil {
					tx.Rollback()
					return err
				}
			case <-ctx.Done():
				tx.Rollback()
				return ctx.Err()
			}
		} else if !resultClosed {
			// Both channels active — wait for either, but prefer entries
			select {
			case entry, ok := <-entryCh:
				if !ok {
					entryClosed = true
					continue
				}
				if err := doInsert(entry); err != nil {
					tx.Rollback()
					return err
				}
			case result, ok := <-resultCh:
				if !ok {
					resultClosed = true
					continue
				}
				if err := doUpdate(result); err != nil {
					tx.Rollback()
					return err
				}
			case <-ctx.Done():
				tx.Rollback()
				return ctx.Err()
			}
		}
	}

	// Final commit
	insertStmt.Close()
	updateStmt.Close()
	return tx.Commit()
}
