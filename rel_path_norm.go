package inventory

import (
	"context"
	"database/sql"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

const (
	metadataRelPathNormBackfillV1Key = "rel_path_norm_backfill_v1"
	relPathNormBackfillDoneValue     = "done"
	relPathNormBackfillBatchSize     = 1000
)

// CompareRelPathKey returns the normalization-aware compare key for relPath.
//
// The raw path remains the authoritative on-disk identity. This helper only
// returns the canonical comparison form used to reconcile visually identical
// Unicode paths across filesystems. Invalid UTF-8 is left unchanged.
func CompareRelPathKey(relPath string) string {
	if normalized, ok := storedRelPathNorm(relPath); ok {
		return normalized
	}
	return relPath
}

func storedRelPathNorm(relPath string) (string, bool) {
	if !utf8.ValidString(relPath) {
		return "", false
	}
	normalized := norm.NFC.String(relPath)
	if normalized == relPath {
		return "", false
	}
	return normalized, true
}

// EnsureRelPathNormBackfill populates rel_path_norm for historical rows once.
//
// Sparse NULL values are part of the steady-state design, so a metadata marker
// records when the one-time scan has completed.
func EnsureRelPathNormBackfill(ctx context.Context, db *sql.DB) error {
	done, err := relPathNormBackfillDone(ctx, db)
	if err != nil || done {
		return err
	}

	var lastRowID int64
	for {
		rows, err := db.QueryContext(ctx, `
			SELECT rowid, rel_path, rel_path_norm
			FROM entries
			WHERE rowid > ?
			ORDER BY rowid
			LIMIT ?
		`, lastRowID, relPathNormBackfillBatchSize)
		if err != nil {
			return err
		}

		type pendingUpdate struct {
			rowID      int64
			normalized string
		}

		var (
			updates  []pendingUpdate
			rowCount int
		)
		for rows.Next() {
			var (
				rowID          int64
				relPath        string
				relPathNormRaw sql.NullString
			)
			if err := rows.Scan(&rowID, &relPath, &relPathNormRaw); err != nil {
				rows.Close()
				return err
			}
			lastRowID = rowID
			rowCount++
			if relPathNormRaw.Valid && relPathNormRaw.String != "" {
				continue
			}
			if normalized, ok := storedRelPathNorm(relPath); ok {
				updates = append(updates, pendingUpdate{
					rowID:      rowID,
					normalized: normalized,
				})
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()

		if rowCount == 0 {
			break
		}

		if len(updates) == 0 {
			continue
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		stmt, err := tx.PrepareContext(ctx, "UPDATE entries SET rel_path_norm = ? WHERE rowid = ?")
		if err != nil {
			tx.Rollback()
			return err
		}
		for _, update := range updates {
			if _, err := stmt.ExecContext(ctx, update.normalized, update.rowID); err != nil {
				stmt.Close()
				tx.Rollback()
				return err
			}
		}
		if err := stmt.Close(); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return markRelPathNormBackfillDone(ctx, db)
}

func relPathNormBackfillDone(ctx context.Context, db *sql.DB) (bool, error) {
	var value string
	err := db.QueryRowContext(ctx,
		"SELECT value FROM metadata WHERE key = ?",
		metadataRelPathNormBackfillV1Key,
	).Scan(&value)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return value == relPathNormBackfillDoneValue, nil
}

func markRelPathNormBackfillDone(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO metadata (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		metadataRelPathNormBackfillV1Key,
		relPathNormBackfillDoneValue,
	)
	return err
}
