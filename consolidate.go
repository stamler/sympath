package inventory

// consolidate.go provides the machine-aware import helpers used by the
// CLI startup consolidator.
//
// The key rule is that consolidation copies only authoritative current
// scans from each source database. Importing the same remote DB on every
// startup must therefore be idempotent: a machine/path pair in the
// source database replaces the target's existing snapshot for that same
// machine/path pair instead of accumulating duplicate scans over time.

import (
	"context"
	"database/sql"
	"sort"
)

// ImportSummary describes what [ImportCurrentScans] copied.
type ImportSummary struct {
	MachineIDs []string
	Roots      int
}

type currentScanRecord struct {
	ScanID        int64
	MachineID     string
	Hostname      string
	Root          string
	StartedAt     int64
	FinishedAt    sql.NullInt64
	Status        string
	GOOS          sql.NullString
	GOARCH        sql.NullString
	FSType        sql.NullString
	CaseSensitive sql.NullInt64
}

// ImportCurrentScans copies the authoritative current scans from source
// into target.
//
// For each `(machine_id, root)` pair in source, any existing target data
// for that same key is removed before the incoming scan and entries are
// inserted. This makes repeated consolidation runs safe and idempotent.
func ImportCurrentScans(ctx context.Context, target, source *sql.DB) (ImportSummary, error) {
	rows, err := source.QueryContext(ctx, `
		SELECT
			s.scan_id,
			s.machine_id,
			s.hostname,
			s.root,
			s.started_at,
			s.finished_at,
			s.status,
			s.goos,
			s.goarch,
			s.fs_type,
			s.case_sensitive
		FROM roots r
		JOIN scans s ON s.scan_id = r.current_scan_id
		ORDER BY s.machine_id, s.root
	`)
	if err != nil {
		return ImportSummary{}, err
	}
	defer rows.Close()

	var records []currentScanRecord
	machineSet := make(map[string]struct{})
	for rows.Next() {
		var rec currentScanRecord
		if err := rows.Scan(
			&rec.ScanID,
			&rec.MachineID,
			&rec.Hostname,
			&rec.Root,
			&rec.StartedAt,
			&rec.FinishedAt,
			&rec.Status,
			&rec.GOOS,
			&rec.GOARCH,
			&rec.FSType,
			&rec.CaseSensitive,
		); err != nil {
			return ImportSummary{}, err
		}
		records = append(records, rec)
		machineSet[rec.MachineID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return ImportSummary{}, err
	}

	for _, rec := range records {
		if err := importCurrentScan(ctx, target, source, rec); err != nil {
			return ImportSummary{}, err
		}
	}

	machineIDs := make([]string, 0, len(machineSet))
	for machineID := range machineSet {
		machineIDs = append(machineIDs, machineID)
	}
	sort.Strings(machineIDs)

	return ImportSummary{
		MachineIDs: machineIDs,
		Roots:      len(records),
	}, nil
}

// DeleteMachineData removes all scans, entries, and root pointers for the
// supplied machine IDs from db.
func DeleteMachineData(ctx context.Context, db *sql.DB, machineIDs []string) error {
	seen := make(map[string]struct{})
	for _, machineID := range machineIDs {
		if machineID == "" {
			continue
		}
		if _, ok := seen[machineID]; ok {
			continue
		}
		seen[machineID] = struct{}{}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			"DELETE FROM entries WHERE scan_id IN (SELECT scan_id FROM scans WHERE machine_id = ?)",
			machineID,
		); err != nil {
			tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM roots WHERE machine_id = ?", machineID); err != nil {
			tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM scans WHERE machine_id = ?", machineID); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func importCurrentScan(ctx context.Context, target, source *sql.DB, rec currentScanRecord) error {
	tx, err := target.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		"DELETE FROM entries WHERE scan_id IN (SELECT scan_id FROM scans WHERE machine_id = ? AND root = ?)",
		rec.MachineID, rec.Root,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM roots WHERE machine_id = ? AND root = ?", rec.MachineID, rec.Root); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM scans WHERE machine_id = ? AND root = ?", rec.MachineID, rec.Root); err != nil {
		return err
	}

	result, err := tx.ExecContext(ctx, `
		INSERT INTO scans (
			machine_id,
			hostname,
			root,
			started_at,
			finished_at,
			status,
			goos,
			goarch,
			fs_type,
			case_sensitive
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		rec.MachineID,
		rec.Hostname,
		rec.Root,
		rec.StartedAt,
		nullInt64Value(rec.FinishedAt),
		rec.Status,
		nullStringValue(rec.GOOS),
		nullStringValue(rec.GOARCH),
		nullStringValue(rec.FSType),
		nullInt64Value(rec.CaseSensitive),
	)
	if err != nil {
		return err
	}
	newScanID, err := result.LastInsertId()
	if err != nil {
		return err
	}

	entryRows, err := source.QueryContext(ctx, `
		SELECT rel_path, name, ext, size, mtime_ns, fingerprint, sha256, state, err
		FROM entries
		WHERE scan_id = ?
		ORDER BY rel_path
	`, rec.ScanID)
	if err != nil {
		return err
	}
	defer entryRows.Close()

	insertEntryStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO entries (
			scan_id, rel_path, name, ext, size, mtime_ns, fingerprint, sha256, state, err
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer insertEntryStmt.Close()

	for entryRows.Next() {
		var relPath, name, ext string
		var size, mtimeNS int64
		var fingerprint, sha256, state, errText sql.NullString
		if err := entryRows.Scan(
			&relPath,
			&name,
			&ext,
			&size,
			&mtimeNS,
			&fingerprint,
			&sha256,
			&state,
			&errText,
		); err != nil {
			return err
		}
		if _, err := insertEntryStmt.ExecContext(
			ctx,
			newScanID,
			relPath,
			name,
			ext,
			size,
			mtimeNS,
			nullStringValue(fingerprint),
			nullStringValue(sha256),
			nullStringValue(state),
			nullStringValue(errText),
		); err != nil {
			return err
		}
	}
	if err := entryRows.Err(); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO roots (machine_id, root, current_scan_id) VALUES (?, ?, ?)
		ON CONFLICT(machine_id, root) DO UPDATE SET current_scan_id = excluded.current_scan_id
	`, rec.MachineID, rec.Root, newScanID); err != nil {
		return err
	}

	return tx.Commit()
}

func nullStringValue(v sql.NullString) any {
	if v.Valid {
		return v.String
	}
	return nil
}

func nullInt64Value(v sql.NullInt64) any {
	if v.Valid {
		return v.Int64
	}
	return nil
}
