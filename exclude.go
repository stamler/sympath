package inventory

// exclude.go handles detection and exclusion of the SQLite database file
// and its companions (WAL, SHM, journal) from the directory scan.
//
// If the database lives inside the tree being scanned, these files must
// be excluded to avoid:
//   - hashing the DB while it is being written to
//   - recording the DB as a content file
//   - pointless churn on every scan
//
// Path resolution uses filepath.EvalSymlinks to handle macOS symlinks
// (e.g., /var -> /private/var) so that the paths returned by
// PRAGMA database_list match the paths seen by the walker.

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
)

// getDBPath returns the absolute, symlink-resolved file path of the main
// database by querying PRAGMA database_list. Returns an empty string if
// the database is in-memory or the path cannot be determined.
func getDBPath(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA database_list")
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		var seq int
		var name, file string
		if err := rows.Scan(&seq, &name, &file); err != nil {
			return "", err
		}
		if name == "main" && file != "" {
			return resolveAbsPath(file)
		}
	}
	return "", rows.Err()
}

// resolveAbsPath returns the absolute, symlink-resolved path.
// On macOS, /var → /private/var, so we must resolve symlinks
// to ensure consistent path comparison.
func resolveAbsPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return path, nil
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return abs, nil
	}
	return resolved, nil
}

// makeExcludeSet returns a set of absolute paths that should be excluded
// from scanning: the database file and its WAL/SHM companions.
func makeExcludeSet(dbPath string) map[string]struct{} {
	if dbPath == "" {
		return nil
	}
	return map[string]struct{}{
		dbPath:          {},
		dbPath + "-wal": {},
		dbPath + "-shm": {},
	}
}

// shouldExclude checks whether the given absolute path should be excluded
// from scanning. It first does a direct set lookup, then falls back to
// pattern matching for SQLite companion files (-wal, -shm, -journal) that
// share a directory with a known excluded database file.
func shouldExclude(absPath string, excludeSet map[string]struct{}) bool {
	if excludeSet != nil {
		if _, ok := excludeSet[absPath]; ok {
			return true
		}
	}
	// Fallback: exclude files that look like SQLite DB companions
	base := filepath.Base(absPath)
	if strings.HasSuffix(base, "-wal") || strings.HasSuffix(base, "-shm") ||
		strings.HasSuffix(base, "-journal") {
		// Check if the base DB file is in the exclude set
		if excludeSet != nil {
			dir := filepath.Dir(absPath)
			for p := range excludeSet {
				if filepath.Dir(p) == dir {
					// Same directory as a known excluded file
					dbBase := filepath.Base(p)
					if strings.TrimSuffix(base, "-wal") == dbBase ||
						strings.TrimSuffix(base, "-shm") == dbBase ||
						strings.TrimSuffix(base, "-journal") == dbBase {
						return true
					}
				}
			}
		}
	}
	return false
}
