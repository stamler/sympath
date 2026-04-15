package main

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"sort"
	"strings"
	"unicode/utf8"
)

var ignoredCommonOSBasenames = []string{
	".ds_store",
	"thumbs.db",
	"ehthumbs.db",
	"desktop.ini",
	".directory",
}

type compareResult struct {
	IdenticalCount   int                `json:"identical_count"`
	LeftOnly         []fileEntry        `json:"left_only"`
	LeftOnlyCompact  []fileDisplayEntry `json:"left_only_compact"`
	RightOnly        []fileEntry        `json:"right_only"`
	RightOnlyCompact []fileDisplayEntry `json:"right_only_compact"`
	Different        []fileDiffPair     `json:"different"`
}

type fileEntry struct {
	RelPath string `json:"rel_path"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
}

type fileDisplayEntry struct {
	RelPath   string `json:"rel_path"`
	Size      int64  `json:"size"`
	SHA256    string `json:"sha256"`
	FileCount int    `json:"file_count,omitempty"`
	Collapsed bool   `json:"collapsed,omitempty"`
}

type fileDiffPair struct {
	RelPath string    `json:"rel_path"`
	Left    fileBrief `json:"left"`
	Right   fileBrief `json:"right"`
}

type fileBrief struct {
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

func resolveScanID(ctx context.Context, db *sql.DB, machineID, root string) (int64, error) {
	var scanID int64
	err := db.QueryRowContext(ctx,
		"SELECT current_scan_id FROM roots WHERE machine_id = ? AND root = ?",
		machineID, root,
	).Scan(&scanID)
	if err == sql.ErrNoRows {
		return 0, fmt.Errorf("no scan found for machine %q root %q", machineID, root)
	}
	return scanID, err
}

// normalizePrefix ensures a non-empty prefix ends with "/" so it acts
// as a proper directory filter.
func normalizePrefix(p string) string {
	p = strings.ReplaceAll(p, "\\", "/")
	p = strings.Trim(p, "/")
	if p == "" {
		return ""
	}
	return p + "/"
}

func ignoredCommonOSClause(ignoreCommonOS bool) (string, []any) {
	if !ignoreCommonOS {
		return "", nil
	}

	args := make([]any, 0, len(ignoredCommonOSBasenames))
	placeholders := make([]string, 0, len(ignoredCommonOSBasenames))
	for _, name := range ignoredCommonOSBasenames {
		placeholders = append(placeholders, "?")
		args = append(args, name)
	}

	return fmt.Sprintf(" AND LOWER(name) NOT IN (%s)", strings.Join(placeholders, ", ")), args
}

// entryCTEs returns the WITH clause and args that produce left_entries
// and right_entries filtered by prefix.
func entryCTEs(leftScan, rightScan int64, leftPrefix, rightPrefix string, ignoreCommonOS bool) (string, []any) {
	lp := normalizePrefix(leftPrefix)
	rp := normalizePrefix(rightPrefix)
	joinOnRawPath := lp != "" && lp == rp

	leftCTE, leftArgs := filteredEntriesCTE("left_entries", leftScan, lp, joinOnRawPath, ignoreCommonOS)
	rightCTE, rightArgs := filteredEntriesCTE("right_entries", rightScan, rp, joinOnRawPath, ignoreCommonOS)

	return fmt.Sprintf(`
		WITH %s,
		%s`, leftCTE, rightCTE), append(leftArgs, rightArgs...)
}

func filteredEntriesCTE(name string, scanID int64, prefix string, joinOnRawPath, ignoreCommonOS bool) (string, []any) {
	ignoreClause, ignoreArgs := ignoredCommonOSClause(ignoreCommonOS)

	if prefix == "" {
		return fmt.Sprintf(`%s AS (
			SELECT rel_path AS join_path, rel_path, size, sha256
			FROM entries
			WHERE scan_id = ?
			%s
		)`, name, ignoreClause), append([]any{scanID}, ignoreArgs...)
	}

	// SQLite SUBSTR counts characters, not bytes, so this offset must
	// use rune count to stay aligned with multibyte UTF-8 prefixes.
	start := utf8.RuneCountInString(prefix) + 1
	// SQLite's default BINARY collation compares TEXT byte-wise, so
	// prefix || x'FF' is an exclusive upper bound for every UTF-8 path
	// that begins with prefix.
	if joinOnRawPath {
		return fmt.Sprintf(`%s AS (
			SELECT
				rel_path AS join_path,
				SUBSTR(rel_path, ?) AS rel_path,
				size, sha256
			FROM entries
			WHERE scan_id = ?
			  AND rel_path >= ?
			  AND rel_path < ? || x'FF'
			  %s
		)`, name, ignoreClause), append([]any{start, scanID, prefix, prefix}, ignoreArgs...)
	}

	return fmt.Sprintf(`%s AS (
		SELECT
			SUBSTR(rel_path, ?) AS join_path,
			SUBSTR(rel_path, ?) AS rel_path,
			size, sha256
		FROM entries
		WHERE scan_id = ?
		  AND rel_path >= ?
		  AND rel_path < ? || x'FF'
		  %s
	)`, name, ignoreClause), append([]any{start, start, scanID, prefix, prefix}, ignoreArgs...)
}

func compareRoots(ctx context.Context, db *sql.DB, leftMachine, leftRoot, rightMachine, rightRoot, leftPrefix, rightPrefix string, byContent, ignoreCommonOS bool) (compareResult, error) {
	leftScan, err := resolveScanID(ctx, db, leftMachine, leftRoot)
	if err != nil {
		return compareResult{}, fmt.Errorf("resolve left scan: %w", err)
	}
	rightScan, err := resolveScanID(ctx, db, rightMachine, rightRoot)
	if err != nil {
		return compareResult{}, fmt.Errorf("resolve right scan: %w", err)
	}

	if byContent {
		return compareByContent(ctx, db, leftScan, rightScan, leftPrefix, rightPrefix, ignoreCommonOS)
	}
	return compareByPath(ctx, db, leftScan, rightScan, leftPrefix, rightPrefix, ignoreCommonOS)
}

func compareByPath(ctx context.Context, db *sql.DB, leftScan, rightScan int64, leftPrefix, rightPrefix string, ignoreCommonOS bool) (compareResult, error) {
	cte, args := entryCTEs(leftScan, rightScan, leftPrefix, rightPrefix, ignoreCommonOS)
	identicalPredicate := identicalByPathPredicate("l", "r")

	var result compareResult

	// Identical count.
	err := db.QueryRowContext(ctx, cte+fmt.Sprintf(`
		SELECT COUNT(*) FROM left_entries l
		JOIN right_entries r ON l.join_path = r.join_path
		WHERE %s
	`, identicalPredicate), args...).Scan(&result.IdenticalCount)
	if err != nil {
		return compareResult{}, fmt.Errorf("count identical: %w", err)
	}

	// Left-only files.
	result.LeftOnly, err = queryOnlyFilesCTE(ctx, db, cte, args, "left_entries", "right_entries")
	if err != nil {
		return compareResult{}, fmt.Errorf("query left-only: %w", err)
	}

	// Right-only files.
	result.RightOnly, err = queryOnlyFilesCTE(ctx, db, cte, args, "right_entries", "left_entries")
	if err != nil {
		return compareResult{}, fmt.Errorf("query right-only: %w", err)
	}

	result.LeftOnlyCompact, err = compactMissingTreesCTE(ctx, db, cte, args, result.LeftOnly, "right_entries")
	if err != nil {
		return compareResult{}, fmt.Errorf("compact left-only: %w", err)
	}
	result.RightOnlyCompact, err = compactMissingTreesCTE(ctx, db, cte, args, result.RightOnly, "left_entries")
	if err != nil {
		return compareResult{}, fmt.Errorf("compact right-only: %w", err)
	}

	// Different files.
	diffRows, err := db.QueryContext(ctx, cte+fmt.Sprintf(`
		SELECT l.rel_path,
		       l.size, COALESCE(l.sha256, ''),
		       r.size, COALESCE(r.sha256, '')
		FROM left_entries l
		JOIN right_entries r ON l.join_path = r.join_path
		WHERE NOT (%s)
		ORDER BY l.rel_path
	`, identicalPredicate), args...)
	if err != nil {
		return compareResult{}, fmt.Errorf("query different: %w", err)
	}
	defer diffRows.Close()

	for diffRows.Next() {
		var d fileDiffPair
		if err := diffRows.Scan(
			&d.RelPath,
			&d.Left.Size, &d.Left.SHA256,
			&d.Right.Size, &d.Right.SHA256,
		); err != nil {
			return compareResult{}, err
		}
		result.Different = append(result.Different, d)
	}
	if err := diffRows.Err(); err != nil {
		return compareResult{}, err
	}

	return ensureNonNilSlices(result), nil
}

func identicalByPathPredicate(leftAlias, rightAlias string) string {
	return fmt.Sprintf(`%[1]s.size = %[2]s.size
		  AND NULLIF(%[1]s.sha256, '') IS NOT NULL
		  AND NULLIF(%[2]s.sha256, '') IS NOT NULL
		  AND %[1]s.sha256 = %[2]s.sha256`, leftAlias, rightAlias)
}

// compareByContent matches files by sha256 regardless of path.
//
// A file's content (sha256) is either present in both trees or only in
// one. There is no "different" category — files match by content or
// they don't. Identical files are counted. Left-only and right-only
// entries show the path where the unmatched content lives.
//
// When the same sha256 appears N times on the left and M times on the
// right, min(N,M) copies are counted as identical and the remaining
// |N-M| copies appear as left-only or right-only.
func compareByContent(ctx context.Context, db *sql.DB, leftScan, rightScan int64, leftPrefix, rightPrefix string, ignoreCommonOS bool) (compareResult, error) {
	cte, args := entryCTEs(leftScan, rightScan, leftPrefix, rightPrefix, ignoreCommonOS)

	var result compareResult

	// Count matched content: for each sha256, min(left count, right count).
	err := db.QueryRowContext(ctx, cte+`
		SELECT COALESCE(SUM(matched), 0) FROM (
			SELECT MIN(l.cnt, r.cnt) AS matched
			FROM (SELECT sha256, COUNT(*) AS cnt FROM left_entries WHERE sha256 IS NOT NULL AND sha256 != '' GROUP BY sha256) l
			JOIN (SELECT sha256, COUNT(*) AS cnt FROM right_entries WHERE sha256 IS NOT NULL AND sha256 != '' GROUP BY sha256) r
			  ON l.sha256 = r.sha256
		)
	`, args...).Scan(&result.IdenticalCount)
	if err != nil {
		return compareResult{}, fmt.Errorf("count identical by content: %w", err)
	}

	// Left-only: content not present on the right at all, OR excess
	// copies when left has more than right.
	//
	// Strategy: for each sha256 on the left, compute how many are
	// unmatched = left_count - min(left_count, right_count). Then
	// pick that many rows (by rowid order) from left_entries.
	result.LeftOnly, err = queryContentOnly(ctx, db, cte, args, "left_entries", "right_entries")
	if err != nil {
		return compareResult{}, fmt.Errorf("query left-only by content: %w", err)
	}

	result.RightOnly, err = queryContentOnly(ctx, db, cte, args, "right_entries", "left_entries")
	if err != nil {
		return compareResult{}, fmt.Errorf("query right-only by content: %w", err)
	}

	// No "different" category in content mode.
	return ensureNonNilSlices(result), nil
}

func queryContentOnly(ctx context.Context, db *sql.DB, cte string, args []any, present, absent string) ([]fileEntry, error) {
	// For each sha256 in `present`, figure out how many excess copies
	// exist vs `absent`. Then return those excess rows.
	//
	// unmatched_per_hash = present_count - COALESCE(absent_count, 0)
	// We take the first `unmatched_per_hash` rows (ordered by rel_path)
	// for each sha256 using a window function.
	query := fmt.Sprintf(cte+`,
		present_counts AS (
			SELECT sha256, COUNT(*) AS cnt
			FROM %[1]s
			WHERE sha256 IS NOT NULL AND sha256 != ''
			GROUP BY sha256
		),
		absent_counts AS (
			SELECT sha256, COUNT(*) AS cnt
			FROM %[2]s
			WHERE sha256 IS NOT NULL AND sha256 != ''
			GROUP BY sha256
		),
		unmatched AS (
			SELECT p.sha256, p.cnt - COALESCE(a.cnt, 0) AS excess
			FROM present_counts p
			LEFT JOIN absent_counts a ON p.sha256 = a.sha256
			WHERE p.cnt > COALESCE(a.cnt, 0)
		),
		ranked AS (
			SELECT e.rel_path, e.size, e.sha256,
			       ROW_NUMBER() OVER (PARTITION BY e.sha256 ORDER BY e.rel_path) AS rn
			FROM %[1]s e
			JOIN unmatched u ON e.sha256 = u.sha256
		)
		SELECT rel_path, size, COALESCE(sha256, '')
		FROM ranked r
		WHERE rn > (
			SELECT (SELECT cnt FROM present_counts WHERE sha256 = r.sha256) - excess
			FROM unmatched WHERE sha256 = r.sha256
		)
		ORDER BY rel_path
	`, present, absent)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []fileEntry
	for rows.Next() {
		var e fileEntry
		if err := rows.Scan(&e.RelPath, &e.Size, &e.SHA256); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}

	// Also include files with NULL/empty sha256 — they can never match.
	nullQuery := fmt.Sprintf(cte+`
		SELECT rel_path, size, COALESCE(sha256, '')
		FROM %s
		WHERE sha256 IS NULL OR sha256 = ''
		ORDER BY rel_path
	`, present)
	nullRows, err := db.QueryContext(ctx, nullQuery, args...)
	if err != nil {
		return nil, err
	}
	defer nullRows.Close()
	for nullRows.Next() {
		var e fileEntry
		if err := nullRows.Scan(&e.RelPath, &e.Size, &e.SHA256); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}

	return entries, nil
}

func queryOnlyFilesCTE(ctx context.Context, db *sql.DB, cte string, args []any, present, absent string) ([]fileEntry, error) {
	query := fmt.Sprintf(cte+`
		SELECT l.rel_path, l.size, COALESCE(l.sha256, '')
		FROM %s l
		LEFT JOIN %s r ON l.join_path = r.join_path
		WHERE r.join_path IS NULL
		ORDER BY l.rel_path
	`, present, absent)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []fileEntry
	for rows.Next() {
		var e fileEntry
		if err := rows.Scan(&e.RelPath, &e.Size, &e.SHA256); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

func compactMissingTreesCTE(ctx context.Context, db *sql.DB, cte string, args []any, entries []fileEntry, oppositeTable string) ([]fileDisplayEntry, error) {
	if len(entries) == 0 {
		return []fileDisplayEntry{}, nil
	}

	subtreeCounts := countMissingTreeDirs(entries)
	candidateDirs := collapseCandidateDirs(subtreeCounts)
	if len(candidateDirs) == 0 {
		return rawDisplayEntries(entries), nil
	}

	blockedDirs, err := queryBlockedDirsCTE(ctx, db, cte, args, oppositeTable, candidateDirs)
	if err != nil {
		return nil, err
	}

	return compactMissingTrees(entries, subtreeCounts, blockedDirs), nil
}

func queryBlockedDirsCTE(ctx context.Context, db *sql.DB, cte string, args []any, table string, candidateDirs []string) (map[string]struct{}, error) {
	blockedDirs := make(map[string]struct{})
	if len(candidateDirs) == 0 {
		return blockedDirs, nil
	}

	const candidateDirBatchSize = 400

	for start := 0; start < len(candidateDirs); start += candidateDirBatchSize {
		end := start + candidateDirBatchSize
		if end > len(candidateDirs) {
			end = len(candidateDirs)
		}

		batch := candidateDirs[start:end]
		values := make([]string, 0, len(batch))
		queryArgs := append([]any{}, args...)
		for _, dir := range batch {
			values = append(values, "(?)")
			queryArgs = append(queryArgs, dir)
		}

		query := fmt.Sprintf(cte+`,
			candidate_dirs(dir) AS (
				VALUES %s
			)
			SELECT DISTINCT c.dir
			FROM candidate_dirs c
			JOIN %s o
			  ON o.rel_path = c.dir
			  OR (
				o.rel_path >= c.dir || '/'
				AND o.rel_path < c.dir || '/' || x'FF'
			  )
		`, strings.Join(values, ", "), table)

		rows, err := db.QueryContext(ctx, query, queryArgs...)
		if err != nil {
			return nil, err
		}

		for rows.Next() {
			var dir string
			if err := rows.Scan(&dir); err != nil {
				rows.Close()
				return nil, err
			}
			blockedDirs[dir] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}

	return blockedDirs, nil
}

func compactMissingTrees(entries []fileEntry, subtreeCounts map[string]int, blockedDirs map[string]struct{}) []fileDisplayEntry {
	if len(entries) == 0 {
		return []fileDisplayEntry{}
	}

	compact := make([]fileDisplayEntry, 0, len(entries))
	emittedDirs := make(map[string]struct{})
	for _, entry := range entries {
		if dir := topmostCollapsibleDir(entry.RelPath, subtreeCounts, blockedDirs); dir != "" {
			if _, ok := emittedDirs[dir]; !ok {
				compact = append(compact, fileDisplayEntry{
					RelPath:   dir + "/*",
					FileCount: subtreeCounts[dir],
					Collapsed: true,
				})
				emittedDirs[dir] = struct{}{}
			}
			continue
		}

		compact = append(compact, fileDisplayEntry{
			RelPath: entry.RelPath,
			Size:    entry.Size,
			SHA256:  entry.SHA256,
		})
	}

	return compact
}

func countMissingTreeDirs(entries []fileEntry) map[string]int {
	subtreeCounts := make(map[string]int)
	for _, entry := range entries {
		for _, dir := range ancestorDirs(entry.RelPath) {
			subtreeCounts[dir]++
		}
	}
	return subtreeCounts
}

func collapseCandidateDirs(subtreeCounts map[string]int) []string {
	candidateDirs := make([]string, 0, len(subtreeCounts))
	for dir, count := range subtreeCounts {
		if count >= 2 {
			candidateDirs = append(candidateDirs, dir)
		}
	}
	sort.Strings(candidateDirs)
	return candidateDirs
}

func rawDisplayEntries(entries []fileEntry) []fileDisplayEntry {
	display := make([]fileDisplayEntry, 0, len(entries))
	for _, entry := range entries {
		display = append(display, fileDisplayEntry{
			RelPath: entry.RelPath,
			Size:    entry.Size,
			SHA256:  entry.SHA256,
		})
	}
	return display
}

func topmostCollapsibleDir(relPath string, subtreeCounts map[string]int, blockedDirs map[string]struct{}) string {
	dirs := ancestorDirs(relPath)
	for i := len(dirs) - 1; i >= 0; i-- {
		dir := dirs[i]
		if subtreeCounts[dir] < 2 {
			continue
		}
		if _, blocked := blockedDirs[dir]; blocked {
			continue
		}
		return dir
	}
	return ""
}

func ancestorDirs(relPath string) []string {
	var dirs []string
	for dir := path.Dir(relPath); dir != "." && dir != ""; dir = path.Dir(dir) {
		dirs = append(dirs, dir)
	}
	return dirs
}

func ensureNonNilSlices(r compareResult) compareResult {
	if r.LeftOnly == nil {
		r.LeftOnly = []fileEntry{}
	}
	if r.LeftOnlyCompact == nil {
		r.LeftOnlyCompact = []fileDisplayEntry{}
	}
	if r.RightOnly == nil {
		r.RightOnly = []fileEntry{}
	}
	if r.RightOnlyCompact == nil {
		r.RightOnlyCompact = []fileDisplayEntry{}
	}
	if r.Different == nil {
		r.Different = []fileDiffPair{}
	}
	return r
}
