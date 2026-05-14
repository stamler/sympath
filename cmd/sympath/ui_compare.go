// ui_compare implements the compare and duplicate query layer that powers the
// UI endpoints in this package.
//
// # Executive summary
//
// This file answers two related UI questions over the inventory database:
//
// 1. "How do these two scanned trees compare?"
// 2. "Which files inside this scanned tree are duplicates by content?"
//
// The compare path is the more involved of the two. It has to preserve several
// competing goals at once:
//
//   - exact raw-path matching should stay fast on large mirrored trees
//   - Unicode-normalized path matching must still work when raw paths differ only
//     by normalization
//   - compact-view directory collapsing must stay semantically correct
//   - production should avoid known pathological query shapes on low-overlap or
//     prefix-remapped compares
//
// # High-level compare flow
//
// 1. Resolve each requested machine/root pair to a concrete scan ID.
// 2. Choose compare-by-content or compare-by-path.
// 3. For compare-by-path:
//   - choose between the in-memory fully materialized path and the SQL-first path
//     using a benchmark-driven overlap heuristic
//   - run exact raw-path matching first
//   - run normalization-aware fallback only on rows that survived the exact
//     raw-path phase
//   - compute compact-view rows while preserving blocked-directory semantics
//
// 4. Return stable JSON-friendly result slices for the UI.
//
// # Why two compare-by-path implementations exist
//
// The in-memory implementation is simple and robust: load both filtered trees into
// Go, do exact raw-path matching, then do normalization-aware fallback. That is
// still the fastest production choice for some workloads, especially
// low-overlap and prefix-remapped compares.
//
// The SQL-first implementation moves broad exact-path work back into SQLite so
// exact-heavy mirrored compares do not need to materialize both full trees
// before classifying obvious matches. It then narrows the in-memory
// normalization-aware work to the raw-unmatched remainder.
//
// The production selector is intentionally conservative. Benchmarks against
// real copied ~/.sympath data showed that neither strategy dominates across
// every workload shape, so this file treats strategy selection as part of the
// compare algorithm rather than an implementation detail.
//
// The duplicate path is simpler: it uses SQL grouping by size and sha256 to
// return duplicate content groups, optionally scoped by prefix and with the
// same ignore-common-OS filter used by compare.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"sort"
	"strings"
	"unicode/utf8"

	inventory "sympath"
)

var ignoredCommonOSBasenames = []string{
	".ds_store",
	"thumbs.db",
	"ehthumbs.db",
	"desktop.ini",
	".directory",
}

var ignoredCommonOSDirnames = []string{
	"@eadir",
}

type compareResult struct {
	IdenticalCount   int                `json:"identical_count"`
	LeftOnly         []fileEntry        `json:"left_only"`
	LeftOnlyCompact  []fileDisplayEntry `json:"left_only_compact"`
	RightOnly        []fileEntry        `json:"right_only"`
	RightOnlyCompact []fileDisplayEntry `json:"right_only_compact"`
	Different        []fileDiffPair     `json:"different"`
}

type duplicatesResult struct {
	Groups []duplicateGroup `json:"groups"`
}

type duplicateGroup struct {
	Size   int64           `json:"size"`
	SHA256 string          `json:"sha256"`
	Files  []duplicateFile `json:"files"`
}

type duplicateFile struct {
	RelPath string `json:"rel_path"`
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

type comparePathEntry struct {
	RawRelPath     string
	CompareRelPath string
	Size           int64
	SHA256         string
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

// ignoredCommonOSClause appends the opt-in filter for well-known OS metadata
// files. It is used only in query shapes where the referenced name column is
// unambiguous.
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

	clause := fmt.Sprintf(" AND LOWER(name) NOT IN (%s)", strings.Join(placeholders, ", "))
	dirClause, dirArgs := ignoredCommonOSDirClause("rel_path")
	return clause + dirClause, append(args, dirArgs...)
}

func ignoredCommonOSDirClause(relPathExpr string) (string, []any) {
	args := make([]any, 0, len(ignoredCommonOSDirnames)*2)
	var clause strings.Builder
	for _, name := range ignoredCommonOSDirnames {
		clause.WriteString(fmt.Sprintf(" AND LOWER(%s) NOT LIKE ?", relPathExpr))
		args = append(args, name+"/%")
		clause.WriteString(fmt.Sprintf(" AND LOWER(%s) NOT LIKE ?", relPathExpr))
		args = append(args, "%/"+name+"/%")
	}
	return clause.String(), args
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

func singleEntryCTE(name string, scanID int64, prefix string, ignoreCommonOS bool) (string, []any) {
	cte, args := filteredEntriesCTE(name, scanID, normalizePrefix(prefix), true, ignoreCommonOS)
	return "WITH " + cte, args
}

// filteredEntriesCTE produces one filtered entry relation used by compare and
// duplicate queries.
//
// Every projected row carries three path views:
//   - join_path: the path key used for exact raw-path matching
//   - rel_path: the raw path trimmed relative to the requested prefix
//   - compare_rel_path: the normalization-aware path trimmed relative to the
//     normalization-aware form of the requested prefix
//
// The split between rel_path and compare_rel_path is deliberate. Prefix
// trimming must preserve exact raw-path semantics for phase one while also
// preserving normalization-aware fallback semantics for phase two.
func filteredEntriesCTE(name string, scanID int64, prefix string, joinOnRawPath, ignoreCommonOS bool) (string, []any) {
	ignoreClause, ignoreArgs := ignoredCommonOSClause(ignoreCommonOS)

	if prefix == "" {
		return fmt.Sprintf(`%s AS (
			SELECT rel_path AS join_path, rel_path, COALESCE(rel_path_norm, rel_path) AS compare_rel_path, size, sha256
			FROM entries
			WHERE scan_id = ?
			%s
		)`, name, ignoreClause), append([]any{scanID}, ignoreArgs...)
	}

	// SQLite SUBSTR counts characters, not bytes, so this offset must
	// use rune count to stay aligned with multibyte UTF-8 prefixes.
	rawStart := utf8.RuneCountInString(prefix) + 1
	comparePrefix := inventory.CompareRelPathKey(prefix)
	compareStart := utf8.RuneCountInString(comparePrefix) + 1
	// SQLite's default BINARY collation compares TEXT byte-wise, so
	// prefix || x'FF' is an exclusive upper bound for every UTF-8 path
	// that begins with prefix.
	if joinOnRawPath {
		return fmt.Sprintf(`%s AS (
			SELECT
				rel_path AS join_path,
				SUBSTR(rel_path, ?) AS rel_path,
				SUBSTR(COALESCE(rel_path_norm, rel_path), ?) AS compare_rel_path,
				size, sha256
			FROM entries
			WHERE scan_id = ?
			  AND rel_path >= ?
			  AND rel_path < ? || x'FF'
			  %s
		)`, name, ignoreClause), append([]any{rawStart, compareStart, scanID, prefix, prefix}, ignoreArgs...)
	}

	return fmt.Sprintf(`%s AS (
		SELECT
			SUBSTR(rel_path, ?) AS join_path,
			SUBSTR(rel_path, ?) AS rel_path,
			SUBSTR(COALESCE(rel_path_norm, rel_path), ?) AS compare_rel_path,
			size, sha256
		FROM entries
		WHERE scan_id = ?
		  AND rel_path >= ?
		  AND rel_path < ? || x'FF'
		  %s
	)`, name, ignoreClause), append([]any{rawStart, rawStart, compareStart, scanID, prefix, prefix}, ignoreArgs...)
}

// ignoredCommonOSClauseForAlias is the aliased form of
// ignoredCommonOSClause. It exists for query shapes that address base tables
// directly instead of going through left_entries/right_entries CTE names.
func ignoredCommonOSClauseForAlias(alias string, ignoreCommonOS bool) (string, []any) {
	if !ignoreCommonOS {
		return "", nil
	}

	args := make([]any, 0, len(ignoredCommonOSBasenames))
	placeholders := make([]string, 0, len(ignoredCommonOSBasenames))
	for _, name := range ignoredCommonOSBasenames {
		placeholders = append(placeholders, "?")
		args = append(args, name)
	}

	clause := fmt.Sprintf(" AND LOWER(%s.name) NOT IN (%s)", alias, strings.Join(placeholders, ", "))
	dirClause, dirArgs := ignoredCommonOSDirClause(alias + ".rel_path")
	return clause + dirClause, append(args, dirArgs...)
}

// filteredEntriesWhereClause mirrors filteredEntriesCTE's filtering rules but
// returns only the WHERE predicate and its arguments.
//
// This helper is used by the remapped-prefix SQL-first path, where joining
// directly against base-table entries preserves the primary-key lookup on
// (scan_id, rel_path) better than routing everything through a trimmed join key.
func filteredEntriesWhereClause(alias string, scanID int64, prefix string, ignoreCommonOS bool) (string, []any) {
	ignoreClause, ignoreArgs := ignoredCommonOSClauseForAlias(alias, ignoreCommonOS)

	args := []any{scanID}
	where := fmt.Sprintf("%s.scan_id = ?", alias)
	if prefix != "" {
		where += fmt.Sprintf(" AND %s.rel_path >= ? AND %s.rel_path < ? || x'FF'", alias, alias)
		args = append(args, prefix, prefix)
	}
	where += ignoreClause
	args = append(args, ignoreArgs...)
	return where, args
}

// trimmedRawPathSQL returns the SQL expression that trims a base-table rel_path
// down to the caller-visible raw relative path for the requested prefix.
func trimmedRawPathSQL(alias, prefix string) string {
	if prefix == "" {
		return alias + ".rel_path"
	}
	return fmt.Sprintf("SUBSTR(%s.rel_path, %d)", alias, utf8.RuneCountInString(prefix)+1)
}

// trimmedComparePathSQL returns the SQL expression that trims the
// normalization-aware compare path relative to the normalization-aware form of
// the requested prefix.
//
// This cannot reuse the raw prefix offset directly because a Unicode-normalized
// prefix may have a different rune length than the raw prefix.
func trimmedComparePathSQL(alias, prefix string) string {
	if prefix == "" {
		return fmt.Sprintf("COALESCE(%s.rel_path_norm, %s.rel_path)", alias, alias)
	}
	comparePrefix := inventory.CompareRelPathKey(prefix)
	return fmt.Sprintf("SUBSTR(COALESCE(%s.rel_path_norm, %s.rel_path), %d)", alias, alias, utf8.RuneCountInString(comparePrefix)+1)
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

func findDuplicates(ctx context.Context, db *sql.DB, machineID, root, prefix string, ignoreCommonOS bool) (duplicatesResult, error) {
	scanID, err := resolveScanID(ctx, db, machineID, root)
	if err != nil {
		return duplicatesResult{}, fmt.Errorf("resolve scan: %w", err)
	}

	cte, args := singleEntryCTE("target_entries", scanID, prefix, ignoreCommonOS)
	rows, err := db.QueryContext(ctx, cte+`
		, duplicate_keys AS (
			SELECT size, sha256
			FROM target_entries
			WHERE NULLIF(sha256, '') IS NOT NULL
			GROUP BY size, sha256
			HAVING COUNT(*) >= 2
		)
		SELECT t.rel_path, t.size, COALESCE(t.sha256, '')
		FROM target_entries t
		JOIN duplicate_keys d
		  ON d.size = t.size
		 AND d.sha256 = t.sha256
		ORDER BY t.size DESC, t.sha256 ASC, t.rel_path ASC
	`, args...)
	if err != nil {
		return duplicatesResult{}, fmt.Errorf("query duplicates: %w", err)
	}
	defer rows.Close()

	var result duplicatesResult
	groupIndex := -1
	var lastSize int64
	var lastSHA string
	for rows.Next() {
		var relPath string
		var size int64
		var sha string
		if err := rows.Scan(&relPath, &size, &sha); err != nil {
			return duplicatesResult{}, err
		}
		if groupIndex == -1 || size != lastSize || sha != lastSHA {
			result.Groups = append(result.Groups, duplicateGroup{
				Size:   size,
				SHA256: sha,
				Files:  []duplicateFile{},
			})
			groupIndex = len(result.Groups) - 1
			lastSize = size
			lastSHA = sha
		}
		result.Groups[groupIndex].Files = append(result.Groups[groupIndex].Files, duplicateFile{RelPath: relPath})
	}
	if err := rows.Err(); err != nil {
		return duplicatesResult{}, err
	}

	return ensureNonNilDuplicateGroups(result), nil
}

// compareByPath chooses between two internal implementations that have the
// same external semantics but very different performance profiles.
//
// The in-memory path is simple and robust: it loads both filtered trees into
// memory, does exact raw-path matching first, then performs a
// normalization-aware fallback pass over the remaining rows. That shape is
// fast when most rows are unmatched or when the caller compares different raw
// prefixes that only line up after trimming.
//
// The SQL-first path moves the exact raw-path work back into SQLite and only
// materializes the raw-unmatched remainder for normalization-aware matching.
// Benchmarks on real user inventories showed that this sharply reduces
// allocations and peak heap growth when two trees are largely exact mirrors,
// but it can be much slower on low-overlap or prefix-remapped compares.
//
// Because neither implementation dominates across every workload, production
// selects a strategy at runtime using a lightweight overlap probe.
func compareByPath(ctx context.Context, db *sql.DB, leftScan, rightScan int64, leftPrefix, rightPrefix string, ignoreCommonOS bool) (compareResult, error) {
	useSQLFirst, err := shouldUseSQLFirstPathCompare(ctx, db, leftScan, rightScan, leftPrefix, rightPrefix, ignoreCommonOS)
	if err != nil {
		return compareResult{}, fmt.Errorf("choose compare-by-path strategy: %w", err)
	}
	if !useSQLFirst {
		return compareByPathInMemory(ctx, db, leftScan, rightScan, leftPrefix, rightPrefix, ignoreCommonOS)
	}
	return compareByPathSQLFirst(ctx, db, leftScan, rightScan, leftPrefix, rightPrefix, ignoreCommonOS)
}

// shouldUseSQLFirstPathCompare keeps the SQL-first path on a short leash.
//
// The first gate is semantic: if the prefixes do not collapse to the same
// compare-space prefix, the SQL-first implementation is a poor fit because the
// raw-path join no longer represents the dominant work. In those cases the
// fully in-memory matcher is consistently safer.
//
// When the compare prefixes do align, we still require high exact raw-path
// overlap before using SQL-first. The threshold is intentionally conservative:
// benchmarks showed that SQL-first is worthwhile when the exact raw-path phase
// handles most rows, but can regress badly when large unmatched sets still need
// to flow through fallback and compact-view processing.
func shouldUseSQLFirstPathCompare(ctx context.Context, db *sql.DB, leftScan, rightScan int64, leftPrefix, rightPrefix string, ignoreCommonOS bool) (bool, error) {
	if normalizePrefix(leftPrefix) != normalizePrefix(rightPrefix) {
		// Prefix-remapped SQL-first compare is no longer pathological, but the
		// current benchmarks still favor the in-memory path for production use.
		return false, nil
	}

	cte, args := entryCTEs(leftScan, rightScan, leftPrefix, rightPrefix, ignoreCommonOS)
	leftCount, rightCount, exactMatches, err := queryExactMatchStatsCTE(ctx, db, cte, args)
	if err != nil {
		return false, err
	}
	smallerSide := min(leftCount, rightCount)
	if smallerSide == 0 {
		return true, nil
	}

	// The SQL-first path pays off when exact raw-path overlap dominates and
	// normalization fallback only needs to inspect a small remainder.
	return exactMatches*100 >= smallerSide*80, nil
}

// queryExactMatchStatsCTE fetches just enough information to decide whether the
// SQL-first path is likely to pay for itself. We deliberately keep this probe
// small so the strategy decision does not erase the benefits of the faster
// implementation on exact-heavy workloads.
func queryExactMatchStatsCTE(ctx context.Context, db *sql.DB, cte string, args []any) (int, int, int, error) {
	var leftCount, rightCount, exactMatches int
	err := db.QueryRowContext(ctx, cte+`
		SELECT
			(SELECT COUNT(*) FROM left_entries),
			(SELECT COUNT(*) FROM right_entries),
			(SELECT COUNT(*) FROM left_entries l JOIN right_entries r ON l.join_path = r.join_path)
	`, args...).Scan(&leftCount, &rightCount, &exactMatches)
	if err != nil {
		return 0, 0, 0, err
	}
	return leftCount, rightCount, exactMatches, nil
}

// compareByPathSQLFirst restores the old SQL fast path for exact raw-path
// matching while preserving the newer normalization-aware correctness fixes.
//
// The intent is to let SQLite handle the broad "same raw path" questions:
// identical count, diff rows, and which rows are unmatched by raw path. Only
// the unmatched remainder is materialized for the normalization-aware fallback.
//
// Compact-view blocking still uses the full filtered opposite tree, not just
// the unmatched remainder. That detail is important: collapse decisions must
// reflect whether the opposite side contains anything under a directory after
// compare semantics are applied, even if those opposite-side rows matched
// earlier in the raw-path phase.
func compareByPathSQLFirst(ctx context.Context, db *sql.DB, leftScan, rightScan int64, leftPrefix, rightPrefix string, ignoreCommonOS bool) (compareResult, error) {
	cte, args := entryCTEs(leftScan, rightScan, leftPrefix, rightPrefix, ignoreCommonOS)
	lp := normalizePrefix(leftPrefix)
	rp := normalizePrefix(rightPrefix)

	// Phase 1 stays entirely in SQLite so exact-heavy compares do not pay to
	// materialize both trees before classifying obvious matches. We stream exact
	// matches and raw unmatched rows from one query so the SQL-first path does
	// not pay for repeated full CTE scans.
	var (
		result       compareResult
		leftEntries  []comparePathEntry
		rightEntries []comparePathEntry
		err          error
	)
	if lp == rp {
		result, leftEntries, rightEntries, err = queryCompareByPathPhaseOneCTE(ctx, db, cte, args)
	} else {
		result, leftEntries, rightEntries, err = queryCompareByPathPhaseOneRemappedPrefixes(ctx, db, leftScan, rightScan, lp, rp, ignoreCommonOS)
	}
	if err != nil {
		return compareResult{}, fmt.Errorf("query compare-by-path phase one: %w", err)
	}

	// Phase 2 reuses the normalization-aware reconciliation logic, but only for
	// rows that survived the raw-path phase.
	leftOnly, rightOnly := reconcileComparePathFallback(&result, leftEntries, rightEntries)
	sortFileEntriesByRelPath(leftOnly)
	sortFileEntriesByRelPath(rightOnly)
	result.LeftOnly = leftOnly
	result.RightOnly = rightOnly

	result.LeftOnlyCompact, err = compactMissingTreesFromComparePathsCTE(ctx, db, cte, args, result.LeftOnly, "right_entries")
	if err != nil {
		return compareResult{}, fmt.Errorf("compact left-only trees: %w", err)
	}
	result.RightOnlyCompact, err = compactMissingTreesFromComparePathsCTE(ctx, db, cte, args, result.RightOnly, "left_entries")
	if err != nil {
		return compareResult{}, fmt.Errorf("compact right-only trees: %w", err)
	}

	sort.Slice(result.Different, func(i, j int) bool {
		return result.Different[i].RelPath < result.Different[j].RelPath
	})

	return ensureNonNilSlices(result), nil
}

type comparePhaseOneRow struct {
	LeftRelPath      sql.NullString
	LeftComparePath  sql.NullString
	LeftSize         sql.NullInt64
	LeftSHA256       string
	RightRelPath     sql.NullString
	RightComparePath sql.NullString
	RightSize        sql.NullInt64
	RightSHA256      string
}

// queryCompareByPathPhaseOneCTE performs exact raw-path matching and raw-path
// unmatched extraction in one streamed pass for the "same raw prefix on both
// sides" case.
//
// This is the simplest SQL-first shape: both sides already agree on the raw
// relative path key, so SQLite can stream exact matches and anti-join rows
// without reconstructing full paths.
func queryCompareByPathPhaseOneCTE(ctx context.Context, db *sql.DB, cte string, args []any) (compareResult, []comparePathEntry, []comparePathEntry, error) {
	rows, err := db.QueryContext(ctx, cte+`
		SELECT
			l.rel_path,
			l.compare_rel_path,
			l.size,
			COALESCE(l.sha256, ''),
			r.rel_path,
			r.compare_rel_path,
			r.size,
			COALESCE(r.sha256, '')
		FROM left_entries l
		LEFT JOIN right_entries r ON l.join_path = r.join_path
		UNION ALL
		SELECT
			NULL,
			NULL,
			NULL,
			'',
			r.rel_path,
			r.compare_rel_path,
			r.size,
			COALESCE(r.sha256, '')
		FROM right_entries r
		LEFT JOIN left_entries l ON l.join_path = r.join_path
		WHERE l.join_path IS NULL
	`, args...)
	if err != nil {
		return compareResult{}, nil, nil, err
	}
	defer rows.Close()

	var result compareResult
	var leftEntries []comparePathEntry
	var rightEntries []comparePathEntry
	for rows.Next() {
		var row comparePhaseOneRow
		if err := rows.Scan(
			&row.LeftRelPath,
			&row.LeftComparePath,
			&row.LeftSize,
			&row.LeftSHA256,
			&row.RightRelPath,
			&row.RightComparePath,
			&row.RightSize,
			&row.RightSHA256,
		); err != nil {
			return compareResult{}, nil, nil, err
		}

		switch {
		case !row.RightRelPath.Valid:
			leftEntries = append(leftEntries, comparePathEntry{
				RawRelPath:     row.LeftRelPath.String,
				CompareRelPath: row.LeftComparePath.String,
				Size:           row.LeftSize.Int64,
				SHA256:         row.LeftSHA256,
			})
		case !row.LeftRelPath.Valid:
			rightEntries = append(rightEntries, comparePathEntry{
				RawRelPath:     row.RightRelPath.String,
				CompareRelPath: row.RightComparePath.String,
				Size:           row.RightSize.Int64,
				SHA256:         row.RightSHA256,
			})
		default:
			left := comparePathEntry{
				RawRelPath:     row.LeftRelPath.String,
				CompareRelPath: row.LeftComparePath.String,
				Size:           row.LeftSize.Int64,
				SHA256:         row.LeftSHA256,
			}
			right := comparePathEntry{
				RawRelPath:     row.RightRelPath.String,
				CompareRelPath: row.RightComparePath.String,
				Size:           row.RightSize.Int64,
				SHA256:         row.RightSHA256,
			}
			if identicalCompareEntries(left, right) {
				result.IdenticalCount++
				continue
			}
			result.Different = append(result.Different, fileDiffPair{
				RelPath: left.RawRelPath,
				Left:    fileBrief{Size: left.Size, SHA256: left.SHA256},
				Right:   fileBrief{Size: right.Size, SHA256: right.SHA256},
			})
		}
	}
	if err := rows.Err(); err != nil {
		return compareResult{}, nil, nil, err
	}
	return result, leftEntries, rightEntries, nil
}

// queryCompareByPathPhaseOneRemappedPrefixes handles compares where the left
// and right prefixes differ after trimming. Instead of joining on a computed
// trimmed join key, it scans the filtered side and probes the opposite side by
// reconstructed full raw path, preserving the primary-key lookup on
// (scan_id, rel_path).
//
// This query shape exists because the straightforward "trim both sides and join
// on join_path" approach benchmarked poorly for prefix-remapped compares. The
// current production selector still routes those workloads to the in-memory path,
// but this specialized SQL-first path is retained both as a benchmarked
// fallback implementation and as a foundation for future tuning.
func queryCompareByPathPhaseOneRemappedPrefixes(ctx context.Context, db *sql.DB, leftScan, rightScan int64, leftPrefix, rightPrefix string, ignoreCommonOS bool) (compareResult, []comparePathEntry, []comparePathEntry, error) {
	leftWhere, leftArgs := filteredEntriesWhereClause("l", leftScan, leftPrefix, ignoreCommonOS)
	rightWhere, rightArgs := filteredEntriesWhereClause("r", rightScan, rightPrefix, ignoreCommonOS)
	leftRaw := trimmedRawPathSQL("l", leftPrefix)
	leftCompare := trimmedComparePathSQL("l", leftPrefix)
	rightRaw := trimmedRawPathSQL("r", rightPrefix)
	rightCompare := trimmedComparePathSQL("r", rightPrefix)

	query := fmt.Sprintf(`
		SELECT
			%s,
			%s,
			l.size,
			COALESCE(l.sha256, ''),
			CASE WHEN r.rel_path IS NULL THEN NULL ELSE %s END,
			CASE WHEN r.rel_path IS NULL THEN NULL ELSE %s END,
			r.size,
			COALESCE(r.sha256, '')
		FROM entries l
		LEFT JOIN entries r
		  ON r.scan_id = ?
		 AND r.rel_path = ? || %s
		WHERE %s
		UNION ALL
		SELECT
			NULL,
			NULL,
			NULL,
			'',
			%s,
			%s,
			r.size,
			COALESCE(r.sha256, '')
		FROM entries r
		LEFT JOIN entries l
		  ON l.scan_id = ?
		 AND l.rel_path = ? || %s
		WHERE %s
		  AND l.rel_path IS NULL
	`, leftRaw, leftCompare, rightRaw, rightCompare, leftRaw, leftWhere, rightRaw, rightCompare, rightRaw, rightWhere)

	args := []any{rightScan, rightPrefix}
	args = append(args, leftArgs...)
	args = append(args, leftScan, leftPrefix)
	args = append(args, rightArgs...)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return compareResult{}, nil, nil, err
	}
	defer rows.Close()

	var result compareResult
	var leftEntries []comparePathEntry
	var rightEntries []comparePathEntry
	for rows.Next() {
		var row comparePhaseOneRow
		if err := rows.Scan(
			&row.LeftRelPath,
			&row.LeftComparePath,
			&row.LeftSize,
			&row.LeftSHA256,
			&row.RightRelPath,
			&row.RightComparePath,
			&row.RightSize,
			&row.RightSHA256,
		); err != nil {
			return compareResult{}, nil, nil, err
		}

		switch {
		case !row.RightRelPath.Valid:
			leftEntries = append(leftEntries, comparePathEntry{
				RawRelPath:     row.LeftRelPath.String,
				CompareRelPath: row.LeftComparePath.String,
				Size:           row.LeftSize.Int64,
				SHA256:         row.LeftSHA256,
			})
		case !row.LeftRelPath.Valid:
			rightEntries = append(rightEntries, comparePathEntry{
				RawRelPath:     row.RightRelPath.String,
				CompareRelPath: row.RightComparePath.String,
				Size:           row.RightSize.Int64,
				SHA256:         row.RightSHA256,
			})
		default:
			left := comparePathEntry{
				RawRelPath:     row.LeftRelPath.String,
				CompareRelPath: row.LeftComparePath.String,
				Size:           row.LeftSize.Int64,
				SHA256:         row.LeftSHA256,
			}
			right := comparePathEntry{
				RawRelPath:     row.RightRelPath.String,
				CompareRelPath: row.RightComparePath.String,
				Size:           row.RightSize.Int64,
				SHA256:         row.RightSHA256,
			}
			if identicalCompareEntries(left, right) {
				result.IdenticalCount++
				continue
			}
			result.Different = append(result.Different, fileDiffPair{
				RelPath: left.RawRelPath,
				Left:    fileBrief{Size: left.Size, SHA256: left.SHA256},
				Right:   fileBrief{Size: right.Size, SHA256: right.SHA256},
			})
		}
	}
	if err := rows.Err(); err != nil {
		return compareResult{}, nil, nil, err
	}
	return result, leftEntries, rightEntries, nil
}

// compareByPathInMemory is the fully materialized implementation retained for
// workloads where the SQL-first path regressed in benchmarks.
//
// Keeping this code path intact is intentional. It serves two purposes:
// preserving the benchmark baseline that motivated the adaptive strategy, and
// acting as the faster production fallback when exact raw-path overlap is too
// low for the SQL-first path to help.
func compareByPathInMemory(ctx context.Context, db *sql.DB, leftScan, rightScan int64, leftPrefix, rightPrefix string, ignoreCommonOS bool) (compareResult, error) {
	leftEntries, err := loadCompareEntries(ctx, db, leftScan, leftPrefix, ignoreCommonOS)
	if err != nil {
		return compareResult{}, fmt.Errorf("load left compare entries: %w", err)
	}
	rightEntries, err := loadCompareEntries(ctx, db, rightScan, rightPrefix, ignoreCommonOS)
	if err != nil {
		return compareResult{}, fmt.Errorf("load right compare entries: %w", err)
	}

	// The in-memory path keeps the whole working set in memory so later phases can
	// revisit it freely without more database round-trips.
	var result compareResult
	rightByRaw := make(map[string]comparePathEntry, len(rightEntries))
	for _, entry := range rightEntries {
		rightByRaw[entry.RawRelPath] = entry
	}

	leftMatched := make(map[string]struct{}, len(leftEntries))
	rightMatched := make(map[string]struct{}, len(rightEntries))

	// Phase 1: preserve exact raw-path behavior before any normalization-aware matching.
	for _, left := range leftEntries {
		right, ok := rightByRaw[left.RawRelPath]
		if !ok {
			continue
		}
		leftMatched[left.RawRelPath] = struct{}{}
		rightMatched[right.RawRelPath] = struct{}{}
		if identicalCompareEntries(left, right) {
			result.IdenticalCount++
			continue
		}
		result.Different = append(result.Different, fileDiffPair{
			RelPath: left.RawRelPath,
			Left:    fileBrief{Size: left.Size, SHA256: left.SHA256},
			Right:   fileBrief{Size: right.Size, SHA256: right.SHA256},
		})
	}

	leftFallbackGroups := make(map[string][]comparePathEntry)
	rightFallbackGroups := make(map[string][]comparePathEntry)
	for _, left := range leftEntries {
		if _, ok := leftMatched[left.RawRelPath]; ok {
			continue
		}
		leftFallbackGroups[left.CompareRelPath] = append(leftFallbackGroups[left.CompareRelPath], left)
	}
	for _, right := range rightEntries {
		if _, ok := rightMatched[right.RawRelPath]; ok {
			continue
		}
		rightFallbackGroups[right.CompareRelPath] = append(rightFallbackGroups[right.CompareRelPath], right)
	}

	// Phase 2: only reconcile normalized keys that are unique on both sides.
	for compareRelPath, leftGroup := range leftFallbackGroups {
		rightGroup := rightFallbackGroups[compareRelPath]
		if len(leftGroup) != 1 || len(rightGroup) != 1 {
			continue
		}
		left := leftGroup[0]
		right := rightGroup[0]
		leftMatched[left.RawRelPath] = struct{}{}
		rightMatched[right.RawRelPath] = struct{}{}
		if identicalCompareEntries(left, right) {
			result.IdenticalCount++
			continue
		}
		result.Different = append(result.Different, fileDiffPair{
			RelPath: compareRelPath,
			Left:    fileBrief{Size: left.Size, SHA256: left.SHA256},
			Right:   fileBrief{Size: right.Size, SHA256: right.SHA256},
		})
	}

	for _, left := range leftEntries {
		if _, ok := leftMatched[left.RawRelPath]; ok {
			continue
		}
		result.LeftOnly = append(result.LeftOnly, fileEntry{
			RelPath: left.RawRelPath,
			Size:    left.Size,
			SHA256:  left.SHA256,
		})
	}
	for _, right := range rightEntries {
		if _, ok := rightMatched[right.RawRelPath]; ok {
			continue
		}
		result.RightOnly = append(result.RightOnly, fileEntry{
			RelPath: right.RawRelPath,
			Size:    right.Size,
			SHA256:  right.SHA256,
		})
	}

	sort.Slice(result.Different, func(i, j int) bool {
		return result.Different[i].RelPath < result.Different[j].RelPath
	})
	result.LeftOnlyCompact = compactMissingTreesFromEntries(result.LeftOnly, rightEntries)
	result.RightOnlyCompact = compactMissingTreesFromEntries(result.RightOnly, leftEntries)

	return ensureNonNilSlices(result), nil
}

// reconcileComparePathFallback performs the normalization-aware second phase
// shared by both compare strategies.
//
// Only compare paths that are unique on both sides are eligible for fallback
// reconciliation. Ambiguous normalization collisions stay unmatched on purpose:
// once multiple raw paths collapse to the same compare path, matching them by
// position would be guesswork and would silently change compare semantics.
func reconcileComparePathFallback(result *compareResult, leftEntries, rightEntries []comparePathEntry) ([]fileEntry, []fileEntry) {
	leftFallbackGroups := make(map[string][]comparePathEntry)
	rightFallbackGroups := make(map[string][]comparePathEntry)
	for _, left := range leftEntries {
		leftFallbackGroups[left.CompareRelPath] = append(leftFallbackGroups[left.CompareRelPath], left)
	}
	for _, right := range rightEntries {
		rightFallbackGroups[right.CompareRelPath] = append(rightFallbackGroups[right.CompareRelPath], right)
	}

	// Phase 2: only reconcile normalized keys that are unique on both sides.
	leftMatched := make(map[string]struct{}, len(leftEntries))
	rightMatched := make(map[string]struct{}, len(rightEntries))
	for compareRelPath, leftGroup := range leftFallbackGroups {
		rightGroup := rightFallbackGroups[compareRelPath]
		if len(leftGroup) != 1 || len(rightGroup) != 1 {
			continue
		}
		left := leftGroup[0]
		right := rightGroup[0]
		leftMatched[left.RawRelPath] = struct{}{}
		rightMatched[right.RawRelPath] = struct{}{}
		if identicalCompareEntries(left, right) {
			result.IdenticalCount++
			continue
		}
		result.Different = append(result.Different, fileDiffPair{
			RelPath: compareRelPath,
			Left:    fileBrief{Size: left.Size, SHA256: left.SHA256},
			Right:   fileBrief{Size: right.Size, SHA256: right.SHA256},
		})
	}

	leftOnly := make([]fileEntry, 0, len(leftEntries))
	for _, left := range leftEntries {
		if _, ok := leftMatched[left.RawRelPath]; ok {
			continue
		}
		leftOnly = append(leftOnly, fileEntry{
			RelPath: left.RawRelPath,
			Size:    left.Size,
			SHA256:  left.SHA256,
		})
	}
	rightOnly := make([]fileEntry, 0, len(rightEntries))
	for _, right := range rightEntries {
		if _, ok := rightMatched[right.RawRelPath]; ok {
			continue
		}
		rightOnly = append(rightOnly, fileEntry{
			RelPath: right.RawRelPath,
			Size:    right.Size,
			SHA256:  right.SHA256,
		})
	}
	return leftOnly, rightOnly
}

func sortFileEntriesByRelPath(entries []fileEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].RelPath != entries[j].RelPath {
			return entries[i].RelPath < entries[j].RelPath
		}
		if entries[i].Size != entries[j].Size {
			return entries[i].Size < entries[j].Size
		}
		return entries[i].SHA256 < entries[j].SHA256
	})
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

func loadCompareEntries(ctx context.Context, db *sql.DB, scanID int64, prefix string, ignoreCommonOS bool) ([]comparePathEntry, error) {
	rawPrefix := normalizePrefix(prefix)
	comparePrefix := inventory.CompareRelPathKey(rawPrefix)
	ignoreClause, ignoreArgs := ignoredCommonOSClause(ignoreCommonOS)

	args := []any{scanID}
	query := `
		SELECT rel_path, COALESCE(rel_path_norm, rel_path) AS compare_path, size, COALESCE(sha256, '')
		FROM entries
		WHERE scan_id = ?
	`
	if rawPrefix != "" {
		query += `
		  AND rel_path >= ?
		  AND rel_path < ? || x'FF'
	`
		args = append(args, rawPrefix, rawPrefix)
	}
	query += ignoreClause + `
		ORDER BY rel_path
	`
	args = append(args, ignoreArgs...)

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []comparePathEntry
	for rows.Next() {
		var entry comparePathEntry
		if err := rows.Scan(&entry.RawRelPath, &entry.CompareRelPath, &entry.Size, &entry.SHA256); err != nil {
			return nil, err
		}
		if rawPrefix != "" {
			entry.RawRelPath = strings.TrimPrefix(entry.RawRelPath, rawPrefix)
		}
		if comparePrefix != "" {
			entry.CompareRelPath = strings.TrimPrefix(entry.CompareRelPath, comparePrefix)
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
}

// identicalCompareEntries defines "identical" for path compare mode.
//
// Path compare intentionally treats two entries as identical only when both the
// size and non-empty sha256 match. Missing hashes keep the row in Different so
// the UI does not silently treat "unknown content" as "same content".
func identicalCompareEntries(left, right comparePathEntry) bool {
	return left.Size == right.Size &&
		left.SHA256 != "" &&
		right.SHA256 != "" &&
		left.SHA256 == right.SHA256
}

// compactMissingTreesFromEntries is the in-memory compact-view helper that works
// from already materialized opposite-side compare entries.
func compactMissingTreesFromEntries(entries []fileEntry, opposite []comparePathEntry) []fileDisplayEntry {
	if len(entries) == 0 {
		return []fileDisplayEntry{}
	}

	subtreeCounts := countMissingTreeDirs(entries)
	candidateDirs := collapseCandidateDirs(subtreeCounts)
	if len(candidateDirs) == 0 {
		return rawDisplayEntries(entries)
	}

	return compactMissingTrees(entries, subtreeCounts, blockedDirsFromEntries(opposite, candidateDirs))
}

// compactMissingTreesFromComparePathsCTE keeps the SQL-first path from paying
// for expensive subtree-blocking joins by loading only the opposite side's
// compare paths and resolving blocked directories in Go.
func compactMissingTreesFromComparePathsCTE(ctx context.Context, db *sql.DB, cte string, args []any, entries []fileEntry, oppositeTable string) ([]fileDisplayEntry, error) {
	if len(entries) == 0 {
		return []fileDisplayEntry{}, nil
	}

	subtreeCounts := countMissingTreeDirs(entries)
	candidateDirs := collapseCandidateDirs(subtreeCounts)
	if len(candidateDirs) == 0 {
		return rawDisplayEntries(entries), nil
	}

	opposite, err := loadComparePathsCTE(ctx, db, cte, args, oppositeTable)
	if err != nil {
		return nil, err
	}

	return compactMissingTrees(entries, subtreeCounts, blockedDirsFromEntries(opposite, candidateDirs)), nil
}

func blockedDirsFromEntries(opposite []comparePathEntry, candidateDirs []string) map[string]struct{} {
	blockedDirs := make(map[string]struct{})
	if len(candidateDirs) == 0 {
		return blockedDirs
	}

	candidateCompareDirs := make(map[string][]string, len(candidateDirs))
	for _, rawDir := range candidateDirs {
		compareDir := inventory.CompareRelPathKey(rawDir)
		candidateCompareDirs[compareDir] = append(candidateCompareDirs[compareDir], rawDir)
	}
	for _, entry := range opposite {
		if rawDirs, ok := candidateCompareDirs[entry.CompareRelPath]; ok {
			for _, rawDir := range rawDirs {
				blockedDirs[rawDir] = struct{}{}
			}
		}
		for _, compareDir := range ancestorDirs(entry.CompareRelPath) {
			if rawDirs, ok := candidateCompareDirs[compareDir]; ok {
				for _, rawDir := range rawDirs {
					blockedDirs[rawDir] = struct{}{}
				}
			}
		}
	}
	return blockedDirs
}

// loadComparePathsCTE loads only compare-space paths from one side of a compare
// relation. The SQL-first path uses this instead of the older blocked-dir SQL
// join because benchmarking showed that loading compare paths once and
// resolving blocked directories in Go was cheaper than repeatedly asking
// SQLite subtree questions for compact view blocking.
func loadComparePathsCTE(ctx context.Context, db *sql.DB, cte string, args []any, table string) ([]comparePathEntry, error) {
	rows, err := db.QueryContext(ctx, cte+`
		SELECT compare_rel_path
		FROM `+table+`
		ORDER BY compare_rel_path
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []comparePathEntry
	for rows.Next() {
		var entry comparePathEntry
		if err := rows.Scan(&entry.CompareRelPath); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, rows.Err()
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

// topmostCollapsibleDir chooses the highest ancestor that may safely collapse a
// missing entry in compact view.
//
// The search walks outward so the UI prefers one coarse subtree row over many
// smaller nested collapses, unless an ancestor is blocked by opposite-side
// presence under compare semantics.
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

func ensureNonNilDuplicateGroups(r duplicatesResult) duplicatesResult {
	if r.Groups == nil {
		r.Groups = []duplicateGroup{}
	}
	return r
}
