package inventory

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type S3ChecksumReportImportSummary struct {
	ImportID        int64
	ManifestPath    string
	ReportSchema    string
	InventoryPath   string
	Buckets         []S3ChecksumReportBucketSummary
	RowsTotal       int
	RowsSucceeded   int
	RowsUsable      int
	RowsFailed      int
	RowsUnsupported int
	RowsWithSize    int
	RowsFolderMarks int
}

type S3ChecksumReportBucketSummary struct {
	Bucket  string
	Root    string
	ScanID  int64
	Entries int
}

type s3ChecksumReportManifest struct {
	ReportSchema string                   `json:"ReportSchema"`
	Results      []s3ChecksumReportResult `json:"Results"`
}

type s3ChecksumReportResult struct {
	TaskExecutionStatus string `json:"TaskExecutionStatus"`
	Bucket              string `json:"Bucket"`
	Key                 string `json:"Key"`
}

type S3ChecksumReportImportOptions struct {
	InventoryManifestPath string
}

type s3InventoryManifest struct {
	SourceBucket      string            `json:"sourceBucket"`
	FileFormat        string            `json:"fileFormat"`
	FileSchema        string            `json:"fileSchema"`
	CreationTimestamp string            `json:"creationTimestamp"`
	Files             []s3InventoryFile `json:"files"`
}

type s3InventoryFile struct {
	Key string `json:"key"`
}

type s3InventoryObject struct {
	Size    int64
	MtimeNS int64
}

type s3ReportRow struct {
	Bucket         string
	Key            string
	VersionID      string
	TaskStatus     string
	ErrorCode      string
	HTTPStatusCode sql.NullInt64
	ResultMessage  string
	ETag           string
	ChecksumBase64 string
	ChecksumHex    string
	Algorithm      string
	ChecksumType   string
	Usable         bool
	Err            string
}

type s3ImportBucket struct {
	bucket  string
	root    string
	scanID  int64
	entries int
	seenKey map[string]struct{}
}

const s3ChecksumReportColumns = 7

func ImportS3ChecksumReportManifest(ctx context.Context, db *sql.DB, manifestPath string) (S3ChecksumReportImportSummary, error) {
	return ImportS3ChecksumReportManifestWithOptions(ctx, db, manifestPath, S3ChecksumReportImportOptions{})
}

func ImportS3ChecksumReportManifestWithOptions(ctx context.Context, db *sql.DB, manifestPath string, opts S3ChecksumReportImportOptions) (S3ChecksumReportImportSummary, error) {
	manifestPath = strings.TrimSpace(manifestPath)
	if manifestPath == "" {
		return S3ChecksumReportImportSummary{}, errors.New("manifest path is required")
	}
	absManifestPath, err := filepath.Abs(manifestPath)
	if err != nil {
		return S3ChecksumReportImportSummary{}, err
	}

	manifestData, err := os.ReadFile(absManifestPath)
	if err != nil {
		return S3ChecksumReportImportSummary{}, fmt.Errorf("read manifest: %w", err)
	}
	var manifest s3ChecksumReportManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return S3ChecksumReportImportSummary{}, fmt.Errorf("parse manifest: %w", err)
	}
	if len(manifest.Results) == 0 {
		return S3ChecksumReportImportSummary{}, errors.New("manifest has no report results")
	}

	reportPaths, err := resolveLocalS3ReportFiles(filepath.Dir(absManifestPath), manifest.Results)
	if err != nil {
		return S3ChecksumReportImportSummary{}, err
	}
	if len(reportPaths) == 0 {
		return S3ChecksumReportImportSummary{}, errors.New("manifest did not resolve any local report CSV files")
	}

	inventoryObjects, inventoryManifestPath, err := loadS3InventoryObjects(opts.InventoryManifestPath)
	if err != nil {
		return S3ChecksumReportImportSummary{}, err
	}

	if err := ConfigureConnection(ctx, db); err != nil {
		return S3ChecksumReportImportSummary{}, err
	}
	identity, err := GetLocalMachineIdentity(ctx, db)
	if err != nil {
		return S3ChecksumReportImportSummary{}, err
	}

	importedAt := time.Now().UnixNano()
	summary := S3ChecksumReportImportSummary{
		ManifestPath:  absManifestPath,
		ReportSchema:  manifest.ReportSchema,
		InventoryPath: inventoryManifestPath,
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return S3ChecksumReportImportSummary{}, err
	}
	defer tx.Rollback()

	importResult, err := tx.ExecContext(ctx, `
		INSERT INTO s3_imports (manifest_path, report_schema, imported_at, buckets, rows_total, rows_succeeded, rows_usable, rows_failed, rows_unsupported, rows_with_size, rows_folder_markers)
		VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, 0, 0)
	`, absManifestPath, nullString(manifest.ReportSchema), importedAt)
	if err != nil {
		return S3ChecksumReportImportSummary{}, err
	}
	importID, err := importResult.LastInsertId()
	if err != nil {
		return S3ChecksumReportImportSummary{}, err
	}
	summary.ImportID = importID

	reportStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO s3_report_rows (
			import_id, row_index, bucket, object_key, version_id, task_status, error_code,
			http_status_code, result_message, etag, checksum_base64, checksum_hex,
			checksum_algorithm, checksum_type, usable, err
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return S3ChecksumReportImportSummary{}, err
	}
	defer reportStmt.Close()

	entryStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO entries (scan_id, rel_path, rel_path_norm, name, ext, size, mtime_ns, fingerprint, sha256, state, err)
		VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, 'ok', NULL)
	`)
	if err != nil {
		return S3ChecksumReportImportSummary{}, err
	}
	defer entryStmt.Close()

	buckets := make(map[string]*s3ImportBucket)
	rowIndex := 0
	for _, reportPath := range reportPaths {
		if err := importS3ReportCSV(ctx, tx, reportStmt, entryStmt, reportPath, importID, &rowIndex, identity, importedAt, buckets, inventoryObjects, &summary); err != nil {
			return S3ChecksumReportImportSummary{}, err
		}
	}

	for _, bucket := range buckets {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO roots (machine_id, root, current_scan_id) VALUES (?, ?, ?)
			 ON CONFLICT(machine_id, root) DO UPDATE SET current_scan_id=excluded.current_scan_id`,
			identity.MachineID, bucket.root, bucket.scanID,
		); err != nil {
			return S3ChecksumReportImportSummary{}, err
		}
		if _, err := tx.ExecContext(ctx,
			"DELETE FROM scans WHERE machine_id = ? AND root = ? AND scan_id <> ?",
			identity.MachineID, bucket.root, bucket.scanID,
		); err != nil {
			return S3ChecksumReportImportSummary{}, err
		}
		summary.Buckets = append(summary.Buckets, S3ChecksumReportBucketSummary{
			Bucket:  bucket.bucket,
			Root:    bucket.root,
			ScanID:  bucket.scanID,
			Entries: bucket.entries,
		})
	}

	sortS3BucketSummaries(summary.Buckets)
	if _, err := tx.ExecContext(ctx, `
		UPDATE s3_imports
		SET buckets = ?, rows_total = ?, rows_succeeded = ?, rows_usable = ?, rows_failed = ?, rows_unsupported = ?, rows_with_size = ?, rows_folder_markers = ?
		WHERE import_id = ?
	`, len(summary.Buckets), summary.RowsTotal, summary.RowsSucceeded, summary.RowsUsable, summary.RowsFailed, summary.RowsUnsupported, summary.RowsWithSize, summary.RowsFolderMarks, importID); err != nil {
		return S3ChecksumReportImportSummary{}, err
	}

	if err := tx.Commit(); err != nil {
		return S3ChecksumReportImportSummary{}, err
	}
	return summary, nil
}

func importS3ReportCSV(
	ctx context.Context,
	tx *sql.Tx,
	reportStmt *sql.Stmt,
	entryStmt *sql.Stmt,
	reportPath string,
	importID int64,
	rowIndex *int,
	identity MachineIdentity,
	importedAt int64,
	buckets map[string]*s3ImportBucket,
	inventoryObjects map[string]s3InventoryObject,
	summary *S3ChecksumReportImportSummary,
) error {
	file, err := os.Open(reportPath)
	if err != nil {
		return fmt.Errorf("open report CSV %s: %w", reportPath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true

	for {
		record, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read report CSV %s: %w", reportPath, err)
		}
		if isS3ReportHeader(record) {
			continue
		}
		if len(record) != s3ChecksumReportColumns {
			return fmt.Errorf("report CSV %s: expected %d columns, got %d", reportPath, s3ChecksumReportColumns, len(record))
		}

		(*rowIndex)++
		row := parseS3ReportRow(record)
		summary.RowsTotal++
		if strings.EqualFold(row.TaskStatus, "succeeded") {
			summary.RowsSucceeded++
		} else {
			summary.RowsFailed++
		}

		bucket, err := ensureS3ImportBucket(ctx, tx, identity, importedAt, buckets, row.Bucket)
		if err != nil {
			return err
		}

		var metadata s3InventoryObject
		skippedFolderMarker := false
		if row.Usable {
			var hasMetadata bool
			metadata, hasMetadata = inventoryObjects[s3InventoryLookupKey(row.Bucket, row.Key, row.VersionID)]
			if !hasMetadata {
				metadata, hasMetadata = inventoryObjects[s3InventoryLookupKey(row.Bucket, row.Key, "")]
			}
			if hasMetadata {
				summary.RowsWithSize++
			}
			if isS3FolderMarker(row.Key, metadata) {
				summary.RowsFolderMarks++
				row.Err = "folder marker skipped"
				row.Usable = false
				skippedFolderMarker = true
			}
		}

		if row.Usable {
			if _, ok := bucket.seenKey[row.Key]; ok {
				return fmt.Errorf("multiple usable checksum rows for s3://%s/%s; versioned duplicate keys are unsupported", row.Bucket, row.Key)
			}
			bucket.seenKey[row.Key] = struct{}{}
			if err := insertS3Entry(ctx, entryStmt, bucket.scanID, row.Key, row.ChecksumHex, metadata); err != nil {
				return err
			}
			bucket.entries++
			summary.RowsUsable++
		} else if strings.EqualFold(row.TaskStatus, "succeeded") && !skippedFolderMarker {
			summary.RowsUnsupported++
		}

		usable := 0
		if row.Usable {
			usable = 1
		}
		if _, err := reportStmt.ExecContext(ctx,
			importID, *rowIndex, row.Bucket, row.Key, nullString(row.VersionID), nullString(row.TaskStatus), nullString(row.ErrorCode),
			nullInt64(row.HTTPStatusCode), nullString(row.ResultMessage), nullString(row.ETag), nullString(row.ChecksumBase64),
			nullString(row.ChecksumHex), nullString(row.Algorithm), nullString(row.ChecksumType), usable, nullString(row.Err),
		); err != nil {
			return err
		}
	}
}

func resolveLocalS3ReportFiles(manifestDir string, results []s3ChecksumReportResult) ([]string, error) {
	var reportPaths []string
	seen := make(map[string]struct{})
	for _, result := range results {
		resultKey := strings.TrimSpace(result.Key)
		if resultKey == "" {
			continue
		}
		reportPath, err := resolveLocalS3ReportFile(manifestDir, resultKey)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[reportPath]; ok {
			continue
		}
		seen[reportPath] = struct{}{}
		reportPaths = append(reportPaths, reportPath)
	}
	return reportPaths, nil
}

func resolveLocalS3ReportFile(manifestDir, resultKey string) (string, error) {
	base := path.Base(resultKey)
	if base == "." || base == "/" || base == "" {
		return "", fmt.Errorf("cannot resolve report CSV for manifest key %q", resultKey)
	}

	var matches []string
	err := filepath.WalkDir(manifestDir, func(candidate string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if d != nil && d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if d.Name() == base {
			matches = append(matches, candidate)
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("search local report bundle: %w", err)
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("local report CSV %q was not found under %s", base, manifestDir)
	}
	if len(matches) > 1 {
		return "", fmt.Errorf("local report CSV %q is ambiguous under %s", base, manifestDir)
	}
	return matches[0], nil
}

func parseS3ReportRow(record []string) s3ReportRow {
	key, err := url.QueryUnescape(record[1])
	if err != nil {
		key = record[1]
	}
	row := s3ReportRow{
		Bucket:        record[0],
		Key:           key,
		VersionID:     s3NullableCSVValue(record[2]),
		TaskStatus:    record[3],
		ResultMessage: record[6],
	}
	if code, err := strconv.ParseInt(strings.TrimSpace(record[4]), 10, 64); err == nil {
		row.HTTPStatusCode = sql.NullInt64{Int64: code, Valid: true}
		row.ErrorCode = s3NullableCSVValue(record[5])
	} else {
		row.ErrorCode = s3NullableCSVValue(record[4])
		if code, err := strconv.ParseInt(strings.TrimSpace(record[5]), 10, 64); err == nil {
			row.HTTPStatusCode = sql.NullInt64{Int64: code, Valid: true}
		}
	}
	if strings.EqualFold(row.TaskStatus, "succeeded") {
		parseS3ChecksumResultMessage(&row)
	}
	return row
}

func s3NullableCSVValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "null") {
		return ""
	}
	return value
}

func parseS3ChecksumResultMessage(row *s3ReportRow) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(row.ResultMessage), &raw); err != nil {
		row.Err = "parse result message: " + err.Error()
		return
	}
	row.ETag = jsonStringValue(raw, "etag", "ETag")
	row.ChecksumBase64 = jsonStringValue(raw, "checksum_base64", "checksumBase64", "ChecksumBase64")
	row.ChecksumHex = strings.ToLower(jsonStringValue(raw, "checksum_hex", "checksumHex", "ChecksumHex"))
	row.Algorithm = strings.ToUpper(jsonStringValue(raw, "checksumAlgorithm", "checksum_algorithm", "ChecksumAlgorithm"))
	row.ChecksumType = strings.ToUpper(jsonStringValue(raw, "checksumType", "checksum_type", "ChecksumType"))
	if row.ChecksumHex == "" && row.ChecksumBase64 != "" {
		if decoded, err := base64.StdEncoding.DecodeString(row.ChecksumBase64); err == nil {
			row.ChecksumHex = hex.EncodeToString(decoded)
		}
	}
	if row.Algorithm == "SHA256" && row.ChecksumType == "FULL_OBJECT" && isSHA256Hex(row.ChecksumHex) {
		row.Usable = true
		return
	}
	row.Err = "unsupported checksum"
}

func ensureS3ImportBucket(ctx context.Context, tx *sql.Tx, identity MachineIdentity, importedAt int64, buckets map[string]*s3ImportBucket, bucketName string) (*s3ImportBucket, error) {
	if strings.TrimSpace(bucketName) == "" {
		return nil, errors.New("report row has empty bucket")
	}
	if bucket, ok := buckets[bucketName]; ok {
		return bucket, nil
	}
	root := "s3://" + bucketName
	caseSensitive := 1
	result, err := tx.ExecContext(ctx,
		`INSERT INTO scans (machine_id, hostname, root, started_at, finished_at, status, goos, goarch, fs_type, case_sensitive)
		 VALUES (?, ?, ?, ?, ?, 'complete', ?, ?, 's3', ?)`,
		identity.MachineID, identity.Hostname, root, importedAt, importedAt, runtime.GOOS, runtime.GOARCH, caseSensitive,
	)
	if err != nil {
		return nil, err
	}
	scanID, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}
	bucket := &s3ImportBucket{
		bucket:  bucketName,
		root:    root,
		scanID:  scanID,
		seenKey: make(map[string]struct{}),
	}
	buckets[bucketName] = bucket
	return bucket, nil
}

func loadS3InventoryObjects(manifestPath string) (map[string]s3InventoryObject, string, error) {
	manifestPath = strings.TrimSpace(manifestPath)
	if manifestPath == "" {
		return nil, "", nil
	}
	absManifestPath, err := filepath.Abs(manifestPath)
	if err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(absManifestPath)
	if err != nil {
		return nil, "", fmt.Errorf("read inventory manifest: %w", err)
	}
	var manifest s3InventoryManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, "", fmt.Errorf("parse inventory manifest: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(manifest.FileFormat), "CSV") {
		return nil, "", fmt.Errorf("inventory manifest fileFormat %q is unsupported; only CSV is supported", manifest.FileFormat)
	}
	columns := parseS3InventorySchema(manifest.FileSchema)
	sizeIndex := indexOfInventoryColumn(columns, "Size")
	if sizeIndex < 0 {
		return nil, "", errors.New("inventory manifest fileSchema must include Size")
	}
	bucketIndex := indexOfInventoryColumn(columns, "Bucket")
	keyIndex := indexOfInventoryColumn(columns, "Key")
	versionIndex := indexOfInventoryColumn(columns, "VersionId")
	lastModifiedIndex := indexOfInventoryColumn(columns, "LastModifiedDate")
	if bucketIndex < 0 || keyIndex < 0 {
		return nil, "", errors.New("inventory manifest fileSchema must include Bucket and Key")
	}

	reportPaths, err := resolveLocalS3InventoryFiles(localS3InventorySearchRoots(filepath.Dir(absManifestPath)), manifest.Files)
	if err != nil {
		return nil, "", err
	}
	if len(reportPaths) == 0 {
		return nil, "", errors.New("inventory manifest did not resolve any local CSV files")
	}

	objects := make(map[string]s3InventoryObject)
	for _, reportPath := range reportPaths {
		if err := loadS3InventoryCSV(reportPath, bucketIndex, keyIndex, versionIndex, sizeIndex, lastModifiedIndex, objects); err != nil {
			return nil, "", err
		}
	}
	return objects, absManifestPath, nil
}

func localS3InventorySearchRoots(manifestDir string) []string {
	roots := []string{manifestDir}
	parent := filepath.Dir(manifestDir)
	if parent != manifestDir {
		roots = append(roots, parent)
		grandparent := filepath.Dir(parent)
		if grandparent != parent {
			roots = append(roots, grandparent)
		}
	}
	return roots
}

func resolveLocalS3InventoryFiles(searchRoots []string, files []s3InventoryFile) ([]string, error) {
	var reportPaths []string
	seen := make(map[string]struct{})
	for _, file := range files {
		fileKey := strings.TrimSpace(file.Key)
		if fileKey == "" {
			continue
		}
		reportPath, err := resolveLocalS3ReportFileFromRoots(searchRoots, fileKey)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[reportPath]; ok {
			continue
		}
		seen[reportPath] = struct{}{}
		reportPaths = append(reportPaths, reportPath)
	}
	return reportPaths, nil
}

func resolveLocalS3ReportFileFromRoots(searchRoots []string, resultKey string) (string, error) {
	var matches []string
	for _, root := range searchRoots {
		reportPath, err := resolveLocalS3ReportFile(root, resultKey)
		if err != nil {
			if strings.Contains(err.Error(), " was not found under ") {
				continue
			}
			return "", err
		}
		matches = append(matches, reportPath)
	}
	if len(matches) == 0 {
		return "", fmt.Errorf("local report CSV %q was not found under %s", path.Base(resultKey), strings.Join(searchRoots, ", "))
	}
	unique := make(map[string]struct{})
	deduped := matches[:0]
	for _, match := range matches {
		if _, ok := unique[match]; ok {
			continue
		}
		unique[match] = struct{}{}
		deduped = append(deduped, match)
	}
	if len(deduped) > 1 {
		return "", fmt.Errorf("local report CSV %q is ambiguous under %s", path.Base(resultKey), strings.Join(searchRoots, ", "))
	}
	return deduped[0], nil
}

func loadS3InventoryCSV(reportPath string, bucketIndex, keyIndex, versionIndex, sizeIndex, lastModifiedIndex int, objects map[string]s3InventoryObject) error {
	file, err := os.Open(reportPath)
	if err != nil {
		return fmt.Errorf("open inventory CSV %s: %w", reportPath, err)
	}
	defer file.Close()

	var reader io.Reader = file
	if strings.HasSuffix(strings.ToLower(reportPath), ".gz") {
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("open inventory gzip %s: %w", reportPath, err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	}

	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1
	csvReader.TrimLeadingSpace = true
	for {
		record, err := csvReader.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read inventory CSV %s: %w", reportPath, err)
		}
		requiredIndex := maxInt(bucketIndex, keyIndex, sizeIndex)
		if len(record) <= requiredIndex {
			return fmt.Errorf("inventory CSV %s: expected at least %d columns, got %d", reportPath, requiredIndex+1, len(record))
		}
		size, err := strconv.ParseInt(strings.TrimSpace(record[sizeIndex]), 10, 64)
		if err != nil {
			return fmt.Errorf("inventory CSV %s: parse Size %q: %w", reportPath, record[sizeIndex], err)
		}
		key, err := url.QueryUnescape(record[keyIndex])
		if err != nil {
			return fmt.Errorf("inventory CSV %s: decode Key %q: %w", reportPath, record[keyIndex], err)
		}
		bucket := record[bucketIndex]
		versionID := ""
		if versionIndex >= 0 && len(record) > versionIndex {
			versionID = record[versionIndex]
		}
		mtimeNS := int64(0)
		if lastModifiedIndex >= 0 && len(record) > lastModifiedIndex {
			mtimeNS = parseS3InventoryLastModifiedNS(record[lastModifiedIndex])
		}
		objects[s3InventoryLookupKey(bucket, key, versionID)] = s3InventoryObject{
			Size:    size,
			MtimeNS: mtimeNS,
		}
		if versionID != "" {
			if _, ok := objects[s3InventoryLookupKey(bucket, key, "")]; !ok {
				objects[s3InventoryLookupKey(bucket, key, "")] = s3InventoryObject{Size: size, MtimeNS: mtimeNS}
			}
		}
	}
}

func parseS3InventorySchema(schema string) []string {
	parts := strings.Split(schema, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func indexOfInventoryColumn(columns []string, name string) int {
	for i, column := range columns {
		if strings.EqualFold(strings.TrimSpace(column), name) {
			return i
		}
	}
	return -1
}

func parseS3InventoryLastModifiedNS(value string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000Z", "2006-01-02 15:04:05"}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UnixNano()
		}
	}
	return 0
}

func s3InventoryLookupKey(bucket, key, versionID string) string {
	return bucket + "\x00" + key + "\x00" + versionID
}

func isS3FolderMarker(key string, metadata s3InventoryObject) bool {
	return strings.HasSuffix(key, "/") && metadata.Size == 0
}

func maxInt(values ...int) int {
	max := values[0]
	for _, value := range values[1:] {
		if value > max {
			max = value
		}
	}
	return max
}

func insertS3Entry(ctx context.Context, stmt *sql.Stmt, scanID int64, key, sha256 string, metadata s3InventoryObject) error {
	name := s3ObjectName(key)
	ext := strings.ToLower(path.Ext(name))
	var relPathNorm *string
	if normalized, ok := storedRelPathNorm(key); ok {
		relPathNorm = &normalized
	}
	_, err := stmt.ExecContext(ctx, scanID, key, relPathNorm, name, ext, metadata.Size, metadata.MtimeNS, sha256)
	return err
}

func s3ObjectName(key string) string {
	trimmed := strings.TrimRight(key, "/")
	if trimmed == "" {
		return ""
	}
	return path.Base(trimmed)
}

func isS3ReportHeader(record []string) bool {
	if len(record) != s3ChecksumReportColumns {
		return false
	}
	return strings.EqualFold(record[0], "Bucket") &&
		strings.EqualFold(record[1], "Key") &&
		strings.EqualFold(record[6], "ResultMessage")
}

func jsonStringValue(values map[string]any, keys ...string) string {
	for _, key := range keys {
		value, ok := values[key]
		if !ok {
			continue
		}
		if text, ok := value.(string); ok {
			return strings.TrimSpace(text)
		}
	}
	return ""
}

func isSHA256Hex(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func nullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullInt64(value sql.NullInt64) any {
	if !value.Valid {
		return nil
	}
	return value.Int64
}

func sortS3BucketSummaries(buckets []S3ChecksumReportBucketSummary) {
	for i := 1; i < len(buckets); i++ {
		for j := i; j > 0 && buckets[j-1].Root > buckets[j].Root; j-- {
			buckets[j-1], buckets[j] = buckets[j], buckets[j-1]
		}
	}
}
