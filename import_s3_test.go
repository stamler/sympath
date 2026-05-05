package inventory

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestImportS3ChecksumReportManifest_ImportsUsableFullObjectSHA256Rows(t *testing.T) {
	dir := t.TempDir()
	manifestPath := writeS3ReportBundle(t, dir, map[string][][]string{
		"succeeded.csv": {
			s3ReportRecord("bucket-one", "photos/a.jpg", "succeeded", "", "200", checksumResult("SHA256", "FULL_OBJECT", strings.Repeat("a", 64))),
			s3ReportRecord("bucket-one", "photos/composite.jpg", "succeeded", "", "200", checksumResult("SHA256", "COMPOSITE", strings.Repeat("b", 64))),
			s3ReportRecord("bucket-two", "docs/b.txt", "succeeded", "", "200", checksumResult("SHA256", "FULL_OBJECT", strings.Repeat("c", 64))),
			s3ReportRecord("bucket-two", "docs/folder/", "succeeded", "", "200", checksumResult("SHA256", "FULL_OBJECT", strings.Repeat("e", 64))),
		},
		"failed.csv": {
			s3ReportRecord("bucket-one", "photos/bad.jpg", "failed", "InternalError", "500", "failed to compute checksum"),
		},
	})

	db := openTestDB(t, t.TempDir())
	summary, err := ImportS3ChecksumReportManifest(context.Background(), db, manifestPath)
	if err != nil {
		t.Fatal(err)
	}

	if summary.RowsTotal != 5 || summary.RowsSucceeded != 4 || summary.RowsUsable != 2 || summary.RowsFailed != 1 || summary.RowsUnsupported != 1 || summary.RowsFolderMarks != 1 {
		t.Fatalf("unexpected summary: %#v", summary)
	}
	if len(summary.Buckets) != 2 {
		t.Fatalf("expected 2 bucket scans, got %#v", summary.Buckets)
	}
	if got := countEntries(t, db, "s3://bucket-one"); got != 1 {
		t.Fatalf("bucket-one entries = %d, want 1", got)
	}
	if got := countEntries(t, db, "s3://bucket-two"); got != 1 {
		t.Fatalf("bucket-two entries = %d, want 1", got)
	}

	var sha string
	if err := db.QueryRow(`
		SELECT e.sha256
		FROM entries e
		JOIN roots r ON r.current_scan_id = e.scan_id
		WHERE r.root = 's3://bucket-one' AND e.rel_path = 'photos/a.jpg'
	`).Scan(&sha); err != nil {
		t.Fatal(err)
	}
	if sha != strings.Repeat("a", 64) {
		t.Fatalf("sha256 = %q", sha)
	}

	var totalRows, usableRows int
	if err := db.QueryRow("SELECT COUNT(*), SUM(usable) FROM s3_report_rows").Scan(&totalRows, &usableRows); err != nil {
		t.Fatal(err)
	}
	if totalRows != 5 || usableRows != 2 {
		t.Fatalf("s3_report_rows total=%d usable=%d", totalRows, usableRows)
	}

	var persistedSized, persistedFolderMarkers int
	if err := db.QueryRow("SELECT rows_with_size, rows_folder_markers FROM s3_imports").Scan(&persistedSized, &persistedFolderMarkers); err != nil {
		t.Fatal(err)
	}
	if persistedFolderMarkers != 1 {
		t.Fatalf("rows_folder_markers = %d, want 1", persistedFolderMarkers)
	}
}

func TestImportS3ChecksumReportManifest_EnrichesSizeFromInventoryManifest(t *testing.T) {
	dir := t.TempDir()
	manifestPath := writeS3ReportBundle(t, dir, map[string][][]string{
		"succeeded.csv": {
			s3ReportRecord("bucket-one", "photos%2Fa+space.jpg", "succeeded", "", "200", checksumResult("SHA256", "FULL_OBJECT", strings.Repeat("a", 64))),
		},
	})
	inventoryManifestPath := writeS3InventoryBundle(t, t.TempDir(), "inventory.csv.gz", [][]string{
		{"bucket-one", "photos%2Fa+space.jpg", "", "12345", "2026-05-04T14:15:16.000Z"},
	}, true)

	db := openTestDB(t, t.TempDir())
	summary, err := ImportS3ChecksumReportManifestWithOptions(context.Background(), db, manifestPath, S3ChecksumReportImportOptions{
		InventoryManifestPath: inventoryManifestPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.RowsWithSize != 1 {
		t.Fatalf("RowsWithSize = %d, want 1", summary.RowsWithSize)
	}

	var size, mtimeNS int64
	if err := db.QueryRow(`
		SELECT e.size, e.mtime_ns
		FROM entries e
		JOIN roots r ON r.current_scan_id = e.scan_id
		WHERE r.root = 's3://bucket-one' AND e.rel_path = 'photos/a space.jpg'
	`).Scan(&size, &mtimeNS); err != nil {
		t.Fatal(err)
	}
	if size != 12345 {
		t.Fatalf("size = %d, want 12345", size)
	}
	if mtimeNS == 0 {
		t.Fatal("expected mtime_ns to be populated from inventory")
	}
}

func TestImportS3ChecksumReportManifest_ReimportReplacesAuthoritativeBucketScan(t *testing.T) {
	db := openTestDB(t, t.TempDir())
	firstDir := t.TempDir()
	firstManifest := writeS3ReportBundle(t, firstDir, map[string][][]string{
		"first.csv": {
			s3ReportRecord("bucket-one", "photos/a.jpg", "succeeded", "", "200", checksumResult("SHA256", "FULL_OBJECT", strings.Repeat("a", 64))),
		},
	})
	if _, err := ImportS3ChecksumReportManifest(context.Background(), db, firstManifest); err != nil {
		t.Fatal(err)
	}
	firstScanID := currentScanID(t, db, "s3://bucket-one")

	secondDir := t.TempDir()
	secondManifest := writeS3ReportBundle(t, secondDir, map[string][][]string{
		"second.csv": {
			s3ReportRecord("bucket-one", "photos/a.jpg", "succeeded", "", "200", checksumResult("SHA256", "FULL_OBJECT", strings.Repeat("d", 64))),
		},
	})
	if _, err := ImportS3ChecksumReportManifest(context.Background(), db, secondManifest); err != nil {
		t.Fatal(err)
	}
	secondScanID := currentScanID(t, db, "s3://bucket-one")
	if firstScanID == secondScanID {
		t.Fatal("expected reimport to publish a new scan")
	}
	if got := countScans(t, db, "s3://bucket-one"); got != 1 {
		t.Fatalf("expected old bucket scan to be deleted, got %d scans", got)
	}
	if got := countEntries(t, db, "s3://bucket-one"); got != 1 {
		t.Fatalf("expected one current entry, got %d", got)
	}
}

func TestImportS3ChecksumReportManifest_RejectsDuplicateUsableKeys(t *testing.T) {
	dir := t.TempDir()
	manifestPath := writeS3ReportBundle(t, dir, map[string][][]string{
		"succeeded.csv": {
			s3ReportRecord("bucket-one", "photos/a.jpg", "succeeded", "", "200", checksumResult("SHA256", "FULL_OBJECT", strings.Repeat("a", 64))),
			s3ReportRecord("bucket-one", "photos/a.jpg", "succeeded", "", "200", checksumResult("SHA256", "FULL_OBJECT", strings.Repeat("b", 64))),
		},
	})

	db := openTestDB(t, t.TempDir())
	_, err := ImportS3ChecksumReportManifest(context.Background(), db, manifestPath)
	if err == nil {
		t.Fatal("expected duplicate key error")
	}
	if !strings.Contains(err.Error(), "multiple usable checksum rows") {
		t.Fatalf("unexpected error: %v", err)
	}

	var imports int
	if err := db.QueryRow("SELECT COUNT(*) FROM s3_imports").Scan(&imports); err != nil && err != sql.ErrNoRows {
		t.Fatal(err)
	}
	if imports != 0 {
		t.Fatalf("expected failed import to roll back, got %d imports", imports)
	}
}

func writeS3ReportBundle(t *testing.T, dir string, files map[string][][]string) string {
	t.Helper()
	type manifestResult struct {
		TaskExecutionStatus string `json:"TaskExecutionStatus"`
		Bucket              string `json:"Bucket"`
		Key                 string `json:"Key"`
	}
	var results []manifestResult
	for name, rows := range files {
		reportPath := filepath.Join(dir, "results", name)
		if err := os.MkdirAll(filepath.Dir(reportPath), 0755); err != nil {
			t.Fatal(err)
		}
		file, err := os.Create(reportPath)
		if err != nil {
			t.Fatal(err)
		}
		writer := csv.NewWriter(file)
		if err := writer.Write([]string{"Bucket", "Key", "VersionId", "TaskStatus", "ErrorCode", "HTTPStatusCode", "ResultMessage"}); err != nil {
			t.Fatal(err)
		}
		for _, row := range rows {
			if err := writer.Write(row); err != nil {
				t.Fatal(err)
			}
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		results = append(results, manifestResult{
			TaskExecutionStatus: strings.TrimSuffix(name, ".csv"),
			Bucket:              "report-bucket",
			Key:                 "reports/job/results/" + name,
		})
	}
	manifest := map[string]any{
		"ReportSchema": "Bucket, Key, VersionId, TaskStatus, ErrorCode, HTTPStatusCode, ResultMessage",
		"Results":      results,
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, data, 0644); err != nil {
		t.Fatal(err)
	}
	return manifestPath
}

func writeS3InventoryBundle(t *testing.T, dir, name string, rows [][]string, compressed bool) string {
	t.Helper()
	reportPath := filepath.Join(dir, "inventory", name)
	if err := os.MkdirAll(filepath.Dir(reportPath), 0755); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var writerTarget io.Writer = file
	var gzipWriter *gzip.Writer
	if compressed {
		gzipWriter = gzip.NewWriter(file)
		writerTarget = gzipWriter
	}
	writer := csv.NewWriter(writerTarget)
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			t.Fatal(err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatal(err)
	}
	if gzipWriter != nil {
		if err := gzipWriter.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	manifest := map[string]any{
		"sourceBucket":      "bucket-one",
		"destinationBucket": "arn:aws:s3:::report-bucket",
		"version":           "2016-11-30",
		"creationTimestamp": "1777385716000",
		"fileFormat":        "CSV",
		"fileSchema":        "Bucket, Key, VersionId, Size, LastModifiedDate",
		"files": []map[string]any{
			{
				"key":         "inventory/" + name,
				"size":        123,
				"MD5checksum": strings.Repeat("0", 32),
			},
		},
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, data, 0644); err != nil {
		t.Fatal(err)
	}
	return manifestPath
}

func s3ReportRecord(bucket, key, status, errorCode, httpCode, resultMessage string) []string {
	return []string{bucket, key, "", status, errorCode, httpCode, resultMessage}
}

func checksumResult(algorithm, checksumType, checksumHex string) string {
	data, err := json.Marshal(map[string]string{
		"checksumAlgorithm": algorithm,
		"checksumType":      checksumType,
		"checksum_hex":      checksumHex,
		"etag":              "etag-value",
	})
	if err != nil {
		panic(err)
	}
	return string(data)
}
