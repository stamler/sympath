package inventory

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestComputeHashes_KnownValue(t *testing.T) {
	dir := t.TempDir()
	content := []byte("hello world\n")
	path := filepath.Join(dir, "test.txt")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, readBufSize)
	r := computeHashes(path, info.Size(), info.ModTime().UnixNano(), buf)

	if r.State != "ok" {
		t.Fatalf("expected state ok, got %s (err: %s)", r.State, r.Err)
	}

	// Verify SHA-256
	expected := sha256.Sum256(content)
	expectedHex := hex.EncodeToString(expected[:])
	if r.SHA256 != expectedHex {
		t.Errorf("sha256 mismatch:\n  got  %s\n  want %s", r.SHA256, expectedHex)
	}

	if r.Fingerprint == "" {
		t.Error("fingerprint should not be empty")
	}
}

func TestComputeHashes_ZeroByte(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.txt")
	if err := os.WriteFile(path, nil, 0644); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, readBufSize)
	r := computeHashes(path, info.Size(), info.ModTime().UnixNano(), buf)

	if r.State != "ok" {
		t.Fatalf("expected state ok, got %s (err: %s)", r.State, r.Err)
	}

	// SHA-256 of empty content
	expected := sha256.Sum256(nil)
	expectedHex := hex.EncodeToString(expected[:])
	if r.SHA256 != expectedHex {
		t.Errorf("sha256 mismatch for empty file:\n  got  %s\n  want %s", r.SHA256, expectedHex)
	}
}

func TestComputeHashes_LargeFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "large.bin")

	// Create a 256KB file (larger than fingerprint sample size)
	size := 256 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251) // deterministic pattern
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, readBufSize)
	r := computeHashes(path, info.Size(), info.ModTime().UnixNano(), buf)

	if r.State != "ok" {
		t.Fatalf("expected state ok, got %s (err: %s)", r.State, r.Err)
	}

	expected := sha256.Sum256(data)
	expectedHex := hex.EncodeToString(expected[:])
	if r.SHA256 != expectedHex {
		t.Errorf("sha256 mismatch for large file:\n  got  %s\n  want %s", r.SHA256, expectedHex)
	}

	if r.Fingerprint == "" {
		t.Error("fingerprint should not be empty")
	}

	// Fingerprint should differ from full hash (different input)
	if r.Fingerprint == r.SHA256 {
		t.Error("fingerprint should differ from full sha256 for files > 128KB")
	}
}

func TestComputeHashes_Vanished(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gone.txt")

	buf := make([]byte, readBufSize)
	r := computeHashes(path, 100, 0, buf)

	if r.State != "vanished" {
		t.Fatalf("expected state vanished, got %s", r.State)
	}
}

func TestComputeHashes_Idempotent(t *testing.T) {
	dir := t.TempDir()
	content := []byte("deterministic content for idempotency test")
	path := filepath.Join(dir, "idem.txt")
	if err := os.WriteFile(path, content, 0644); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, readBufSize)
	r1 := computeHashes(path, info.Size(), info.ModTime().UnixNano(), buf)
	r2 := computeHashes(path, info.Size(), info.ModTime().UnixNano(), buf)

	if r1.SHA256 != r2.SHA256 {
		t.Error("sha256 not idempotent")
	}
	if r1.Fingerprint != r2.Fingerprint {
		t.Error("fingerprint not idempotent")
	}
}

func TestComputeFingerprint_SmallFile(t *testing.T) {
	// For files smaller than 128KB, first and last overlap
	content := []byte("small file content")
	fp1 := computeFingerprint(content, content, int64(len(content)))
	fp2 := computeFingerprint(content, content, int64(len(content)))
	if fp1 != fp2 {
		t.Error("fingerprint not deterministic for small files")
	}
	if fp1 == "" {
		t.Error("fingerprint should not be empty")
	}
}
