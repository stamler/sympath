package inventory

// hash.go implements content hashing for the inventory pipeline.
//
// Two hashes are computed in a single read pass per file:
//
//   - Full SHA-256: cryptographic hash of the entire file content.
//   - Fast fingerprint: SHA-256 of (first 64KB || last 64KB || file size
//     as 8-byte little-endian). For files smaller than 128KB the first
//     and last regions overlap, which is fine — the fingerprint is still
//     deterministic and content-dependent.
//
// After reading, the file is stat'd again to detect changes that
// occurred during the read (stat-before / stat-after). If the size or
// mtime changed, the result is marked "unstable" and the hash worker
// retries once before giving up.
//
// Each hash worker goroutine allocates a single 1MB read buffer at
// startup and reuses it across all files to minimize allocations.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
)

const (
	// readBufSize is the size of the reusable per-worker read buffer.
	readBufSize = 1024 * 1024 // 1MB

	// fingerprintSample is the number of bytes captured from the
	// beginning and end of a file for the fast fingerprint.
	fingerprintSample = 64 * 1024 // 64KB
)

// computeFingerprint computes a partial SHA-256 fingerprint from the
// first and last 64KB of file content plus the file size encoded as an
// 8-byte little-endian integer. The result is a 64-character hex string.
func computeFingerprint(first, last []byte, size int64) string {
	h := sha256.New()
	h.Write(first)
	h.Write(last)
	var sizeBuf [8]byte
	binary.LittleEndian.PutUint64(sizeBuf[:], uint64(size))
	h.Write(sizeBuf[:])
	return hex.EncodeToString(h.Sum(nil))
}

// computeHashes opens a file and computes both the full SHA-256 and the
// partial fingerprint in a single sequential read pass. It validates
// that the file did not change during the read by comparing os.Stat
// results before and after. The caller-provided buf is used for reading
// to avoid per-call allocation. Returns a HashResult with State set to
// "ok", "unstable", "vanished", or "error".
func computeHashes(absPath string, expectedSize, expectedMtimeNS int64, buf []byte) HashResult {
	// Pre-stat
	preInfo, err := os.Stat(absPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return HashResult{State: "vanished", Err: err.Error()}
		}
		return HashResult{State: "error", Err: err.Error()}
	}

	f, err := os.Open(absPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return HashResult{State: "vanished", Err: err.Error()}
		}
		return HashResult{State: "error", Err: err.Error()}
	}
	defer f.Close()

	fileSize := preInfo.Size()

	fullHasher := sha256.New()

	// Capture first and last 64KB for fingerprint
	var firstBuf []byte
	var lastBuf []byte
	var totalRead int64

	if len(buf) == 0 {
		buf = make([]byte, readBufSize)
	}

	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			fullHasher.Write(chunk)

			// Capture first 64KB
			if len(firstBuf) < fingerprintSample {
				need := fingerprintSample - len(firstBuf)
				if need > n {
					need = n
				}
				firstBuf = append(firstBuf, chunk[:need]...)
			}

			// Track last 64KB using a rolling window
			totalRead += int64(n)
			lastBuf = appendLast(lastBuf, chunk, fingerprintSample)
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return HashResult{State: "error", Err: readErr.Error()}
		}
	}

	fullHash := hex.EncodeToString(fullHasher.Sum(nil))
	fingerprint := computeFingerprint(firstBuf, lastBuf, fileSize)

	// Post-stat validation
	postInfo, err := os.Stat(absPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return HashResult{State: "vanished", Err: err.Error()}
		}
		return HashResult{State: "error", Err: err.Error()}
	}

	if postInfo.Size() != preInfo.Size() ||
		postInfo.ModTime().UnixNano() != preInfo.ModTime().UnixNano() ||
		preInfo.Size() != expectedSize ||
		preInfo.ModTime().UnixNano() != expectedMtimeNS {
		return HashResult{
			State:       "unstable",
			Fingerprint: fingerprint,
			SHA256:      fullHash,
			Err:         "file changed during read",
		}
	}

	return HashResult{
		State:       "ok",
		Fingerprint: fingerprint,
		SHA256:      fullHash,
	}
}

// appendLast maintains a sliding window of the last maxSize bytes by
// appending chunk to lastBuf and trimming from the front if needed.
func appendLast(lastBuf, chunk []byte, maxSize int) []byte {
	combined := append(lastBuf, chunk...)
	if len(combined) > maxSize {
		combined = combined[len(combined)-maxSize:]
	}
	return combined
}

// hashWorker runs in its own goroutine, reading HashJobs from the jobs
// channel and sending HashResults to the results channel. Each worker
// allocates a 1MB read buffer once and reuses it across all files. If
// a file is detected as unstable (changed during read), the worker
// retries once before marking the result as unstable.
func hashWorker(ctx context.Context, jobs <-chan HashJob, results chan<- HashResult) {
	buf := make([]byte, readBufSize)
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				return
			}
			r := computeHashes(job.AbsPath, job.Size, job.MtimeNS, buf)
			r.RelPath = job.RelPath
			if r.State == "unstable" {
				// Retry once
				r = computeHashes(job.AbsPath, job.Size, job.MtimeNS, buf)
				r.RelPath = job.RelPath
			}
			select {
			case results <- r:
			case <-ctx.Done():
				return
			}
		}
	}
}
