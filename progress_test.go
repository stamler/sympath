package inventory

import "testing"

func TestScanProgressSnapshot(t *testing.T) {
	var progress ScanProgress

	progress.noteDiscovered("pending")

	snapshot := progress.Snapshot()
	if snapshot.FilesDiscovered != 1 {
		t.Fatalf("expected 1 discovered file, got %d", snapshot.FilesDiscovered)
	}
	if snapshot.FilesProcessed != 0 {
		t.Fatalf("expected 0 processed files, got %d", snapshot.FilesProcessed)
	}
	if snapshot.FilesPending != 1 {
		t.Fatalf("expected 1 pending file, got %d", snapshot.FilesPending)
	}

	progress.noteDiscovered("reused")
	progress.noteDiscovered("error")
	progress.noteHashed()
	progress.noteWalkComplete()

	snapshot = progress.Snapshot()
	if snapshot.FilesDiscovered != 3 {
		t.Fatalf("expected 3 discovered files, got %d", snapshot.FilesDiscovered)
	}
	if snapshot.FilesProcessed != 3 {
		t.Fatalf("expected 3 processed files, got %d", snapshot.FilesProcessed)
	}
	if snapshot.FilesPending != 0 {
		t.Fatalf("expected 0 pending files, got %d", snapshot.FilesPending)
	}
	if snapshot.FilesHashed != 1 {
		t.Fatalf("expected 1 hashed file, got %d", snapshot.FilesHashed)
	}
	if snapshot.FilesReused != 1 {
		t.Fatalf("expected 1 reused file, got %d", snapshot.FilesReused)
	}
	if !snapshot.WalkComplete {
		t.Fatal("expected walk to be marked complete")
	}
}
