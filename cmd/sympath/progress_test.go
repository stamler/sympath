package main

import (
	"strings"
	"testing"

	inventory "sympath"
)

func TestFormatCount(t *testing.T) {
	tests := map[int64]string{
		0:         "0",
		7:         "7",
		999:       "999",
		1000:      "1,000",
		123456789: "123,456,789",
	}

	for input, want := range tests {
		if got := formatCount(input); got != want {
			t.Fatalf("formatCount(%d) = %q, want %q", input, got, want)
		}
	}
}

func TestRenderProgressTrackBounces(t *testing.T) {
	width := 20
	first := renderProgressTrack(0, width)
	middle := renderProgressTrack(7, width)
	returned := renderProgressTrack(22, width)

	if len(first) != width || len(middle) != width || len(returned) != width {
		t.Fatalf("expected track width %d, got %d/%d/%d", width, len(first), len(middle), len(returned))
	}
	if first == middle {
		t.Fatalf("expected track to move, both frames were %q", first)
	}
	if first != returned {
		t.Fatalf("expected bounce cycle to return to start, got %q then %q", first, returned)
	}
}

func TestFormatScanProgressLine(t *testing.T) {
	scanning := formatScanProgressLine(0, inventory.ScanProgressSnapshot{
		FilesDiscovered: 1234,
		FilesProcessed:  567,
		FilesPending:    667,
	})
	if !strings.Contains(scanning, "scanning files: 1,234 done: 567") {
		t.Fatalf("expected scanning line, got %q", scanning)
	}

	hashing := formatScanProgressLine(1, inventory.ScanProgressSnapshot{
		FilesDiscovered: 1234,
		FilesProcessed:  1200,
		FilesPending:    34,
		WalkComplete:    true,
	})
	if !strings.Contains(hashing, "hashing  files: 1,234 done: 1,200") {
		t.Fatalf("expected hashing line, got %q", hashing)
	}
}
