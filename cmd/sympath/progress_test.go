package main

import (
	"strings"
	"testing"
	"unicode/utf8"

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
	returned := renderProgressTrack((width-1)*2, width)

	if utf8.RuneCountInString(first) != width || utf8.RuneCountInString(middle) != width || utf8.RuneCountInString(returned) != width {
		t.Fatalf("expected track width %d, got %d/%d/%d", width, utf8.RuneCountInString(first), utf8.RuneCountInString(middle), utf8.RuneCountInString(returned))
	}
	if first == middle {
		t.Fatalf("expected track to move, both frames were %q", first)
	}
	if first != returned {
		t.Fatalf("expected bounce cycle to return to start, got %q then %q", first, returned)
	}
}

func TestRenderProgressTrackTailFollowsDirection(t *testing.T) {
	if got := renderProgressTrack(5, 10); got != "  ░▒▓█    " {
		t.Fatalf("expected rightward tail on the left, got %q", got)
	}

	if got := renderProgressTrack(13, 10); got != "     █▓▒░ " {
		t.Fatalf("expected leftward tail on the right, got %q", got)
	}
}

func TestFormatScanProgressLine(t *testing.T) {
	scanning := formatScanProgressLine(0, inventory.ScanProgressSnapshot{
		FilesDiscovered: 1234,
		FilesProcessed:  567,
		FilesPending:    667,
	})
	if !strings.Contains(scanning, "Scanning files: 1,234 done: 567") {
		t.Fatalf("expected scanning line, got %q", scanning)
	}
	if strings.Contains(scanning, "| [") {
		t.Fatalf("expected no spinner prefix, got %q", scanning)
	}

	walkComplete := formatScanProgressLine(1, inventory.ScanProgressSnapshot{
		FilesDiscovered: 1234,
		FilesProcessed:  1200,
		FilesPending:    34,
		WalkComplete:    true,
	})
	if !strings.Contains(walkComplete, "Scanning files: 1,234 done: 1,200") {
		t.Fatalf("expected scanning line after walk completion, got %q", walkComplete)
	}
}

func TestFormatScanProgressText(t *testing.T) {
	got := formatScanProgressText(inventory.ScanProgressSnapshot{
		FilesDiscovered: 1234,
		FilesProcessed:  567,
	})
	if got != "Scanning files: 1,234 done: 567" {
		t.Fatalf("expected non-tty scan text, got %q", got)
	}
}

func TestFormatItemProgressLine(t *testing.T) {
	got := formatItemProgressLine(3, "Fetching", "remote-a (1/2)")
	if !strings.Contains(got, "Fetching: remote-a (1/2)") {
		t.Fatalf("expected fetching line, got %q", got)
	}
}
