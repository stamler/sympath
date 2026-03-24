package inventory

import (
	"strings"
	"testing"
)

func TestNewRandomSympathFilename_DefaultShape(t *testing.T) {
	name, err := NewRandomSympathFilename()
	if err != nil {
		t.Fatal(err)
	}

	assertRandomSympathFilename(t, name, DefaultRandomSympathNameLength)
}

func TestRandomSympathFilename_CustomLength(t *testing.T) {
	name, err := RandomSympathFilename(16)
	if err != nil {
		t.Fatal(err)
	}

	assertRandomSympathFilename(t, name, 16)
}

func TestRandomSympathFilename_RejectsShortNames(t *testing.T) {
	name, err := RandomSympathFilename(DefaultRandomSympathNameLength - 1)
	if err == nil {
		t.Fatalf("expected error for short name, got %q", name)
	}
}

func TestRandomSympathFilename_Varies(t *testing.T) {
	first, err := NewRandomSympathFilename()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRandomSympathFilename()
	if err != nil {
		t.Fatal(err)
	}

	if first == second {
		t.Fatalf("expected different random filenames, got %q twice", first)
	}
}

func assertRandomSympathFilename(t *testing.T, name string, wantLength int) {
	t.Helper()

	if !strings.HasSuffix(name, ".sympath") {
		t.Fatalf("expected .sympath suffix, got %q", name)
	}

	base := strings.TrimSuffix(name, ".sympath")
	if len(base) != wantLength {
		t.Fatalf("expected basename length %d, got %d (%q)", wantLength, len(base), name)
	}

	for _, ch := range base {
		if !strings.ContainsRune(randomSympathAlphabet, ch) {
			t.Fatalf("unexpected character %q in %q", ch, name)
		}
	}
}
