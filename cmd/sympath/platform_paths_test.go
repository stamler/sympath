package main

import (
	"path/filepath"
	"testing"
)

func TestSympathStateDir_PrefersHOMEOverride(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	dir, err := sympathStateDir()
	if err != nil {
		t.Fatal(err)
	}

	if dir != filepath.Join(home, ".sympath") {
		t.Fatalf("expected state dir inside HOME override, got %q", dir)
	}
}

func TestReadOnlySQLiteDSNForGOOS_WindowsDrivePath(t *testing.T) {
	got := readOnlySQLiteDSNForGOOS(`C:\Users\runneradmin\AppData\Local\Temp\ui.sympath`, "windows")
	want := "file:///C:/Users/runneradmin/AppData/Local/Temp/ui.sympath?mode=ro"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestReadOnlySQLiteDSNForGOOS_UnixPath(t *testing.T) {
	got := readOnlySQLiteDSNForGOOS("/tmp/ui.sympath", "linux")
	want := "file:/tmp/ui.sympath?mode=ro"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
