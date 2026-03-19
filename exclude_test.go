package inventory

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestGetDBPath(t *testing.T) {
	dir := t.TempDir()
	dbFile := filepath.Join(dir, "test.sqlite")
	db, err := sql.Open("sqlite", dbFile)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Must ping to actually open the connection
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	got, err := getDBPath(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	absExpected, _ := resolveAbsPath(dbFile)
	t.Logf("expected: %s", absExpected)
	t.Logf("got:      %s", got)

	if got != absExpected {
		t.Errorf("path mismatch:\n  got  %q\n  want %q", got, absExpected)
	}
}
