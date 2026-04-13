package inventory

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestDeleteMachineData_RemovesEntriesWithoutForeignKeysPragma(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "data")
	dbPath := filepath.Join(dir, "inventory.sympath")
	identity := MachineIdentity{MachineID: "machine-a", Hostname: "host-a"}

	createTestTree(t, root, map[string]string{"file.txt": "content"})

	setupDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := PrepareLocalMachineDB(context.Background(), setupDB, identity); err != nil {
		t.Fatal(err)
	}
	if err := InventoryTree(context.Background(), setupDB, root); err != nil {
		t.Fatal(err)
	}
	setupDB.Close()

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := DeleteMachineData(context.Background(), db, []string{identity.MachineID}); err != nil {
		t.Fatal(err)
	}

	assertRowCount(t, db, "roots", 0)
	assertRowCount(t, db, "scans", 0)
	assertRowCount(t, db, "entries", 0)
}

func TestImportCurrentScans_ReplacesEntriesWithoutForeignKeysPragma(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, "data")
	sourcePath := filepath.Join(dir, "source.sympath")
	targetPath := filepath.Join(dir, "target.sympath")
	sourceIdentity := MachineIdentity{MachineID: "source-machine", Hostname: "source-host"}
	targetIdentity := MachineIdentity{MachineID: "target-machine", Hostname: "target-host"}

	createTestTree(t, root, map[string]string{"file.txt": "first"})

	sourceSetupDB, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := PrepareLocalMachineDB(context.Background(), sourceSetupDB, sourceIdentity); err != nil {
		t.Fatal(err)
	}
	if err := InventoryTree(context.Background(), sourceSetupDB, root); err != nil {
		t.Fatal(err)
	}
	sourceSetupDB.Close()

	targetSetupDB, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := PrepareLocalMachineDB(context.Background(), targetSetupDB, targetIdentity); err != nil {
		t.Fatal(err)
	}
	targetSetupDB.Close()

	targetDB, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer targetDB.Close()

	sourceDB, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceDB.Close()

	if _, err := ImportCurrentScans(context.Background(), targetDB, sourceDB); err != nil {
		t.Fatal(err)
	}
	assertRowCount(t, targetDB, "entries", 1)

	createTestTree(t, root, map[string]string{"file.txt": "second"})

	sourceUpdateDB, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := PrepareLocalMachineDB(context.Background(), sourceUpdateDB, sourceIdentity); err != nil {
		t.Fatal(err)
	}
	if err := InventoryTree(context.Background(), sourceUpdateDB, root); err != nil {
		t.Fatal(err)
	}
	sourceUpdateDB.Close()

	sourceDB.Close()
	sourceDB, err = sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceDB.Close()

	if _, err := ImportCurrentScans(context.Background(), targetDB, sourceDB); err != nil {
		t.Fatal(err)
	}

	assertRowCount(t, targetDB, "roots", 1)
	assertRowCount(t, targetDB, "scans", 1)
	assertRowCount(t, targetDB, "entries", 1)
}

func assertRowCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()

	var got int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s row count: want %d, got %d", table, want, got)
	}
}
