package inventory

// identity.go manages the stable local-machine identity stored inside a
// sympath database.
//
// A database may contain scans from many machines after consolidation,
// but local rescans still need to know which machine identity to use
// for new rows. The CLI sets that identity explicitly from
// `~/.sympath/machine-id`, and the library persists it in the metadata
// table so future scans into the same database stay consistent.

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/google/uuid"
)

// PrepareLocalMachineDB configures db for local scanning and records the
// supplied machine identity in its metadata.
//
// This is the common setup path used by the CLI before scanning and
// during local-fragment consolidation. Keeping it in one helper avoids
// repeating connection setup and metadata writes at every call site.
func PrepareLocalMachineDB(ctx context.Context, db *sql.DB, identity MachineIdentity) error {
	if err := ConfigureConnection(ctx, db); err != nil {
		return err
	}
	return SetLocalMachineIdentity(ctx, db, identity)
}

// SetLocalMachineIdentity records the stable local machine identity that
// future calls to [InventoryTree] should use when scanning into db.
//
// The value is stored in the database's metadata table so callers can
// consolidate multiple source databases into a new target DB and then
// explicitly mark which machine identity should be used for the next
// local scan into that target.
func SetLocalMachineIdentity(ctx context.Context, db *sql.DB, identity MachineIdentity) error {
	if identity.MachineID == "" {
		return fmt.Errorf("machine ID is required")
	}
	if identity.Hostname == "" {
		return fmt.Errorf("hostname is required")
	}
	if err := EnsureSchema(ctx, db, identity); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO metadata (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		metadataLocalMachineIDKey, identity.MachineID,
	); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO metadata (key, value) VALUES (?, ?)
		 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		metadataLocalHostnameKey, identity.Hostname,
	); err != nil {
		return err
	}
	return tx.Commit()
}

// GetLocalMachineIdentity returns the machine identity persisted in db.
//
// If the database has never had a local identity assigned, a new stable
// UUID is generated, paired with the current OS hostname, stored in the
// metadata table, and returned. This keeps the original InventoryTree
// call pattern working for tests and simple library users even when they
// do not use the CLI's explicit machine-id file.
func GetLocalMachineIdentity(ctx context.Context, db *sql.DB) (MachineIdentity, error) {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown-host"
	}
	bootstrap := MachineIdentity{
		MachineID: uuid.NewString(),
		Hostname:  hostname,
	}
	if err := EnsureSchema(ctx, db, bootstrap); err != nil {
		return MachineIdentity{}, err
	}

	rows, err := db.QueryContext(ctx,
		"SELECT key, value FROM metadata WHERE key IN (?, ?)",
		metadataLocalMachineIDKey, metadataLocalHostnameKey,
	)
	if err != nil {
		return MachineIdentity{}, err
	}
	defer rows.Close()

	identity := MachineIdentity{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return MachineIdentity{}, err
		}
		switch key {
		case metadataLocalMachineIDKey:
			identity.MachineID = value
		case metadataLocalHostnameKey:
			identity.Hostname = value
		}
	}
	if err := rows.Err(); err != nil {
		return MachineIdentity{}, err
	}

	if identity.MachineID != "" && identity.Hostname != "" {
		return identity, nil
	}

	if identity.MachineID == "" {
		identity.MachineID = bootstrap.MachineID
	}
	if identity.Hostname == "" {
		identity.Hostname = bootstrap.Hostname
	}
	if err := SetLocalMachineIdentity(ctx, db, identity); err != nil {
		return MachineIdentity{}, err
	}
	return identity, nil
}
