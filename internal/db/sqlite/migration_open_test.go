// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package sqlite

import (
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestMigrationCatalogUsesNormalJournaling(t *testing.T) {
	path := t.TempDir()
	bulk, err := OpenForMigration(path)
	if err != nil {
		t.Fatal(err)
	}
	defer bulk.Close()
	assertNormalMigrationCatalog(t, bulk)
	if err := bulk.Update("folder", protocol.LocalDeviceID, []protocol.FileInfo{{Name: "file", Version: protocol.Vector{Counters: []protocol.Counter{{ID: 1, Value: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	folder, err := bulk.getFolderDB("folder", false)
	if err != nil {
		t.Fatal(err)
	}
	var folderJournal string
	var folderSync int
	if err := folder.sql.Get(&folderJournal, "PRAGMA journal_mode"); err != nil {
		t.Fatal(err)
	}
	if err := folder.sql.Get(&folderSync, "PRAGMA synchronous"); err != nil {
		t.Fatal(err)
	}
	if folderJournal != "off" || folderSync != 0 {
		t.Fatalf("folder bulk optimizations changed: journal=%s synchronous=%d", folderJournal, folderSync)
	}
	if err := bulk.Close(); err != nil {
		t.Fatal(err)
	}
	completion, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer completion.Close()
	assertNormalMigrationCatalog(t, completion)
	if err := db.NewMiscDB(completion).PutTime("migrated-from-leveldb-at", time.Now()); err != nil {
		t.Fatal(err)
	}
	assertNormalMigrationCatalog(t, completion)
	if err := completion.Close(); err != nil {
		t.Fatal(err)
	}
	// A retained legacy source causes another migration open on retry. Reading
	// an already committed marker must not switch the catalog to unsafe mode.
	retry, err := OpenForMigration(path)
	if err != nil {
		t.Fatal(err)
	}
	defer retry.Close()
	assertNormalMigrationCatalog(t, retry)
	if _, ok, err := db.NewMiscDB(retry).Time("migrated-from-leveldb-at"); err != nil || !ok {
		t.Fatalf("completion marker was not retained: %t %v", ok, err)
	}
}

func assertNormalMigrationCatalog(t *testing.T, d *DB) {
	t.Helper()
	var journal string
	var synchronous int
	if err := d.sql.Get(&journal, "PRAGMA journal_mode"); err != nil {
		t.Fatal(err)
	}
	if err := d.sql.Get(&synchronous, "PRAGMA synchronous"); err != nil {
		t.Fatal(err)
	}
	if journal != "wal" || synchronous != 1 {
		t.Fatalf("migration catalog uses unsafe settings: journal=%s synchronous=%d", journal, synchronous)
	}
}
