// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package syncthing

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/locations"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"testing"
)

func TestCancelledLegacyMigrationReturnsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := TryMigrateDatabase(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled migration continued: %v", err)
	}
}

// The fixture is a key/value dump produced by the retained 1.29.2 engine from
// synthetic file records; no user database, filenames or credentials are used.
func seedLegacyMigrationFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	previous := locations.GetBaseDir(locations.DataBaseDir)
	if err := locations.SetBaseDir(locations.DataBaseDir, dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := locations.SetBaseDir(locations.DataBaseDir, previous); err != nil {
			t.Error(err)
		}
	})
	data, err := os.ReadFile("testdata/legacy-index.json")
	if err != nil {
		t.Fatal(err)
	}
	var rows []struct{ Key, Value []byte }
	if err := json.Unmarshal(data, &rows); err != nil {
		t.Fatal(err)
	}
	legacy, err := leveldb.OpenFile(locations.Get(locations.LegacyDatabase), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := legacy.Put(row.Key, row.Value, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestLegacyMigrationPreservesFileVersionsAndOldDeletes(t *testing.T) {
	seedLegacyMigrationFixture(t)
	if err := TryMigrateDatabase(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	migrated, err := sqlite.Open(locations.Get(locations.Database), sqlite.WithDeleteRetention(0))
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	file, ok, err := migrated.GetDeviceFile("migration-test", protocol.LocalDeviceID, "kept.txt")
	if err != nil || !ok || file.IsDeleted() || file.Size != 4 || len(file.Blocks) != 1 || file.Version.Counter(1) != 3 {
		t.Fatalf("live file changed during migration: %+v %v", file, err)
	}
	deleted, ok, err := migrated.GetDeviceFile("migration-test", protocol.LocalDeviceID, "old-deleted.txt")
	if err != nil || !ok || !deleted.IsDeleted() || deleted.Version.Counter(1) != 4 {
		t.Fatalf("old deletion lost during migration: %+v %v", deleted, err)
	}
	directory, ok, err := migrated.GetDeviceFile("migration-test", protocol.LocalDeviceID, "nested")
	if err != nil || !ok || !directory.IsDirectory() {
		t.Fatalf("directory changed: %+v %v", directory, err)
	}
	if _, err := os.Stat(locations.Get(locations.LegacyDatabase) + "-migrated"); err != nil {
		t.Fatal("legacy state was not retained", err)
	}
}

func TestMigrationRetryRebuildsPartialDestination(t *testing.T) {
	seedLegacyMigrationFixture(t)
	partial, err := sqlite.OpenForMigration(locations.Get(locations.Database))
	if err != nil {
		t.Fatal(err)
	}
	if err := partial.Update("migration-test", protocol.LocalDeviceID, []protocol.FileInfo{{Name: "partial-only.txt", Version: protocol.Vector{Counters: []protocol.Counter{{ID: 1, Value: 1}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := partial.Close(); err != nil {
		t.Fatal(err)
	}
	if err := TryMigrateDatabase(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	migrated, err := sqlite.Open(locations.Get(locations.Database), sqlite.WithDeleteRetention(0))
	if err != nil {
		t.Fatal(err)
	}
	defer migrated.Close()
	if _, ok, err := migrated.GetDeviceFile("migration-test", protocol.LocalDeviceID, "partial-only.txt"); err != nil || ok {
		t.Fatal("partial destination leaked into resumed migration", err)
	}
	if _, ok, err := migrated.GetDeviceFile("migration-test", protocol.LocalDeviceID, "kept.txt"); err != nil || !ok {
		t.Fatal("retry did not restore original state", err)
	}
}
