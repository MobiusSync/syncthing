// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package syncthing

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/db/olddb"
	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/locations"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"google.golang.org/protobuf/proto"
)

const migrationFixtureFiles = 2003

// Expand the retained v1 fixture with enough synthetic files to exercise two
// complete importer batches and a final partial batch. The source is still a
// real LevelDB read through the production legacy adapter.
func seedActiveMigrationFixture(t *testing.T) map[string]protocol.FileInfo {
	t.Helper()
	seedLegacyMigrationFixture(t)
	legacy, err := leveldb.OpenFile(locations.Get(locations.LegacyDatabase), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()

	want := make(map[string]protocol.FileInfo)
	for i := 0; i < 2000; i++ {
		fi := protocol.FileInfo{
			Name:      fmt.Sprintf("nested/file-%04d.txt", i),
			Size:      4,
			ModifiedS: 1700000000,
			Sequence:  int64(i + 4),
			Version:   protocol.Vector{Counters: []protocol.Counter{{ID: 1, Value: uint64(i + 10)}}},
			Blocks:    []protocol.BlockInfo{{Size: 4, Hash: bytes.Repeat([]byte{byte(i)}, 32)}},
		}
		want[fi.Name] = fi
		value, err := proto.Marshal(fi.ToWire(true))
		if err != nil {
			t.Fatal(err)
		}
		key := append([]byte{olddb.KeyTypeDevice, 0, 0, 0, 0, 0, 0, 0, 1}, fi.Name...)
		if err := legacy.Put(key, value, nil); err != nil {
			t.Fatal(err)
		}
		seqKey := binary.BigEndian.AppendUint64([]byte{olddb.KeyTypeSequence, 0, 0, 0, 0}, uint64(fi.Sequence))
		if err := legacy.Put(seqKey, key, nil); err != nil {
			t.Fatal(err)
		}
	}
	// Virtual mtimes are migrated separately after the file batches.
	ondisk, _ := time.Unix(1700000000, 0).MarshalBinary()
	virtual, _ := time.Unix(1700000001, 0).MarshalBinary()
	mtimeKey := append([]byte{olddb.KeyTypeVirtualMtime, 0, 0, 0, 0}, "kept.txt"...)
	if err := legacy.Put(mtimeKey, append(ondisk, virtual...), nil); err != nil {
		t.Fatal(err)
	}
	return want
}

// Wrappers inject failures at real SQLite call boundaries. A successful Update
// returns only after SQLite commits, making cancellation deterministic without
// timing sleeps or depending on machine speed.
type migrationFaultDB struct {
	db.DB
	update   func(string, protocol.DeviceID, []protocol.FileInfo, ...db.UpdateOption) error
	putKV    func(string, []byte) error
	putMtime func(string, string, time.Time, time.Time) error
	close    func() error
}

func (d *migrationFaultDB) Update(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
	if d.update != nil {
		return d.update(folder, device, files, opts...)
	}
	return d.DB.Update(folder, device, files, opts...)
}

func (d *migrationFaultDB) PutKV(key string, value []byte) error {
	if d.putKV != nil {
		return d.putKV(key, value)
	}
	return d.DB.PutKV(key, value)
}

func (d *migrationFaultDB) PutMtime(folder, name string, ondisk, virtual time.Time) error {
	if d.putMtime != nil {
		return d.putMtime(folder, name, ondisk, virtual)
	}
	return d.DB.PutMtime(folder, name, ondisk, virtual)
}

func (d *migrationFaultDB) Close() error {
	if d.close != nil {
		return d.close()
	}
	return d.DB.Close()
}

func migrationTestOpener(t *testing.T, wrap func(db.DB, bool) db.DB) func(string, bool) (db.DB, error) {
	t.Helper()
	return func(path string, bulk bool) (db.DB, error) {
		var d db.DB
		var err error
		if bulk {
			d, err = sqlite.OpenForMigration(path)
		} else {
			d, err = sqlite.Open(path, sqlite.WithDeleteRetention(0))
		}
		if err != nil {
			return nil, err
		}
		return wrap(d, bulk), nil
	}
}

func legacyContents(t *testing.T, path string) map[string]string {
	t.Helper()
	legacy, err := leveldb.OpenFile(path, &opt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal("legacy database was not released/readable", err)
	}
	defer legacy.Close()
	rows := make(map[string]string)
	it := legacy.NewIterator(nil, nil)
	defer it.Release()
	for it.Next() {
		rows[string(it.Key())] = string(it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return rows
}

func assertMigrationIncomplete(t *testing.T, source map[string]string) {
	t.Helper()
	if got := legacyContents(t, locations.Get(locations.LegacyDatabase)); !maps.Equal(got, source) {
		t.Fatal("failed migration changed the source database")
	}
	if _, err := os.Stat(locations.Get(locations.LegacyDatabase) + "-migrated"); !os.IsNotExist(err) {
		t.Fatalf("failed migration renamed the source: %v", err)
	}
	d, err := sqlite.OpenForMigration(locations.Get(locations.Database))
	if err != nil {
		t.Fatal("migration did not release the destination database", err)
	}
	defer d.Close()
	if _, ok, err := db.NewMiscDB(d).Time("migrated-from-leveldb-at"); err != nil || ok {
		t.Fatalf("failed migration was marked complete: %t %v", ok, err)
	}
}

func assertMigrationRecovered(t *testing.T, source map[string]string, want map[string]protocol.FileInfo) {
	t.Helper()
	if err := TryMigrateDatabase(t.Context(), 0); err != nil {
		t.Fatal("retry failed", err)
	}
	// A second startup must be a no-op, not a second conversion or cleanup.
	if err := TryMigrateDatabase(t.Context(), 0); err != nil {
		t.Fatal("completed migration was not idempotent", err)
	}
	assertMigrationData(t, source, want, locations.Get(locations.LegacyDatabase)+"-migrated")
}

func assertMigrationData(t *testing.T, source map[string]string, want map[string]protocol.FileInfo, retainedPath string) {
	t.Helper()
	if got := legacyContents(t, retainedPath); !maps.Equal(got, source) {
		t.Fatal("successful retry changed the retained source database")
	}
	d, err := sqlite.Open(locations.Get(locations.Database), sqlite.WithDeleteRetention(0))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	files, iterErr := d.AllLocalFiles("migration-test", protocol.LocalDeviceID)
	count := 0
	for fi := range files {
		count++
		if original, ok := want[fi.Name]; ok {
			if fi.Size != original.Size || fi.ModifiedS != original.ModifiedS || !reflect.DeepEqual(fi.Version, original.Version) || len(fi.Blocks) != len(original.Blocks) || !reflect.DeepEqual(fi.Blocks, original.Blocks) {
				t.Fatalf("recovered file differs from source: %s: %+v", fi.Name, fi)
			}
		}
	}
	if err := iterErr(); err != nil || count != migrationFixtureFiles {
		t.Fatalf("recovered records: got %d want %d, error %v", count, migrationFixtureFiles, err)
	}
	for name, wantVersion := range map[string]uint64{"kept.txt": 3, "old-deleted.txt": 4, "nested": 2} {
		fi, ok, err := d.GetDeviceFile("migration-test", protocol.LocalDeviceID, name)
		if err != nil || !ok || fi.Version.Counter(1) != wantVersion || fi.IsDeleted() != (name == "old-deleted.txt") || fi.IsDirectory() != (name == "nested") {
			t.Fatalf("fixture metadata changed: %s: %+v %v", name, fi, err)
		}
	}
	ondisk, virtual := d.GetMtime("migration-test", "kept.txt")
	if ondisk != time.Unix(1700000000, 0) || virtual != time.Unix(1700000001, 0) {
		t.Fatalf("virtual mtimes changed: %v %v", ondisk, virtual)
	}
	if _, ok, err := db.NewMiscDB(d).Time("migrated-from-leveldb-at"); err != nil || !ok {
		t.Fatalf("successful retry has no completion marker: %t %v", ok, err)
	}
}

func TestLegacyMigrationCancellationAfterCommittedBatch(t *testing.T) {
	want := seedActiveMigrationFixture(t)
	source := legacyContents(t, locations.Get(locations.LegacyDatabase))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	written := 0
	opener := migrationTestOpener(t, func(d db.DB, bulk bool) db.DB {
		if !bulk {
			t.Fatal("cancelled import reached completion marker")
		}
		return &migrationFaultDB{DB: d, update: func(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
			if err := d.Update(folder, device, files, opts...); err != nil {
				return err
			}
			written += len(files)
			cancel()
			return nil
		}}
	})
	if err := tryMigrateDatabase(ctx, 0, opener); !errors.Is(err, context.Canceled) {
		t.Fatalf("active cancellation returned %v", err)
	}
	if written != 1000 {
		t.Fatalf("cancellation did not interrupt after the first real batch: %d", written)
	}
	assertMigrationIncomplete(t, source)
	assertMigrationRecovered(t, source, want)
}

func TestLegacyMigrationStorageFailuresPreserveSourceAndRetry(t *testing.T) {
	for _, stage := range []string{"batch-write", "mtimes", "bulk-close", "version", "marker"} {
		t.Run(stage, func(t *testing.T) {
			want := seedActiveMigrationFixture(t)
			source := legacyContents(t, locations.Get(locations.LegacyDatabase))
			cause := syscall.ENOSPC
			if stage == "bulk-close" {
				cause = syscall.EIO
			}
			injected := false
			updates := 0
			opener := migrationTestOpener(t, func(d db.DB, bulk bool) db.DB {
				w := &migrationFaultDB{DB: d}
				if bulk {
					w.update = func(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
						updates++
						if stage == "batch-write" && updates == 2 {
							injected = true
							return cause
						}
						return d.Update(folder, device, files, opts...)
					}
					if stage == "mtimes" {
						w.putMtime = func(string, string, time.Time, time.Time) error {
							injected = true
							return cause
						}
					}
					if stage == "bulk-close" {
						w.close = func() error {
							injected = true
							return errors.Join(d.Close(), cause)
						}
					}
				} else {
					w.putKV = func(key string, value []byte) error {
						if key == "misc/migrated-from-leveldb-by" && stage == "version" || key == "misc/migrated-from-leveldb-at" && stage == "marker" {
							injected = true
							return cause
						}
						return d.PutKV(key, value)
					}
				}
				return w
			})
			if err := tryMigrateDatabase(t.Context(), 0, opener); !errors.Is(err, cause) {
				t.Fatalf("storage failure lost underlying cause: %v", err)
			}
			if !injected || updates == 0 {
				t.Fatalf("failure did not exercise an active import: injected=%t batches=%d", injected, updates)
			}
			assertMigrationIncomplete(t, source)
			assertMigrationRecovered(t, source, want)
		})
	}
}

func TestUnreadableLegacyMigrationIsClassified(t *testing.T) {
	dir := seedLegacyMigrationFixture(t)
	if err := os.RemoveAll(locations.Get(locations.LegacyDatabase)); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(locations.Get(locations.LegacyDatabase), []byte("not a LevelDB directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := TryMigrateDatabase(t.Context(), 0); !errors.Is(err, ErrLegacyDatabase) {
		t.Fatalf("unreadable legacy database was not classified: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != filepath.Base(locations.Get(locations.LegacyDatabase)) {
		t.Fatalf("unreadable source caused destination changes: %+v %v", entries, err)
	}
}

func TestLegacyMigrationReadFailureDoesNotCommitPartialImport(t *testing.T) {
	seedLegacyMigrationFixture(t)
	legacy, err := leveldb.OpenFile(locations.Get(locations.LegacyDatabase), nil)
	if err != nil {
		t.Fatal(err)
	}
	// The third sequence record points at this malformed protobuf. The legacy
	// iterator must report its read error instead of marking two records complete.
	if err := legacy.Put(append([]byte{olddb.KeyTypeDevice, 0, 0, 0, 0, 0, 0, 0, 1}, "nested"...), []byte{0xff}, nil); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	source := legacyContents(t, locations.Get(locations.LegacyDatabase))
	if err := TryMigrateDatabase(t.Context(), 0); !errors.Is(err, ErrLegacyDatabase) {
		t.Fatalf("source read failure was not classified: %v", err)
	}
	assertMigrationIncomplete(t, source)
}

func TestLegacyMigrationFinalCloseFailureRemainsRecoverable(t *testing.T) {
	want := seedActiveMigrationFixture(t)
	source := legacyContents(t, locations.Get(locations.LegacyDatabase))
	injected := false
	opener := migrationTestOpener(t, func(d db.DB, bulk bool) db.DB {
		if bulk {
			return d
		}
		return &migrationFaultDB{DB: d, close: func() error {
			injected = true
			return errors.Join(d.Close(), syscall.EIO)
		}}
	})
	if err := tryMigrateDatabase(t.Context(), 0, opener); !errors.Is(err, syscall.EIO) || !injected {
		t.Fatalf("final close did not return the storage failure: %v injected=%t", err, injected)
	}
	// The normal SQLite transaction may already have durably committed the
	// marker. The source is retained, and retry must recognize the completed
	// data rather than lose it or overwrite it with a second migration.
	if got := legacyContents(t, locations.Get(locations.LegacyDatabase)); !maps.Equal(got, source) {
		t.Fatal("close failure changed the source")
	}
	if err := TryMigrateDatabase(t.Context(), 0); err != nil {
		t.Fatal("retry after final close failed", err)
	}
	assertMigrationData(t, source, want, locations.Get(locations.LegacyDatabase))
}
