// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

//go:build darwin

package syncthing

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/locations"
	"github.com/syncthing/syncthing/lib/protocol"
)

// Opt-in host acceptance on a small disposable mounted filesystem. Ordinary
// tests skip this; never point it at a normal developer or production volume.
// The guard requires a distinct volume, a capacity <=128 MiB and an explicit
// marker. A full filesystem must fail a real write or flush; no database
// error is injected. APFS can defer ENOSPC until the bulk import's final fsync.
func TestLegacyMigrationConstrainedVolume(t *testing.T) {
	root := os.Getenv("ST_MIGRATION_CONSTRAINED_ROOT")
	if root == "" {
		t.Skip("requires a disposable <=128 MiB volume and explicit opt-in marker")
	}
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	var volume, parent syscall.Statfs_t
	if err := syscall.Statfs(root, &volume); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Statfs(filepath.Dir(root), &parent); err != nil {
		t.Fatal(err)
	}
	capacity := uint64(volume.Blocks) * uint64(volume.Bsize)
	if volume.Fsid == parent.Fsid || capacity == 0 || capacity > 128<<20 {
		t.Fatalf("refusing to fill a non-disposable volume: capacity=%d distinct=%t", capacity, volume.Fsid != parent.Fsid)
	}
	marker, err := os.ReadFile(filepath.Join(root, ".mobius-migration-acceptance"))
	if err != nil || string(marker) != "disposable synthetic migration acceptance\n" {
		t.Fatal("disposable volume is missing its explicit acceptance marker", err)
	}

	t.Setenv("TMPDIR", root)
	want := seedActiveMigrationFixture(t)
	source := legacyContents(t, locations.Get(locations.LegacyDatabase))
	fillerPath := filepath.Join(locations.GetBaseDir(locations.DataBaseDir), "space-filler")
	defer os.Remove(fillerPath)
	batches := 0
	var filled int64
	opener := migrationTestOpener(t, func(d db.DB, bulk bool) db.DB {
		if !bulk {
			t.Fatal("full-volume import reached completion marker")
		}
		return &migrationFaultDB{DB: d, update: func(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
			batches++
			if err := d.Update(folder, device, files, opts...); err != nil {
				return err
			}
			if batches == 1 {
				// Exhaust only the guarded disposable mount after a real first
				// commit. Subsequent SQLite writes or their final flush must fail.
				filled = fillDisposableMigrationVolume(t, fillerPath)
			}
			return nil
		}}
	})
	err = tryMigrateDatabase(t.Context(), 0, opener)
	if !sqlite.IsStorageFull(err) && !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("real constrained-volume migration did not report full storage: %v", err)
	}
	if batches < 2 || filled == 0 {
		t.Fatalf("fault did not interrupt active writes: batches=%d filler=%d", batches, filled)
	}
	t.Logf("actual full-volume failure; filled after first 1000-record commit, attempted batches=%d volume=%d bytes filler=%d bytes error=%v", batches, capacity, filled, err)
	if err := os.Remove(fillerPath); err != nil {
		t.Fatal("could not clear disposable storage fault", err)
	}
	assertMigrationIncomplete(t, source)
	assertMigrationRecovered(t, source, want)
}

func fillDisposableMigrationVolume(t *testing.T, path string) int64 {
	t.Helper()
	filler, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer filler.Close()
	chunk := bytes.Repeat([]byte{0x5a}, 1<<20)
	var written int64
	for _, size := range []int{len(chunk), 4096} {
		for {
			n, err := filler.Write(chunk[:size])
			written += int64(n)
			if err != nil {
				if !errors.Is(err, syscall.ENOSPC) {
					t.Fatalf("unexpected disposable-volume write error: %v", err)
				}
				break
			}
			if written > 128<<20 {
				t.Fatal("disposable volume exceeded its guarded capacity")
			}
		}
	}
	if err := filler.Sync(); err != nil && !errors.Is(err, syscall.ENOSPC) {
		t.Fatal(err)
	}
	return written
}
