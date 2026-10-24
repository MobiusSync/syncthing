// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

//go:build !windows

package syncthing

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/locations"
	"github.com/syncthing/syncthing/lib/protocol"
)

// This is host process-termination acceptance, not iOS/device or real ENOSPC
// evidence. The child is killed only after the first real SQLite batch commits;
// pipes make that point explicit without guessing migration timing from sleeps.
func TestLegacyMigrationForceTerminationAfterCommittedBatch(t *testing.T) {
	if os.Getenv("ST_MIGRATION_KILL_HELPER") == "1" {
		runMigrationKillHelper(t)
		return
	}

	want := seedActiveMigrationFixture(t)
	source := legacyContents(t, locations.Get(locations.LegacyDatabase))
	signalReader, signalWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer signalReader.Close()
	defer signalWriter.Close()
	holdReader, holdWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer holdReader.Close()
	defer holdWriter.Close()

	cmd := exec.Command(os.Args[0], "-test.run=^TestLegacyMigrationForceTerminationAfterCommittedBatch$", "-test.count=1")
	cmd.Env = append(os.Environ(), "ST_MIGRATION_KILL_HELPER=1", "ST_MIGRATION_KILL_DIR="+locations.GetBaseDir(locations.DataBaseDir))
	// FD 3 signals a committed batch; FD 4 keeps the child blocked in a real
	// read while the parent kills it, without allowing normal cleanup to run.
	cmd.ExtraFiles = []*os.File{signalWriter, holdReader}
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	// The parent must close its duplicate child ends so premature child exit
	// yields EOF instead of hanging until the timeout.
	_ = signalWriter.Close()
	_ = holdReader.Close()
	committed := make(chan error, 1)
	go func() {
		var signal [1]byte
		_, err := io.ReadFull(signalReader, signal[:])
		if err == nil && signal[0] != 1 {
			err = errors.New("unexpected migration commit signal")
		}
		committed <- err
	}()
	select {
	case err := <-committed:
		if err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			waited = true
			t.Fatalf("migration helper did not commit a batch: %v\n%s", err, output.String())
		}
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		waited = true
		t.Fatalf("migration helper did not reach first committed batch\n%s", output.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal("could not force-terminate migration", err)
	}
	err = cmd.Wait()
	waited = true
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) || exitErr.Success() {
		t.Fatalf("migration helper was not terminated: %v\n%s", err, output.String())
	}

	// A whole folder database exists on disk, not just a seeded partial mock.
	entries, err := os.ReadDir(locations.Get(locations.Database))
	if err != nil || len(entries) < 2 {
		t.Fatalf("terminated process left no real SQLite destination: %v %v", entries, err)
	}
	assertMigrationIncomplete(t, source)
	assertMigrationRecovered(t, source, want)
}

func runMigrationKillHelper(t *testing.T) {
	t.Helper()
	dir := os.Getenv("ST_MIGRATION_KILL_DIR")
	if !filepath.IsAbs(dir) {
		t.Fatal("helper requires its parent's absolute disposable directory")
	}
	if err := locations.SetBaseDir(locations.DataBaseDir, dir); err != nil {
		t.Fatal(err)
	}
	signal := os.NewFile(3, "migration-committed")
	hold := os.NewFile(4, "hold-after-commit")
	defer signal.Close()
	defer hold.Close()
	opener := migrationTestOpener(t, func(d db.DB, bulk bool) db.DB {
		if !bulk {
			t.Fatal("helper passed the intended interruption point")
		}
		return &migrationFaultDB{DB: d, update: func(folder string, device protocol.DeviceID, files []protocol.FileInfo, opts ...db.UpdateOption) error {
			if len(files) != 1000 {
				t.Fatalf("unexpected first batch size: %d", len(files))
			}
			if err := d.Update(folder, device, files, opts...); err != nil {
				return err
			}
			if _, err := signal.Write([]byte{1}); err != nil {
				return err
			}
			var b [1]byte
			_, err := hold.Read(b[:])
			return errors.Join(errors.New("parent did not kill migration helper"), err)
		}}
	})
	if err := tryMigrateDatabase(context.Background(), 0, opener); err != nil {
		t.Fatal(err)
	}
	t.Fatal("migration unexpectedly completed before force termination")
}
