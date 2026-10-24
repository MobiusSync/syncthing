// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package model

import (
	"errors"
	"sync"
	"testing"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

type partialDiagnosticProvider struct {
	fs.Filesystem
	partial bool
}

func (f *partialDiagnosticProvider) DirNames(path string) ([]string, error) {
	names, err := f.Filesystem.DirNames(path)
	if f.partial && path == "." {
		filtered := make([]string, 0, len(names))
		for _, name := range names {
			if name != "kept.txt" {
				filtered = append(filtered, name)
			}
		}
		return filtered, err
	}
	return names, err
}
func (f *partialDiagnosticProvider) Lstat(path string) (fs.FileInfo, error) {
	if f.partial && path == "kept.txt" {
		return nil, fs.ErrNotExist
	}
	return f.Filesystem.Lstat(path)
}

func TestExternalFolderDiagnosticCapturesFailureDeletionAndRecovery(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	base := f.mtimefs
	must(t, fs.WriteFile(base, "kept.txt", []byte("keep me"), 0600))
	must(t, f.scanSubdirs(t.Context(), nil))
	var mut sync.Mutex
	var reports []fs.ExternalScanReport
	fs.SetExternalScanReporter(func(report fs.ExternalScanReport) { mut.Lock(); defer mut.Unlock(); reports = append(reports, report) })
	defer fs.SetExternalScanReporter(nil)
	latest := func() fs.ExternalScanReport { mut.Lock(); defer mut.Unlock(); return reports[len(reports)-1] }
	failed := &unavailableProvider{Filesystem: base, mode: "empty-root", broken: true}
	f.mtimefs = fs.NewScanValidationFilesystem(failed, f.MarkerName)
	if err := f.scanSubdirs(t.Context(), nil); !errors.Is(err, fs.ErrIncompleteScan) {
		t.Fatal(err)
	}
	r := latest()
	if r.Result != "incomplete-enumeration" || r.RootEntries != 0 || r.MarkersVerified != 0 || r.DeletionCandidates != 0 || r.IndexedDeletionRecords != 0 {
		t.Fatalf("failed scan: %+v", r)
	}
	provider := &partialDiagnosticProvider{Filesystem: base, partial: true}
	f.mtimefs = fs.NewScanValidationFilesystem(provider, f.MarkerName)
	must(t, f.scanSubdirs(t.Context(), nil))
	r = latest()
	// Characterization: instrumentation must not pretend to fix this synthetic case.
	if r.Result != "ok" || r.MarkersVerified != 1 || r.DeletionCandidates != 1 || r.IndexedDeletionRecords != 1 || r.Deletions[0].Reason != "lstat-not-exist" {
		t.Fatalf("partial scan: %+v", r)
	}
	deletedHash := r.Deletions[0].PathHash
	fi, ok, err := f.db.GetDeviceFile(f.folderID, protocol.LocalDeviceID, "kept.txt")
	must(t, err)
	if !ok || !fi.IsDeleted() {
		t.Fatal("partial absence no longer characterized correctly")
	}
	if _, err := base.Lstat("kept.txt"); err != nil {
		t.Fatalf("real fixture changed: %v", err)
	}
	provider.partial = false
	must(t, f.scanSubdirs(t.Context(), nil))
	r = latest()
	if r.Result != "ok" || r.IndexedDeletionRecords != 0 || r.Rediscovered != 1 || r.RediscoveredPaths[0] != deletedHash {
		t.Fatalf("recovery: %+v", r)
	}
	must(t, base.Remove("kept.txt"))
	must(t, f.scanSubdirs(t.Context(), []string{"kept.txt"}))
	r = latest()
	if r.Subpaths != 1 || r.DeletionCandidates != 1 || r.IndexedDeletionRecords != 1 {
		t.Fatalf("real deletion: %+v", r)
	}
}
