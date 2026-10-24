// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package model

import (
	"errors"
	"testing"

	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
)

type unavailableProvider struct {
	fs.Filesystem
	mode   string
	broken bool
}

func (f *unavailableProvider) DirNames(path string) ([]string, error) {
	if f.broken {
		if f.mode == "empty-root" && path == "." {
			return nil, nil
		}
		if (f.mode == "root-error" && path == ".") || (f.mode == "subtree-error" && path == "nested") {
			return nil, fs.ErrNotExist
		}
	}
	return f.Filesystem.DirNames(path)
}
func (f *unavailableProvider) Lstat(path string) (fs.FileInfo, error) {
	if f.broken && (path == "nested/kept.txt" || (path == "kept.txt" && f.mode != "subtree-error")) {
		return nil, fs.ErrNotExist
	}
	return f.Filesystem.Lstat(path)
}

func TestExternalFolderFailedScanPreservesIndex(t *testing.T) {
	for _, mode := range []string{"empty-root", "root-error", "subtree-error"} {
		t.Run(mode, func(t *testing.T) {
			_, f := setupSendReceiveFolder(t)
			base := f.mtimefs
			must(t, base.MkdirAll("nested", 0700))
			must(t, fs.WriteFile(base, "kept.txt", []byte("keep me"), 0600))
			must(t, fs.WriteFile(base, "nested/kept.txt", []byte("keep me too"), 0600))
			must(t, f.scanSubdirs(t.Context(), nil))
			provider := &unavailableProvider{Filesystem: base, mode: mode, broken: true}
			f.mtimefs = fs.NewScanValidationFilesystem(provider, f.MarkerName)
			if err := f.scanSubdirs(t.Context(), nil); !errors.Is(err, fs.ErrIncompleteScan) {
				t.Fatalf("want incomplete scan, got %v", err)
			}
			for _, name := range []string{"kept.txt", "nested/kept.txt"} {
				file, ok, err := f.db.GetDeviceFile(f.folderID, protocol.LocalDeviceID, name)
				must(t, err)
				if !ok || file.IsDeleted() {
					t.Errorf("%s disappeared from index: %+v", name, file)
				}
			}
			provider.broken = false
			must(t, f.scanSubdirs(t.Context(), nil)) // A later healthy scan recovers without manual reset.
		})
	}
}

func TestExternalFolderHealthyScanAllowsRealDeletion(t *testing.T) {
	_, f := setupSendReceiveFolder(t)
	base := f.mtimefs
	must(t, fs.WriteFile(base, "deleted.txt", []byte("delete me"), 0600))
	must(t, f.scanSubdirs(t.Context(), nil))
	must(t, base.Remove("deleted.txt"))
	f.mtimefs = fs.NewScanValidationFilesystem(base, f.MarkerName)
	must(t, f.scanSubdirs(t.Context(), []string{"deleted.txt"}))
	file, ok, err := f.db.GetDeviceFile(f.folderID, protocol.LocalDeviceID, "deleted.txt")
	must(t, err)
	if !ok || !file.IsDeleted() {
		t.Fatalf("real deletion not indexed: %+v", file)
	}
}
