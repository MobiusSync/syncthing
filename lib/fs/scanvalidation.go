// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package fs

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
)

// ErrIncompleteScan means absence cannot safely be interpreted as deletion.
var ErrIncompleteScan = errors.New("external folder enumeration could not be verified")

type scanValidationFilesystem struct {
	Filesystem
	marker        string
	diagnosticMut sync.Mutex
	diagnostic    *ExternalScanDiagnostic
}

// NewScanValidationFilesystem checks provider-backed directory enumeration,
// including observing the marker in the listing used by the scan itself.
func NewScanValidationFilesystem(next Filesystem, marker string) Filesystem {
	return &scanValidationFilesystem{Filesystem: next, marker: marker}
}

func IsScanValidationFilesystem(f Filesystem) bool {
	_, ok := f.(*scanValidationFilesystem)
	return ok
}

type scanDirectoryFilesystem struct {
	Filesystem
	names      map[string][]string
	diagnostic *ExternalScanDiagnostic
}

func (f *scanDirectoryFilesystem) DirNames(path string) ([]string, error) {
	path = filepath.Clean(path)
	if names, ok := f.names[path]; ok {
		return names, nil
	}
	names, err := f.Filesystem.DirNames(path)
	f.diagnostic.directory(path, len(names), err)
	if err != nil {
		return nil, fmt.Errorf("%w: reading %q: %v", ErrIncompleteScan, path, err)
	}
	f.names[path] = names
	return names, nil
}

func (f *scanDirectoryFilesystem) listed(path string) (bool, error) {
	names, err := f.DirNames(filepath.Dir(path))
	if err != nil {
		return false, err
	}
	for _, name := range names {
		if name == filepath.Base(path) {
			return true, nil
		}
	}
	return false, nil
}

func (f *scanValidationFilesystem) Walk(root string, fn WalkFunc) error {
	diagnostic := f.scanDiagnostic()
	diagnostic.walk()
	view := &scanDirectoryFilesystem{Filesystem: f.Filesystem, names: make(map[string][]string), diagnostic: diagnostic}
	marker := filepath.Clean(f.marker)
	if marker == "." || filepath.IsAbs(marker) || marker == ".." || strings.HasPrefix(marker, ".."+string(filepath.Separator)) {
		diagnostic.failure("marker", marker, "invalid")
		return fmt.Errorf("%w: a folder marker is required", ErrIncompleteScan)
	}
	// Observe every component of custom marker paths, not just a separate stat.
	parent := "."
	for _, component := range strings.Split(marker, string(filepath.Separator)) {
		path := filepath.Join(parent, component)
		present, err := view.listed(path)
		if err != nil {
			return err
		}
		if !present {
			diagnostic.failure("marker", marker, "missing-from-listing")
			return fmt.Errorf("%w: marker %q missing from directory listing", ErrIncompleteScan, marker)
		}
		parent = path
	}
	diagnostic.markerVerified()
	root = filepath.Clean(root)
	if root != "." {
		// Only consider absence after successfully enumerating the parent.
		// A provider that silently omits entries can still defeat this check.
		present, err := view.listed(root)
		if err != nil {
			return err
		}
		if !present {
			return nil
		}
	}
	return NewWalkFilesystem(view).Walk(root, func(path string, info FileInfo, err error) error {
		if err != nil {
			diagnostic.failure("walk", path, diagnosticError(err))
			return fmt.Errorf("%w: walking %q: %v", ErrIncompleteScan, path, err)
		}
		return fn(path, info, nil)
	})
}
