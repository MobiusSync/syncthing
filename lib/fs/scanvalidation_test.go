// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package fs

import (
	"errors"
	"testing"
)

func TestScanValidationMarkerComesFromEnumeration(t *testing.T) {
	base := NewFilesystem(FilesystemTypeBasic, t.TempDir())
	if err := base.Mkdir(".stfolder", 0700); err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{".stfolder", "missing", "", "../outside"} {
		f := NewScanValidationFilesystem(base, marker)
		err := f.Walk(".", func(string, FileInfo, error) error { return nil })
		if marker == ".stfolder" {
			if err != nil {
				t.Fatal(err)
			}
		} else if !errors.Is(err, ErrIncompleteScan) {
			t.Errorf("marker %q: %v", marker, err)
		}
	}
}
