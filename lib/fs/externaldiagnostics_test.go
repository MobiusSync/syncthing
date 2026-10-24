// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package fs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"
)

type diagnosticCountingFS struct {
	Filesystem
	calls []string
}

func (f *diagnosticCountingFS) DirNames(path string) ([]string, error) {
	f.calls = append(f.calls, "readdir:"+path)
	return f.Filesystem.DirNames(path)
}
func (f *diagnosticCountingFS) Lstat(path string) (FileInfo, error) {
	f.calls = append(f.calls, "lstat:"+path)
	return f.Filesystem.Lstat(path)
}

func TestExternalDiagnosticObservesWithoutAdditionalProbes(t *testing.T) {
	base := NewFilesystem(FilesystemTypeBasic, t.TempDir())
	if err := base.Mkdir(".stfolder", 0700); err != nil {
		t.Fatal(err)
	}
	if err := WriteFile(base, "private-title.txt", []byte("private contents"), 0600); err != nil {
		t.Fatal(err)
	}
	counter := &diagnosticCountingFS{Filesystem: base}
	wrapped := NewScanValidationFilesystem(counter, ".stfolder")
	walk := func() {
		t.Helper()
		if err := wrapped.Walk(".", func(string, FileInfo, error) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	walk()
	without := append([]string(nil), counter.calls...)
	counter.calls = nil
	var reports []ExternalScanReport
	SetExternalScanReporter(func(report ExternalScanReport) { reports = append(reports, report) })
	defer SetExternalScanReporter(nil)
	if BeginExternalScan(base, "internal", "sendreceive", 0) != nil {
		t.Fatal("instrumented internal folder")
	}
	d := BeginExternalScan(wrapped, "folder-id", "sendreceive", 0)
	walk()
	RecordExternalDeletion(wrapped, "private-title.txt", "lstat-not-exist")
	d.Rediscovered("private-title.txt")
	d.IndexedDeletionRecords(1)
	d.Phase("complete")
	d.Finish(nil)
	if !reflect.DeepEqual(counter.calls, without) {
		t.Fatalf("diagnostics changed probes: %v vs %v", counter.calls, without)
	}
	if len(reports) != 2 {
		t.Fatalf("reports: %+v", reports)
	}
	r := reports[1]
	if r.Event != "scan-end" || r.Result != "ok" || r.DirectoryReads != 2 || r.RootEntries != 2 || r.MarkersVerified != 1 || r.DeletionCandidates != 1 || r.IndexedDeletionRecords != 1 || r.Rediscovered != 1 {
		t.Fatalf("report: %+v", r)
	}
	if r.Deletions[0].PathHash != r.RediscoveredPaths[0] || r.Deletions[0].ParentEntries != 2 {
		t.Fatalf("correlation lost: %+v", r)
	}
	data, _ := json.Marshal(r)
	if strings.Contains(string(data), "private") || strings.Contains(string(data), base.URI()) {
		t.Fatalf("sensitive content in report: %s", data)
	}
	if wrapped.(*scanValidationFilesystem).scanDiagnostic() != nil {
		t.Fatal("diagnostic still attached")
	}
}

func TestExternalDiagnosticBoundsAndErrorClassification(t *testing.T) {
	d := &ExternalScanDiagnostic{directories: make(map[string]int)}
	for i := 0; i < externalDiagnosticDirectories+10; i++ {
		d.directory(fmt.Sprintf("directory-%d", i), 0, &os.PathError{Op: "readdir", Path: "secret", Err: syscall.EACCES})
	}
	if len(d.directories) != externalDiagnosticDirectories || len(d.report.Failures) != externalDiagnosticSamples {
		t.Fatalf("unbounded diagnostic: %d, %d", len(d.directories), len(d.report.Failures))
	}
	if d.report.DirectoryErrors != externalDiagnosticDirectories+10 || d.report.Failures[0].Result != fmt.Sprintf("errno:%d", syscall.EACCES) {
		t.Fatalf("lost actual error: %+v", d.report)
	}
	for i := 0; i < 100; i++ {
		d.Rediscovered("secret")
	}
	if len(d.report.RediscoveredPaths) != externalDiagnosticSamples || d.report.Rediscovered != 100 {
		t.Fatal("rediscovery bound/count")
	}
	if diagnosticError(errors.New("secret file failed")) != "other" {
		t.Fatal("raw error exposed")
	}
}
