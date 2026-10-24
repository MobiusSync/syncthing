// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package fs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const externalDiagnosticSamples = 8
const externalDiagnosticDirectories = 4096

// These records describe observations, not proof of deletion. No paths, file
// contents, or raw errors (which may contain paths) are passed to the reporter.
type ExternalScanReport struct {
	Scan                   uint64                 `json:"scan"`
	Folder                 string                 `json:"folder"`
	FolderType             string                 `json:"folderType"`
	RootHash               string                 `json:"rootHash"`
	Event                  string                 `json:"event"`
	Phase                  string                 `json:"phase"`
	Result                 string                 `json:"result,omitempty"`
	ElapsedMS              int64                  `json:"elapsedMs"`
	Subpaths               int                    `json:"subpaths"`
	Walks                  int                    `json:"walks"`
	DirectoryReads         int                    `json:"directoryReads"`
	DirectoryErrors        int                    `json:"directoryErrors"`
	RootEntries            int                    `json:"rootEntries"` // -1 if not observed
	MarkersVerified        int                    `json:"markersVerified"`
	DeletionCandidates     int                    `json:"deletionCandidates"`
	IndexedDeletionRecords int                    `json:"indexedDeletionRecords"`
	Failures               []ExternalScanFailure  `json:"failures,omitempty"`
	Deletions              []ExternalScanDeletion `json:"deletions,omitempty"`
	Rediscovered           int                    `json:"rediscovered"`
	RediscoveredPaths      []string               `json:"rediscoveredPaths,omitempty"`
}

type ExternalScanFailure struct {
	Operation string `json:"operation"`
	PathHash  string `json:"pathHash"`
	Result    string `json:"result"`
}

type ExternalScanDeletion struct {
	PathHash      string `json:"pathHash"`
	Reason        string `json:"reason"`
	ParentEntries int    `json:"parentEntries"` // -1 if not observed/retained
}

var externalReporter struct {
	sync.RWMutex
	report func(ExternalScanReport)
}
var externalScanSequence atomic.Uint64

// SetExternalScanReporter enables observation for subsequent external scans.
// The reporter must be concurrency-safe and must not affect scan outcomes.
func SetExternalScanReporter(report func(ExternalScanReport)) {
	externalReporter.Lock()
	externalReporter.report = report
	externalReporter.Unlock()
}

type ExternalScanDiagnostic struct {
	mut         sync.Mutex
	report      ExternalScanReport
	started     time.Time
	directories map[string]int
	filesystem  *scanValidationFilesystem
	emit        func(ExternalScanReport)
}

// BeginExternalScan does not perform any filesystem probes. Non-external
// filesystems, and applications without a reporter, incur no observation work.
func BeginExternalScan(filesystem Filesystem, folder, folderType string, subpaths int) *ExternalScanDiagnostic {
	f, ok := filesystem.(*scanValidationFilesystem)
	if !ok {
		return nil
	}
	externalReporter.RLock()
	emit := externalReporter.report
	externalReporter.RUnlock()
	if emit == nil {
		return nil
	}
	d := &ExternalScanDiagnostic{
		report: ExternalScanReport{Scan: externalScanSequence.Add(1), Folder: folder, FolderType: folderType, RootHash: diagnosticPathHash(f.URI()),
			Event: "scan-start", Phase: "health", Subpaths: subpaths, RootEntries: -1},
		started: time.Now(), directories: make(map[string]int), filesystem: f, emit: emit,
	}
	emit(d.report)
	f.diagnosticMut.Lock()
	f.diagnostic = d
	f.diagnosticMut.Unlock()
	return d
}

func (f *scanValidationFilesystem) scanDiagnostic() *ExternalScanDiagnostic {
	f.diagnosticMut.Lock()
	defer f.diagnosticMut.Unlock()
	return f.diagnostic
}

func (d *ExternalScanDiagnostic) Phase(phase string) {
	if d == nil {
		return
	}
	d.mut.Lock()
	d.report.Phase = phase
	d.mut.Unlock()
}

func (d *ExternalScanDiagnostic) Finish(err error) {
	if d == nil {
		return
	}
	d.filesystem.diagnosticMut.Lock()
	if d.filesystem.diagnostic == d {
		d.filesystem.diagnostic = nil
	}
	d.filesystem.diagnosticMut.Unlock()
	d.mut.Lock()
	d.report.Event = "scan-end"
	d.report.Result = diagnosticError(err)
	if err == nil && d.report.Phase != "complete" {
		d.report.Result = "incomplete-return"
	}
	d.report.ElapsedMS = time.Since(d.started).Milliseconds()
	report := d.report
	report.Failures = append([]ExternalScanFailure(nil), report.Failures...)
	report.Deletions = append([]ExternalScanDeletion(nil), report.Deletions...)
	report.RediscoveredPaths = append([]string(nil), report.RediscoveredPaths...)
	d.mut.Unlock()
	d.emit(report)
}

func (d *ExternalScanDiagnostic) directory(path string, count int, err error) {
	if d == nil {
		return
	}
	d.mut.Lock()
	defer d.mut.Unlock()
	d.report.DirectoryReads++
	if err != nil {
		d.report.DirectoryErrors++
		d.failureLocked("readdir", path, diagnosticError(err))
		count = -1
	}
	if path == "." {
		d.report.RootEntries = count
	}
	if _, ok := d.directories[path]; ok || len(d.directories) < externalDiagnosticDirectories {
		d.directories[path] = count
	}
}

func (d *ExternalScanDiagnostic) walk() {
	if d == nil {
		return
	}
	d.mut.Lock()
	d.report.Walks++
	d.mut.Unlock()
}

func (d *ExternalScanDiagnostic) markerVerified() {
	if d == nil {
		return
	}
	d.mut.Lock()
	d.report.MarkersVerified++
	d.mut.Unlock()
}

func (d *ExternalScanDiagnostic) failure(operation, path, result string) {
	if d == nil {
		return
	}
	d.mut.Lock()
	defer d.mut.Unlock()
	d.failureLocked(operation, path, result)
}

func (d *ExternalScanDiagnostic) failureLocked(operation, path, result string) {
	if len(d.report.Failures) < externalDiagnosticSamples {
		d.report.Failures = append(d.report.Failures, ExternalScanFailure{operation, diagnosticPathHash(path), result})
	}
}

// RecordExternalDeletion records the decision already made by IsDeletedReason;
// it does not restat or reinterpret the result.
func RecordExternalDeletion(filesystem Filesystem, name, reason string) {
	if reason == "" {
		return
	}
	f, ok := filesystem.(*scanValidationFilesystem)
	if !ok {
		return
	}
	d := f.scanDiagnostic()
	if d == nil {
		return
	}
	d.mut.Lock()
	defer d.mut.Unlock()
	d.report.DeletionCandidates++
	if len(d.report.Deletions) < externalDiagnosticSamples {
		count, ok := d.directories[filepath.Dir(name)]
		if !ok {
			count = -1
		}
		d.report.Deletions = append(d.report.Deletions, ExternalScanDeletion{diagnosticPathHash(name), reason, count})
	}
}

// IndexedDeletionRecords counts tombstone records actually submitted to the
// local index, including receive-only records. It does not imply peer receipt.
func (d *ExternalScanDiagnostic) IndexedDeletionRecords(count int) {
	if d == nil {
		return
	}
	d.mut.Lock()
	d.report.IndexedDeletionRecords += count
	d.mut.Unlock()
}

func diagnosticPathHash(path string) string {
	hash := sha256.Sum256([]byte(filepath.ToSlash(filepath.Clean(path))))
	return hex.EncodeToString(hash[:8])
}

func diagnosticError(err error) string {
	if err == nil {
		return "ok"
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return fmt.Sprintf("errno:%d", errno)
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, ErrIncompleteScan):
		return "incomplete-enumeration"
	case IsNotExist(err):
		return "not-exist"
	case IsPermission(err):
		return "permission"
	default:
		return "other"
	}
}

// Rediscovered records a scanner result whose previous local index entry was
// deleted. This is a candidate update, not confirmation that a peer received it.
func (d *ExternalScanDiagnostic) Rediscovered(name string) {
	if d == nil {
		return
	}
	d.mut.Lock()
	defer d.mut.Unlock()
	d.report.Rediscovered++
	if len(d.report.RediscoveredPaths) < externalDiagnosticSamples {
		d.report.RediscoveredPaths = append(d.report.RediscoveredPaths, diagnosticPathHash(name))
	}
}
