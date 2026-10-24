// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package api

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

func supportTestService(t *testing.T) *service {
	id := protocol.NewDeviceID([]byte("support-test"))
	cfg := config.New(id)
	cfg.GUI.APIKey, cfg.GUI.Password = "test-private-api-key", "test-private-password-hash"
	cfg.Options.URAccepted = -1
	cfg.Options.RawGlobalAnnServers = []string{"https://user:url-password@example.test/discover?token=url-token"}
	cfg.Defaults.Folder.Devices = []config.FolderDeviceConfiguration{{DeviceID: id, EncryptionPassword: "test-default-encryption-secret"}}
	cfg.Folders = []config.FolderConfiguration{{ID: "folder", Path: "photos:Camera Roll (folder)", Type: config.FolderTypeSendOnly,
		Devices: []config.FolderDeviceConfiguration{{DeviceID: id, EncryptionPassword: "test-folder-encryption-secret"}}}}
	return &service{cfg: config.Wrap(t.TempDir()+"/config.xml", cfg, id, events.NoopLogger), systemLog: slogutil.GlobalRecorder, guiErrors: slogutil.ErrorRecorder}
}
func supportEntry(t *testing.T, buffer *bytes.Buffer, name string) []byte {
	t.Helper()
	reader, err := zip.NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range reader.File {
		if entry.Name != name {
			continue
		}
		r, err := entry.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	t.Fatalf("missing %s", name)
	return nil
}
func TestSupportSettingsUseExactlyTheUpstreamPasswordRemoval(t *testing.T) {
	s := supportTestService(t)
	var output bytes.Buffer
	archive := zip.NewWriter(&output)
	c := supportCollection{ctx: context.Background(), archive: archive}
	c.json("config.json.txt", getRedactedConfig(s))
	if c.err != nil {
		t.Fatal(c.err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	data := supportEntry(t, &output, "config.json.txt")
	for _, secret := range []string{"test-private-api-key", "test-private-password-hash", "test-folder-encryption-secret"} {
		if bytes.Contains(data, []byte(secret)) {
			t.Fatalf("upstream password removal lost: %s", secret)
		}
	}
	// Parity is deliberate: do not silently reintroduce Möbius-only filtering.
	for _, retained := range []string{"test-default-encryption-secret", "url-password", "url-token"} {
		if !bytes.Contains(data, []byte(retained)) {
			t.Fatalf("added non-upstream filtering: %s", retained)
		}
	}
	if s.cfg.RawCopy().GUI.APIKey != "test-private-api-key" {
		t.Fatal("changed live settings")
	}
}
func TestSupportDiskSectionsAreStreamedWithoutTheOldSizeOmission(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	payload := bytes.Repeat([]byte("retained diagnostic history\n"), 120000)
	if err := os.WriteFile(path, payload, 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	archive := zip.NewWriter(&output)
	c := supportCollection{ctx: context.Background(), archive: archive}
	c.file("log-ondisk.txt", path)
	if c.err != nil {
		t.Fatal(c.err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(supportEntry(t, &output, "log-ondisk.txt"), payload) {
		t.Fatal("lost history exceeding former per-section limit")
	}
}
func TestSupportProfilesCancelAndReleaseTheCPURecorder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	c := supportCollection{ctx: ctx, archive: zip.NewWriter(io.Discard)}
	c.profiles()
	if !errors.Is(c.err, context.DeadlineExceeded) {
		t.Fatalf("want cancellation, got %v", c.err)
	}
	if err := pprof.StartCPUProfile(io.Discard); err != nil {
		t.Fatal("CPU recorder left running:", err)
	}
	pprof.StopCPUProfile()
}
