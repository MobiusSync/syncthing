// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package embedded

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/syncthing"
)

func TestEmbeddedSupportBundleUsesLiveCollectorWithoutChangingConsent(t *testing.T) {
	dir := t.TempDir()
	cert, err := syncthing.LoadOrGenerateCertificate(filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"))
	if err != nil {
		t.Fatal(err)
	}
	id := protocol.NewDeviceID(cert.Certificate[0])
	cfg := config.New(id)
	cfg.Options.RawListenAddresses = []string{"tcp://127.0.0.1:0"}
	cfg.Options.GlobalAnnEnabled, cfg.Options.LocalAnnEnabled = false, false
	cfg.Options.NATEnabled, cfg.Options.RelaysEnabled = false, false
	cfg.Options.StartBrowser, cfg.Options.SetLowPriority = false, false
	cfg.Options.AutoUpgradeIntervalH, cfg.Options.URAccepted = 0, -1
	cfg.Options.CREnabled = false
	wrapper := config.Wrap(filepath.Join(dir, "config.xml"), cfg, id, events.NoopLogger)
	if err := wrapper.Save(); err != nil {
		t.Fatal(err)
	}
	var service Service
	result := make(chan error, 1)
	go func() {
		result <- service.Run(Options{ConfigDir: dir, GUIAddress: "http://127.0.0.1:0", AllowMigration: true})
	}()
	defer func() {
		service.Stop()
		select {
		case err := <-result:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("engine did not stop after support export")
		}
	}()
	deadline := time.After(10 * time.Second)
	for !service.IsRunning() {
		select {
		case <-deadline:
			t.Fatal("engine did not start")
		case <-time.After(10 * time.Millisecond):
		}
	}
	output := filepath.Join(t.TempDir(), "support.zip")
	file, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	archive := zip.NewWriter(file)
	if err := service.WriteSupportBundle(context.Background(), archive); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := zip.OpenReader(output)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	files := make(map[string][]byte)
	profiles := make(map[string]bool)
	for _, entry := range reader.File {
		r, err := entry.Open()
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		files[entry.Name] = data
		for _, kind := range []string{"goroutines", "heap", "cpu", "block"} {
			if strings.HasPrefix(entry.Name, "syncthing-"+kind+"-") && len(data) > 0 {
				profiles[kind] = true
			}
		}
	}
	for _, name := range []string{"config.json.txt", "log-inmemory.txt", "version-platform.json.txt", "metrics.txt", "connection-stats.json.txt", "database-summary.json.txt", "usage-reporting.json.txt"} {
		if len(files[name]) == 0 {
			t.Fatalf("missing standard live section %s", name)
		}
	}
	if len(profiles) != 4 {
		t.Fatalf("missing standard performance recordings: %v", profiles)
	}
	if _, ok := files["key.pem"]; ok {
		t.Fatal("collected identity key")
	}

	service.mu.RLock()
	consent := service.cfg.Options().URAccepted
	service.mu.RUnlock()
	if consent != -1 {
		t.Fatal("support collection changed reporting consent")
	}
	if bundles, _ := filepath.Glob(filepath.Join(dir, "support-bundle-*.zip")); len(bundles) != 0 {
		t.Fatal("saved a configuration-directory archive")
	}
}

func TestBackgroundMigrationIsRejectedBeforeStateChanges(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, "index-v0.14.0.db")
	if err := os.Mkdir(legacy, 0700); err != nil {
		t.Fatal(err)
	}
	var service Service
	err := service.Run(Options{ConfigDir: dir})
	if !errors.Is(err, ErrMigrationRequired) {
		t.Fatalf("want migration-required, got %v", err)
	}
	for _, name := range []string{"config.xml", "cert.pem", "key.pem", "index-v2"} {
		if _, err := os.Lstat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Fatalf("background start changed %s: %v", name, err)
		}
	}
	if service.IsRunning() || service.IsMigrating() || !service.IsIdle() {
		t.Fatal("rejected startup left the service active")
	}
}

func TestStartupFailureReturnsWithoutStartingEngine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, []byte("keep this file"), 0600); err != nil {
		t.Fatal(err)
	}
	var service Service
	if err := service.Run(Options{ConfigDir: path, AllowMigration: true}); err == nil {
		t.Fatal("startup failure was reported as success")
	}
	if service.IsRunning() || !service.IsIdle() {
		t.Fatal("failed startup left the engine active")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "keep this file" {
		t.Fatal("startup failure damaged its input", err)
	}
}

func TestRepeatedStartStopPreservesIdentityAndReleasesDatabase(t *testing.T) {
	dir := t.TempDir()
	cert, err := syncthing.LoadOrGenerateCertificate(filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"))
	if err != nil {
		t.Fatal(err)
	}
	id := protocol.NewDeviceID(cert.Certificate[0])
	cfg := config.New(id)
	cfg.GUI.Enabled = false
	cfg.Options.RawListenAddresses = []string{"tcp://127.0.0.1:0"}
	cfg.Options.GlobalAnnEnabled = false
	cfg.Options.LocalAnnEnabled = false
	cfg.Options.NATEnabled = false
	cfg.Options.RelaysEnabled = false
	cfg.Options.StartBrowser = false
	cfg.Options.SetLowPriority = false
	cfg.Options.AutoUpgradeIntervalH = 0
	cfg.Options.URAccepted = -1
	cfg.Options.CREnabled = false
	wrapper := config.Wrap(filepath.Join(dir, "config.xml"), cfg, id, events.NoopLogger)
	if err := wrapper.Save(); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(dir, "key.pem"))
	if err != nil {
		t.Fatal(err)
	}
	var service Service
	for run := 0; run < 2; run++ {
		result := make(chan error, 1)
		go func() { result <- service.Run(Options{ConfigDir: dir, AllowMigration: true}) }()
		timer := time.NewTimer(10 * time.Second)
		ticker := time.NewTicker(10 * time.Millisecond)
		for !service.IsRunning() {
			select {
			case err := <-result:
				t.Fatal("engine did not start", err)
			case <-timer.C:
				service.Stop()
				t.Fatal("engine start timed out")
			case <-ticker.C:
			}
		}
		timer.Stop()
		ticker.Stop()
		// A second starter must not mutate process-global locations or open the DB.
		var second Service
		if err := second.Run(Options{ConfigDir: t.TempDir()}); !errors.Is(err, ErrAlreadyRunning) {
			t.Fatalf("duplicate startup: %v", err)
		}
		service.Stop()
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("engine stop did not finish")
		}
		if service.IsRunning() || !service.IsIdle() {
			t.Fatal("stop did not clear readiness")
		}
		after, err := os.ReadFile(filepath.Join(dir, "key.pem"))
		if err != nil || !bytes.Equal(original, after) {
			t.Fatal("restart changed device identity", err)
		}
	}
}

func TestInvalidLegacyDatabaseDoesNotBecomeAnEmptyIndex(t *testing.T) {
	dir := t.TempDir()
	legacy := filepath.Join(dir, "index-v0.14.0.db")
	if err := os.Mkdir(legacy, 0700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(legacy, "preserve-this-state")
	if err := os.WriteFile(sentinel, []byte("original legacy state"), 0600); err != nil {
		t.Fatal(err)
	}
	var service Service
	if err := service.Run(Options{ConfigDir: dir, AllowMigration: true}); err == nil {
		t.Fatal("invalid legacy database was silently replaced")
	}
	if _, err := os.Lstat(filepath.Join(dir, "index-v2")); !os.IsNotExist(err) {
		t.Fatal("created empty index after a failed legacy open", err)
	}
	data, err := os.ReadFile(sentinel)
	if err != nil || string(data) != "original legacy state" {
		t.Fatal("failed migration damaged legacy state", err)
	}
}
