// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package embedded

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/locations"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/syncthing"
	"github.com/syndtr/goleveldb/leveldb"
)

func identityTestDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	previousConfig := locations.GetBaseDir(locations.ConfigBaseDir)
	previousData := locations.GetBaseDir(locations.DataBaseDir)
	if err := locations.SetBaseDir(locations.ConfigBaseDir, dir); err != nil {
		t.Fatal(err)
	}
	if err := locations.SetBaseDir(locations.DataBaseDir, dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := locations.SetBaseDir(locations.ConfigBaseDir, previousConfig); err != nil {
			t.Error(err)
		}
		if err := locations.SetBaseDir(locations.DataBaseDir, previousData); err != nil {
			t.Error(err)
		}
	})
	return dir
}

func identityStateHashes(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	result := make(map[string][32]byte)
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			result[path] = [32]byte{}
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		result[path] = sha256.Sum256(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestEmbeddedIdentityFailureNeverRegeneratesExistingDevice(t *testing.T) {
	for _, scenario := range []string{"corrupt-cert", "corrupt-key", "missing-cert", "missing-key", "missing-pair-config", "missing-pair-legacy", "missing-pair-sqlite", "missing-pair-config-archive", "missing-pair-retained-legacy"} {
		t.Run(scenario, func(t *testing.T) {
			dir := identityTestDir(t)
			original, err := loadDeviceIdentity()
			if err != nil {
				t.Fatal(err)
			}
			id := protocol.NewDeviceID(original.Certificate[0])
			certBytes, err := os.ReadFile(locations.Get(locations.CertFile))
			if err != nil {
				t.Fatal(err)
			}
			keyBytes, err := os.ReadFile(locations.Get(locations.KeyFile))
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(scenario, "corrupt-") {
				path := locations.Get(locations.CertFile)
				if scenario == "corrupt-key" {
					path = locations.Get(locations.KeyFile)
				}
				if err := os.WriteFile(path, []byte("damaged identity: preserve for recovery"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if strings.HasPrefix(scenario, "missing-pair-") {
				if err := os.Remove(locations.Get(locations.CertFile)); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(locations.Get(locations.KeyFile)); err != nil {
					t.Fatal(err)
				}
				path := map[string]string{
					"missing-pair-config":          locations.Get(locations.ConfigFile),
					"missing-pair-legacy":          locations.Get(locations.LegacyDatabase),
					"missing-pair-sqlite":          locations.Get(locations.Database),
					"missing-pair-config-archive":  locations.Get(locations.ConfigFile) + ".v37",
					"missing-pair-retained-legacy": locations.Get(locations.LegacyDatabase) + "-migrated",
				}[scenario]
				if strings.Contains(scenario, "legacy") || strings.Contains(scenario, "sqlite") {
					if err := os.Mkdir(path, 0o700); err != nil {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(path, []byte("existing configuration: preserve for recovery"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				path := locations.Get(locations.CertFile)
				if scenario == "missing-key" {
					path = locations.Get(locations.KeyFile)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
			before := identityStateHashes(t, dir)
			var service Service
			err = service.Run(Options{ConfigDir: dir, AllowMigration: true})
			if err == nil || !strings.Contains(err.Error(), "load device identity") {
				t.Fatalf("invalid identity did not stop startup: %v", err)
			}
			if strings.Contains(scenario, "missing-") && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("identity load lost missing-file cause: %v", err)
			}
			if after := identityStateHashes(t, dir); !maps.Equal(before, after) {
				t.Fatal("failed startup changed identity/config/database bytes")
			}
			if service.IsRunning() || service.IsMigrating() || !service.IsIdle() {
				t.Fatal("identity failure left the service active")
			}
			if err := os.WriteFile(locations.Get(locations.CertFile), certBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(locations.Get(locations.KeyFile), keyBytes, 0o600); err != nil {
				t.Fatal(err)
			}
			restored, err := loadDeviceIdentity()
			if err != nil || protocol.NewDeviceID(restored.Certificate[0]) != id {
				t.Fatalf("restoring original identity did not recover original device ID: %v", err)
			}
		})
	}
}

func TestEmbeddedFreshIdentityGeneration(t *testing.T) {
	identityTestDir(t)
	cert, err := loadDeviceIdentity()
	if err != nil || len(cert.Certificate) == 0 {
		t.Fatalf("fresh installation could not create identity: %v", err)
	}
	loaded, err := tls.LoadX509KeyPair(locations.Get(locations.CertFile), locations.Get(locations.KeyFile))
	if err != nil || protocol.NewDeviceID(loaded.Certificate[0]) != protocol.NewDeviceID(cert.Certificate[0]) {
		t.Fatalf("new identity was not persisted: %v", err)
	}
}

func saveQuietIdentityConfig(t *testing.T, id protocol.DeviceID) []byte {
	t.Helper()
	cfg := config.New(id)
	cfg.GUI.Enabled = false
	cfg.Options.RawListenAddresses = []string{"tcp://127.0.0.1:0"}
	cfg.Options.GlobalAnnEnabled, cfg.Options.LocalAnnEnabled = false, false
	cfg.Options.NATEnabled, cfg.Options.RelaysEnabled = false, false
	cfg.Options.StartBrowser, cfg.Options.SetLowPriority = false, false
	cfg.Options.AutoUpgradeIntervalH, cfg.Options.URAccepted = 0, -1
	cfg.Options.CREnabled = false
	if err := config.Wrap(locations.Get(locations.ConfigFile), cfg, id, events.NoopLogger).Save(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(locations.Get(locations.ConfigFile))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func runIdentityRecoveryService(t *testing.T, dir string) {
	t.Helper()
	var service Service
	result := make(chan error, 1)
	go func() { result <- service.Run(Options{ConfigDir: dir, AllowMigration: true}) }()
	defer func() {
		service.Stop()
		select {
		case err := <-result:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("recovered engine did not release startup/database handles")
		}
	}()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !service.IsRunning() {
		select {
		case err := <-result:
			// Reinsert the terminal result for the cleanup join.
			result <- err
			t.Fatalf("retry did not start engine: %v", err)
		case <-deadline.C:
			t.Fatal("retry did not finish engine startup")
		case <-ticker.C:
		}
	}
}

func TestEmbeddedRetryAfterOriginalIdentityRestored(t *testing.T) {
	dir := identityTestDir(t)
	cert, err := loadDeviceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	id := protocol.NewDeviceID(cert.Certificate[0])
	saveQuietIdentityConfig(t, id)
	original := identityStateHashes(t, dir)
	keyBytes, err := os.ReadFile(locations.Get(locations.KeyFile))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(locations.Get(locations.KeyFile)); err != nil {
		t.Fatal(err)
	}
	var service Service
	if err := service.Run(Options{ConfigDir: dir, AllowMigration: true}); err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing original private key was not rejected: %v", err)
	}
	if err := os.WriteFile(locations.Get(locations.KeyFile), keyBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	if !maps.Equal(original, identityStateHashes(t, dir)) {
		t.Fatal("failed identity startup changed saved state")
	}
	runIdentityRecoveryService(t, dir)
	restored, err := loadDeviceIdentity()
	if err != nil || protocol.NewDeviceID(restored.Certificate[0]) != id {
		t.Fatalf("successful engine retry changed original device ID: %v", err)
	}
	actualKey, err := os.ReadFile(locations.Get(locations.KeyFile))
	if err != nil || !bytes.Equal(actualKey, keyBytes) {
		t.Fatalf("successful retry changed original private key: %v", err)
	}
}

func TestEmbeddedLegacyFailureRetainsOriginalConfigArchiveOnRetry(t *testing.T) {
	dir := identityTestDir(t)
	cert, err := loadDeviceIdentity()
	if err != nil {
		t.Fatal(err)
	}
	id := protocol.NewDeviceID(cert.Certificate[0])
	currentConfig := saveQuietIdentityConfig(t, id)
	originalConfig := bytes.Replace(currentConfig, []byte(fmt.Sprintf(`version="%d"`, config.CurrentVersion)), []byte(`version="37"`), 1)
	if bytes.Equal(currentConfig, originalConfig) {
		t.Fatal("fixture did not produce a v37 configuration")
	}
	if err := os.WriteFile(locations.Get(locations.ConfigFile), originalConfig, 0o600); err != nil {
		t.Fatal(err)
	}
	keyBytes, err := os.ReadFile(locations.Get(locations.KeyFile))
	if err != nil {
		t.Fatal(err)
	}
	legacyPath := locations.Get(locations.LegacyDatabase)
	if err := os.Mkdir(legacyPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(legacyPath, "CURRENT"), []byte("invalid legacy database"), 0o600); err != nil {
		t.Fatal(err)
	}
	var service Service
	if err := service.Run(Options{ConfigDir: dir, AllowMigration: true}); !errors.Is(err, syncthing.ErrLegacyDatabase) {
		t.Fatalf("legacy fixture did not fail migration: %v", err)
	}
	archivePath := locations.Get(locations.ConfigFile) + ".v37"
	archive, err := os.ReadFile(archivePath)
	if err != nil || !bytes.Equal(archive, originalConfig) {
		t.Fatalf("original config archive was not retained after migration failure: %v", err)
	}
	if err := os.RemoveAll(legacyPath); err != nil {
		t.Fatal(err)
	}
	legacy, err := leveldb.OpenFile(legacyPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	runIdentityRecoveryService(t, dir)
	archive, err = os.ReadFile(archivePath)
	if err != nil || !bytes.Equal(archive, originalConfig) {
		t.Fatalf("successful retry changed original config archive: %v", err)
	}
	restored, err := loadDeviceIdentity()
	if err != nil || protocol.NewDeviceID(restored.Certificate[0]) != id {
		t.Fatalf("migration retry changed original device ID: %v", err)
	}
	actualKey, err := os.ReadFile(locations.Get(locations.KeyFile))
	if err != nil || !bytes.Equal(actualKey, keyBytes) {
		t.Fatalf("migration retry changed original private key: %v", err)
	}
}
