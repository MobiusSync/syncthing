// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package embedded

import (
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/syncthing/syncthing/lib/locations"
	"github.com/syncthing/syncthing/lib/syncthing"
)

// A failed load of an established identity must never silently create a new
// device. Generate only when both files and all previous installation state are
// absent. Lstat also treats dangling symlinks as existing files to preserve.
func loadDeviceIdentity() (tls.Certificate, error) {
	certFile, keyFile := locations.Get(locations.CertFile), locations.Get(locations.KeyFile)
	cert, loadErr := tls.LoadX509KeyPair(certFile, keyFile)
	if loadErr == nil {
		return cert, nil
	}
	for _, path := range []string{certFile, keyFile} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			return tls.Certificate{}, loadErr
		}
	}
	statePaths := []string{
		locations.Get(locations.ConfigFile),
		locations.Get(locations.LegacyDatabase),
		locations.Get(locations.LegacyDatabase) + "-migrated",
		locations.Get(locations.Database),
	}
	archives, err := filepath.Glob(locations.Get(locations.ConfigFile) + ".v*")
	if err != nil {
		return tls.Certificate{}, errors.Join(loadErr, err)
	}
	statePaths = append(statePaths, archives...)
	for _, path := range statePaths {
		if _, err := os.Lstat(path); err == nil {
			return tls.Certificate{}, fmt.Errorf("existing Syncthing state requires its original certificate and private key: %w", loadErr)
		} else if !os.IsNotExist(err) {
			return tls.Certificate{}, errors.Join(loadErr, fmt.Errorf("inspect previous installation state: %w", err))
		}
	}
	return syncthing.GenerateCertificate(certFile, keyFile)
}
