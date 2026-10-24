// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build cgo

package sqlite

import (
	"errors"

	"github.com/mattn/go-sqlite3" // register sqlite3 database driver
)

const (
	dbDriver      = "sqlite3"
	commonOptions = "_fk=true&_rt=true&_sync=1&_txlock=immediate"
)

// IsStorageFull recognizes SQLite's typed full-disk error through wrapping.
func IsStorageFull(err error) bool {
	var sqlErr sqlite3.Error
	return errors.As(err, &sqlErr) && sqlErr.Code == sqlite3.ErrFull
}
