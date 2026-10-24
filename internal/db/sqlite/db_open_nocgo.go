// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build !cgo && !wazero

package sqlite

import (
	"errors"

	"github.com/syncthing/syncthing/lib/build"
	"modernc.org/sqlite"
	"modernc.org/sqlite/lib" // register sqlite database driver
)

const (
	dbDriver      = "sqlite"
	commonOptions = "_pragma=foreign_keys(1)&_pragma=recursive_triggers(1)&_pragma=synchronous(1)&_txlock=immediate"
)

func init() {
	build.AddTag("modernc-sqlite")
}

// IsStorageFull recognizes SQLite's typed full-disk error through wrapping.
func IsStorageFull(err error) bool {
	var sqlErr *sqlite.Error
	return errors.As(err, &sqlErr) && sqlErr.Code()&0xff == sqlite3.SQLITE_FULL
}
