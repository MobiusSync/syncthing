// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package sqlite

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

// Exercise a real SQLite limit and typed SQLITE_FULL error, not string matching
// or a fabricated driver error. Physical filesystem/device headroom is separate.
func TestRecognizeFullDatabaseThroughWrappedDriverError(t *testing.T) {
	conn, err := sql.Open(dbDriver, filepath.Join(t.TempDir(), "limited.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetMaxOpenConns(1)
	for _, statement := range []string{
		"PRAGMA page_size = 4096",
		"CREATE TABLE data (content BLOB)",
		"PRAGMA max_page_count = 2",
	} {
		if _, err := conn.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	_, err = conn.Exec("INSERT INTO data VALUES (zeroblob(65536))")
	if err == nil {
		t.Fatal("constrained SQLite write unexpectedly succeeded")
	}
	if !IsStorageFull(fmt.Errorf("migrate: %w", err)) {
		t.Fatalf("did not recognize real full-storage error: %T %v", err, err)
	}
	if IsStorageFull(fmt.Errorf("other failure")) {
		t.Fatal("classified unrelated failure as storage")
	}
}
