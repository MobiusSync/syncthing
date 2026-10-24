// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package embedded

import (
	"context"
	"errors"
	"syscall"

	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/syncthing"
)

// StartupErrorKind supplies stable recovery categories to the embedding while
// retaining the original wrapped error for diagnostics. It does not decide
// whether to retry or change any database, configuration or identity files.
func StartupErrorKind(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrMigrationRequired):
		return "foreground-required"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, syscall.ENOSPC), errors.Is(err, syscall.EDQUOT), sqlite.IsStorageFull(err):
		return "storage"
	case errors.Is(err, syncthing.ErrLegacyDatabase):
		return "legacy"
	default:
		return "other"
	}
}
