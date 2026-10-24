// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package embedded

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"

	"github.com/syncthing/syncthing/lib/syncthing"
)

func TestStartupErrorRecoveryCategoriesPreserveWrappedCauses(t *testing.T) {
	for _, tc := range []struct {
		err  error
		kind string
	}{
		{nil, ""},
		{fmt.Errorf("background: %w", ErrMigrationRequired), "foreground-required"},
		{fmt.Errorf("migration: %w", context.Canceled), "cancelled"},
		{fmt.Errorf("write: %w", syscall.ENOSPC), "storage"},
		{fmt.Errorf("quota: %w", syscall.EDQUOT), "storage"},
		{fmt.Errorf("legacy: %w", syncthing.ErrLegacyDatabase), "legacy"},
		{errors.Join(syncthing.ErrLegacyDatabase, syscall.ENOSPC), "storage"},
		{errors.New("configuration is unreadable"), "other"},
	} {
		if got := StartupErrorKind(tc.err); got != tc.kind {
			t.Errorf("%v: got %q, want %q", tc.err, got, tc.kind)
		}
	}
}
