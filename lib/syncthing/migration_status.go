// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package syncthing

import (
	"strings"
	"time"

	"github.com/syncthing/syncthing/internal/slogutil"
)

// MigrationStatusPage is the temporary migration page used by the CLI. An
// embedded app can display the same plain-text snapshot without starting a
// second HTTP listener before its normal GUI is available.
func MigrationStatusPage() string {
	var page strings.Builder
	page.WriteString("*** Database migration in progress ***\n\n")
	for _, line := range slogutil.GlobalRecorder.Since(time.Time{}) {
		_, _ = line.WriteTo(&page, slogutil.DefaultLineFormat)
	}
	return page.String()
}
