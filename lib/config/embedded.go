// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package config

import (
	"errors"
	"sync/atomic"
)

var ErrDisabled = errors.New("sync has been disabled")
var ErrExceedsFree = errors.New("sync would exceed free usage allowance")
var externallyDisabled atomic.Bool

func SetExternallyDisabled(disabled bool) { externallyDisabled.Store(disabled) }
func IsExternallyDisabled() bool          { return externallyDisabled.Load() }
