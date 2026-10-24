// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package slogutil

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

var embeddedHandlers struct {
	sync.RWMutex
	next     uint64
	handlers map[uint64]func(Line)
}

// AddLineHandler observes the same formatted, level-filtered lines as the GUI.
// Handlers run without the subscription lock and must not recursively log.
func AddLineHandler(handler func(Line)) func() {
	embeddedHandlers.Lock()
	if embeddedHandlers.handlers == nil {
		embeddedHandlers.handlers = make(map[uint64]func(Line))
	}
	embeddedHandlers.next++
	id := embeddedHandlers.next
	embeddedHandlers.handlers[id] = handler
	embeddedHandlers.Unlock()
	return func() { embeddedHandlers.Lock(); delete(embeddedHandlers.handlers, id); embeddedHandlers.Unlock() }
}

func notifyLineHandlers(line Line) {
	embeddedHandlers.RLock()
	handlers := make([]func(Line), 0, len(embeddedHandlers.handlers))
	for _, handler := range embeddedHandlers.handlers {
		handlers = append(handlers, handler)
	}
	embeddedHandlers.RUnlock()
	for _, handler := range handlers {
		handler(line)
	}
}

func RegisterNamedPackage(name, description string) { globalLevels.SetDescr(name, description) }
func NamedPackageLevel(name string) slog.Level      { return globalLevels.Get(name) }

// LogNamed gives an embedded adapter a package identity independent of its
// caller's source location. A timed debug request may bypass that package's
// manual level without changing it; accepted lines still use the normal GUI,
// recorder, output and persistence pipeline.
func LogNamed(name string, level slog.Level, message string, forceDebug bool) {
	if !forceDebug && globalLevels.Get(name) > level {
		return
	}
	rec := slog.NewRecord(time.Now(), level, message, 0)
	rec.AddAttrs(slog.Group("log", slog.String("pkg", name)))
	_ = slogDef.Handler().Handle(context.Background(), rec)
}
