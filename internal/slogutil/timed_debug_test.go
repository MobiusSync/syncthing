// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package slogutil

import (
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestTimedPackageLevelsExpireWithoutRestoringManualLevels(t *testing.T) {
	now := time.Unix(1800000000, 0)
	tracker := &levelTracker{levels: make(map[string]slog.Level), descrs: make(map[string]string), now: func() time.Time { return now }}
	tracker.Set("connections", slog.LevelWarn)
	tracker.SetDescr("connections", "Connections")
	tracker.setTimedDebugUntil([]string{"connections", "later"}, now.Unix()+3600)
	if tracker.Get("connections") != slog.LevelDebug || tracker.Get("other") != slog.LevelInfo {
		t.Fatal("timed selection was not applied independently")
	}
	tracker.Set("connections", slog.LevelError)
	tracker.SetDescr("later", "Registered after restoring capture")
	if tracker.Get("later") != slog.LevelDebug || tracker.Get("connections") != slog.LevelDebug {
		t.Fatal("manual changes or late registration broke capture")
	}
	if tracker.Levels()["connections"] != slog.LevelError {
		t.Fatal("timed capture overwrote the manual selection")
	}
	now = now.Add(time.Hour)
	if tracker.Get("connections") != slog.LevelError || tracker.Get("later") != slog.LevelInfo {
		t.Fatal("first lookup at the deadline did not expire the overlay")
	}
	tracker.Set("later", slog.LevelDebug)
	tracker.setTimedDebugUntil(nil, 0)
	if tracker.Get("later") != slog.LevelDebug {
		t.Fatal("stopping capture changed manual debugging")
	}
}

func TestTimedPackageLevelsConcurrentChanges(t *testing.T) {
	tracker := &levelTracker{levels: make(map[string]slog.Level), descrs: make(map[string]string)}
	var group sync.WaitGroup
	for i := 0; i < 4; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := 0; j < 50; j++ {
				tracker.setTimedDebugUntil([]string{"test"}, time.Now().Add(time.Hour).Unix())
				tracker.Get("test")
				tracker.Set("test", slog.LevelWarn)
				tracker.Levels()
				tracker.setTimedDebugUntil(nil, 0)
			}
		}()
	}
	group.Wait()
}
