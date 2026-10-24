// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

// Package logging isolates the mobile bridge from Syncthing's internal log
// implementation. Every message and observer uses v2's structured-log pipeline.
package logging

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/syncthing/syncthing/internal/slogutil"
)

type LogLevel = slog.Level

const (
	LevelDebug = slog.LevelDebug
	LevelInfo  = slog.LevelInfo
	LevelWarn  = slog.LevelWarn
	LevelError = slog.LevelError
)

type Logger struct {
	name       string
	forceDebug bool
}

var DefaultLogger = &Logger{name: "MobiusSync", forceDebug: true}

func (*Logger) NewFacility(name, description string) *Logger {
	slogutil.RegisterNamedPackage(name, description)
	return &Logger{name: name}
}
func (*Logger) SetDebug(name string, enabled bool) {
	level := slog.LevelInfo
	if enabled {
		level = slog.LevelDebug
	}
	slogutil.SetPackageLevel(name, level)
}
func (*Logger) ShouldDebug(name string) bool {
	return slogutil.NamedPackageLevel(name) <= slog.LevelDebug
}

func (*Logger) Facilities() map[string]string { return slogutil.PackageDescrs() }
func (*Logger) ManualLevels() map[string]string {
	levels := make(map[string]string)
	for name, level := range slogutil.PackageLevels() {
		levels[name] = level.String()
	}
	return levels
}
func (*Logger) FacilityDebugging() []string {
	var selected []string
	for name, level := range slogutil.PackageLevels() {
		if level <= slog.LevelDebug {
			selected = append(selected, name)
		}
	}
	return selected
}
func SetTimedDebugUntil(packages []string, unixSeconds int64) {
	slogutil.SetTimedDebugUntil(packages, unixSeconds)
}
func (*Logger) AddHandler(level LogLevel, handler func(LogLevel, string)) func() {
	return slogutil.AddLineHandler(func(line slogutil.Line) {
		if line.Level >= level {
			handler(line.Level, line.Message)
		}
	})
}
func (l *Logger) Debugf(format string, values ...any) {
	slogutil.LogNamed(l.name, LevelDebug, fmt.Sprintf(format, values...), l.forceDebug)
}
func (l *Logger) Debugln(values ...any) { l.Debugf("%s", strings.TrimSpace(fmt.Sprintln(values...))) }
func (l *Logger) Infof(format string, values ...any) {
	slogutil.LogNamed(l.name, LevelInfo, fmt.Sprintf(format, values...), false)
}
func (l *Logger) Infoln(values ...any) { l.Infof("%s", strings.TrimSpace(fmt.Sprintln(values...))) }
func (l *Logger) Warnln(values ...any) {
	slogutil.LogNamed(l.name, LevelWarn, strings.TrimSpace(fmt.Sprintln(values...)), false)
}
func (l *Logger) Errorln(values ...any) {
	slogutil.LogNamed(l.name, LevelError, strings.TrimSpace(fmt.Sprintln(values...)), false)
}

type Line = slogutil.Line

// Recorder reads v2's own GUI recorder, bounded further for the bridge consumer.
type Recorder struct {
	created time.Time
	minimum LogLevel
	maximum int
}

func NewRecorder(_ *Logger, minimum LogLevel, maximum, _ int) *Recorder {
	return &Recorder{created: time.Now(), minimum: minimum, maximum: maximum}
}
func (r *Recorder) Since(since time.Time) []Line {
	if since.Before(r.created) {
		since = r.created
	}
	var lines []Line
	for _, line := range slogutil.GlobalRecorder.Since(since) {
		if line.Level >= r.minimum {
			lines = append(lines, line)
		}
	}
	if r.maximum > 0 && len(lines) > r.maximum {
		lines = lines[len(lines)-r.maximum:]
	}
	return lines
}
