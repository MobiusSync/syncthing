// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

// Package embedded provides in-process startup without CLI parsing, process
// exits, signal handling, desktop monitor processes or automatic upgrades.
package embedded

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/locations"
	"github.com/syncthing/syncthing/lib/svcutil"
	"github.com/syncthing/syncthing/lib/syncthing"
	"github.com/thejerf/suture/v4"
)

var ErrAlreadyRunning = errors.New("embedded Syncthing is already starting or running")
var ErrMigrationRequired = errors.New("open Mobius Sync in the foreground to migrate its sync database")

// The engine's locations and logging are process globals, so startup ownership
// applies across Service instances as well as callers of the same instance.
var runGate sync.Mutex

type Options struct {
	ConfigDir      string
	DataDir        string
	GUIAddress     string
	GUIAssetsDir   string
	AllowMigration bool
}

type Service struct {
	mu         sync.RWMutex
	app        *syncthing.App
	cfg        config.Wrapper
	cancel     context.CancelFunc
	runContext context.Context
	starting   bool
	migrating  bool
}

func (s *Service) Run(options Options) error {
	if !runGate.TryLock() {
		return ErrAlreadyRunning
	}
	defer runGate.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.cancel = cancel
	s.runContext = ctx
	s.starting = true
	s.mu.Unlock()
	defer func() {
		cancel()
		s.mu.Lock()
		s.app = nil
		s.cfg = nil
		s.cancel = nil
		s.runContext = nil
		s.starting = false
		s.migrating = false
		s.mu.Unlock()
	}()
	if options.ConfigDir != "" {
		if err := locations.SetBaseDir(locations.ConfigBaseDir, options.ConfigDir); err != nil {
			return err
		}
		if options.DataDir == "" {
			options.DataDir = options.ConfigDir
		}
	}
	if options.DataDir != "" {
		if err := locations.SetBaseDir(locations.DataBaseDir, options.DataDir); err != nil {
			return err
		}
	}
	for _, dir := range []locations.BaseDirEnum{locations.ConfigBaseDir, locations.DataBaseDir} {
		if err := syncthing.EnsureDir(locations.GetBaseDir(dir), 0700); err != nil {
			return err
		}
	}
	if err := os.Setenv("STGUIADDRESS", options.GUIAddress); err != nil {
		return err
	}
	if options.GUIAssetsDir != "" {
		if err := locations.Set(locations.GUIAssets, options.GUIAssetsDir); err != nil {
			return err
		}
	}
	_, legacyErr := os.Lstat(locations.Get(locations.LegacyDatabase))
	if legacyErr != nil && !os.IsNotExist(legacyErr) {
		return legacyErr
	}
	legacyExists := legacyErr == nil
	// Do this before loading/updating configuration or opening any database.
	if legacyExists && !options.AllowMigration {
		return ErrMigrationRequired
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	cert, err := loadDeviceIdentity()
	if err != nil {
		return fmt.Errorf("load device identity: %w", err)
	}
	// A stop request can arrive while app.Start still needs configuration and
	// events. Keep those dependencies alive until app startup and shutdown have
	// finished; the request context still cancels migration and diagnostics.
	dependencyContext, stopDependencies := context.WithCancel(context.Background())
	early := suture.New("embedded-startup", svcutil.SpecWithDebugLogger())
	earlyDone := early.ServeBackground(dependencyContext)
	defer func() { stopDependencies(); <-earlyDone }()
	logger := events.NewLogger()
	early.Add(logger)
	cfg, err := syncthing.LoadConfigAtStartup(locations.Get(locations.ConfigFile), cert, logger, false, true)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	early.Add(cfg)
	unregisterMetrics := config.RegisterInfoMetrics(cfg)
	defer unregisterMetrics()
	s.mu.Lock()
	s.cfg = cfg
	s.migrating = legacyExists
	s.mu.Unlock()
	// Preserve v1's unlimited delete retention through migration and runtime.
	if err := syncthing.TryMigrateDatabase(ctx, 0); err != nil {
		return fmt.Errorf("migrate database: %w", err)
	}
	s.mu.Lock()
	s.migrating = false
	s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	database, err := syncthing.OpenDatabase(locations.Get(locations.Database), 0)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	if err := ctx.Err(); err != nil {
		database.Close()
		return err
	}
	app, err := syncthing.New(cfg, database, logger, cert, syncthing.Options{NoUpgrade: true, DBMaintenanceInterval: 8 * time.Hour})
	if err != nil {
		database.Close()
		return err
	}
	// Start creates the app's supervisor. Wait also joins cleanup after a failed
	// Start, so a later run never races the previous database/services.
	defer func() { cancel(); app.Stop(svcutil.ExitSuccess); app.Wait() }()
	if err := app.Start(); err != nil {
		return fmt.Errorf("start engine: %w", err)
	}
	s.mu.Lock()
	s.app = app
	s.starting = false
	s.mu.Unlock()
	if ctx.Err() != nil {
		app.Stop(svcutil.ExitSuccess)
	}
	if status := app.Wait(); status == svcutil.ExitError {
		return app.Error()
	}
	return nil
}

func (s *Service) Stop() {
	s.mu.RLock()
	cancel, app := s.cancel, s.app
	s.mu.RUnlock()
	if cancel != nil {
		cancel()
	}
	if app != nil {
		app.Stop(svcutil.ExitSuccess)
	}
}
func (s *Service) IsRunning() bool   { s.mu.RLock(); defer s.mu.RUnlock(); return s.app != nil }
func (s *Service) IsMigrating() bool { s.mu.RLock(); defer s.mu.RUnlock(); return s.migrating }
func (s *Service) IsIdle() bool {
	s.mu.RLock()
	app, cfg, starting := s.app, s.cfg, s.starting
	s.mu.RUnlock()
	if starting {
		return false
	}
	if app == nil {
		return true
	}
	for _, folder := range cfg.Folders() {
		if folder.Paused {
			continue
		}
		state, _, err := app.Internals.FolderState(folder.ID)
		if err != nil || state != "idle" {
			return false
		}
	}
	for _, device := range cfg.Devices() {
		if device.Paused || !app.Internals.IsConnectedTo(device.DeviceID) {
			continue
		}
		completion, err := app.Internals.Completion(device.DeviceID, "")
		if err != nil || completion.CompletionPct < 100 {
			return false
		}
	}
	return true
}

// WriteSupportBundle requires an already running engine. Engine shutdown also
// cancels diagnostics so a performance recording cannot delay background expiry.
func (s *Service) WriteSupportBundle(ctx context.Context, archive *zip.Writer) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.app == nil {
		return errors.New("live engine diagnostics are unavailable; the engine is stopped or starting")
	}
	collectionContext, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.runContext, cancel)
	defer stop()
	defer cancel()
	return s.app.WriteSupportBundle(collectionContext, archive)
}
