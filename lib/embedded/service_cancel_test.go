// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package embedded

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/syncthing"
)

// The synchronous log call runs after App.Start has created its supervisor,
// before startup modifies configuration. Blocking it reproduces a stop request
// during real engine startup without changing the production startup API.
type startupCancelGate struct {
	once    sync.Once
	reached chan struct{}
	release chan struct{}
}

type startupCancelHandler struct {
	slog.Handler
	gate *startupCancelGate
}

func (h *startupCancelHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "Calculated our device ID" {
		h.gate.once.Do(func() {
			close(h.gate.reached)
			<-h.gate.release
		})
	}
	return h.Handler.Handle(ctx, record)
}

func (h *startupCancelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &startupCancelHandler{Handler: h.Handler.WithAttrs(attrs), gate: h.gate}
}

func (h *startupCancelHandler) WithGroup(name string) slog.Handler {
	return &startupCancelHandler{Handler: h.Handler.WithGroup(name), gate: h.gate}
}

func TestStopDuringStartupPreservesDependenciesAndAllowsRestart(t *testing.T) {
	dir := t.TempDir()
	cert, err := syncthing.LoadOrGenerateCertificate(filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"))
	if err != nil {
		t.Fatal(err)
	}
	id := protocol.NewDeviceID(cert.Certificate[0])
	cfg := config.New(id)
	cfg.GUI.Enabled = false
	// An explicit empty listener element survives saving/loading without
	// restoring the default TCP/QUIC listeners.
	cfg.Options.RawListenAddresses = []string{""}
	cfg.Options.GlobalAnnEnabled, cfg.Options.LocalAnnEnabled = false, false
	cfg.Options.NATEnabled, cfg.Options.RelaysEnabled = false, false
	cfg.Options.StartBrowser, cfg.Options.SetLowPriority = false, false
	cfg.Options.AutoUpgradeIntervalH, cfg.Options.URAccepted = 0, -1
	cfg.Options.CREnabled = false
	configPath := filepath.Join(dir, "config.xml")
	if err := config.Wrap(configPath, cfg, id, events.NoopLogger).Save(); err != nil {
		t.Fatal(err)
	}
	loaded, _, err := config.Load(configPath, id, events.NoopLogger)
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range loaded.Options().ListenAddresses() {
		if address != "" {
			t.Fatalf("test configuration enabled a listener: %q", address)
		}
	}
	originalKey, err := os.ReadFile(filepath.Join(dir, "key.pem"))
	if err != nil {
		t.Fatal(err)
	}

	gate := &startupCancelGate{reached: make(chan struct{}), release: make(chan struct{})}
	originalLogger := slog.Default()
	slog.SetDefault(slog.New(&startupCancelHandler{
		Handler: slog.NewTextHandler(io.Discard, nil),
		gate:    gate,
	}))
	defer slog.SetDefault(originalLogger)
	var releaseOnce sync.Once
	releaseStartup := func() { releaseOnce.Do(func() { close(gate.release) }) }
	defer releaseStartup()

	var service Service
	result := make(chan error, 1)
	go func() { result <- service.Run(Options{ConfigDir: dir, AllowMigration: true}) }()
	finished := false
	defer func() {
		releaseStartup()
		service.Stop()
		if !finished {
			select {
			case <-result:
			case <-time.After(3 * time.Second):
				t.Error("cancelled startup did not finish during cleanup")
			}
		}
	}()
	select {
	case <-gate.reached:
	case err := <-result:
		finished = true
		t.Fatalf("engine returned before reaching startup gate: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("engine did not reach startup gate")
	}
	if service.IsRunning() || service.IsIdle() {
		t.Fatal("blocked startup was reported as running or idle")
	}
	service.mu.RLock()
	startingConfig, requestContext := service.cfg, service.runContext
	service.mu.RUnlock()

	stopped := make(chan struct{})
	go func() { service.Stop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("Stop blocked while engine startup was paused")
	}
	select {
	case <-requestContext.Done():
	default:
		t.Fatal("Stop did not cancel the startup request")
	}

	// Cancellation must not stop the configuration service out from under
	// App.Start. Repeated handshakes yield to its Serve loop, covering both
	// a queued modification and the loop's following cancellation check.
	for i := 0; i < 16; i++ {
		runtime.Gosched()
		modified := make(chan error, 1)
		go func() {
			_, err := startingConfig.Modify(func(*config.Configuration) {})
			modified <- err
		}()
		select {
		case err := <-modified:
			if err != nil {
				t.Fatal("configuration unavailable during cancelled startup", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Stop terminated configuration service before App.Start finished")
		}
	}
	releaseStartup()
	select {
	case err := <-result:
		finished = true
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal("cancelled engine returned an unexpected error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("engine startup did not finish after Stop")
	}
	if service.IsRunning() || service.IsMigrating() || !service.IsIdle() {
		t.Fatal("cancelled startup did not clear service state")
	}

	// Reusing the same Service proves that startup released its process-wide
	// gate, database handles and metrics, rather than merely returning Stop.
	finished = false
	go func() { result <- service.Run(Options{ConfigDir: dir, AllowMigration: true}) }()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !service.IsRunning() {
		select {
		case err := <-result:
			finished = true
			t.Fatalf("restart returned before becoming ready: %v", err)
		case <-deadline.C:
			t.Fatal("engine did not restart after cancelled startup")
		case <-ticker.C:
		}
	}

	// Simultaneous stop callers must all join the same shutdown.
	var stops sync.WaitGroup
	for i := 0; i < 8; i++ {
		stops.Go(service.Stop)
	}
	allStopped := make(chan struct{})
	go func() { stops.Wait(); close(allStopped) }()
	select {
	case <-allStopped:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Stop calls did not finish")
	}
	select {
	case err := <-result:
		finished = true
		if err != nil {
			t.Fatal("restarted engine did not stop cleanly", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("restarted engine did not release Run")
	}
	if service.IsRunning() || !service.IsIdle() {
		t.Fatal("restarted engine did not clear readiness")
	}
	key, err := os.ReadFile(filepath.Join(dir, "key.pem"))
	if err != nil || !bytes.Equal(originalKey, key) {
		t.Fatal("cancelled startup or restart changed device identity", err)
	}
}
