// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0

package api

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/syncthing/syncthing/lib/build"
	"github.com/syncthing/syncthing/lib/locations"
	"github.com/syncthing/syncthing/lib/ur"
)

const supportProfilingDuration = 4 * time.Second

var supportProfileLock sync.Mutex

// supportCollection writes each diagnostic directly to the caller's archive.
// It does not keep all sections or a second ZIP in memory.
type supportCollection struct {
	ctx     context.Context
	archive *zip.Writer
	err     error
	notes   []string
}

type supportEntryWriter struct {
	ctx    context.Context
	writer io.Writer
	err    error
}

func (w *supportEntryWriter) Write(data []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if err := w.ctx.Err(); err != nil {
		w.err = err
		return 0, err
	}
	n, err := w.writer.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}
func (c *supportCollection) write(name string, write func(io.Writer) error) {
	if c.err != nil {
		return
	}
	if err := c.ctx.Err(); err != nil {
		c.err = err
		return
	}
	entry, err := c.archive.Create(name)
	if err != nil {
		c.err = err
		return
	}
	writer := &supportEntryWriter{ctx: c.ctx, writer: entry}
	c.err = write(writer)
	if c.err == nil {
		c.err = writer.err
	}
}
func (c *supportCollection) json(name string, value any) {
	c.write(name, func(w io.Writer) error {
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "  ")
		return encoder.Encode(value)
	})
}
func (c *supportCollection) file(name, path string) {
	if c.err != nil {
		return
	}
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		c.notes = append(c.notes, name+": could not read this file.")
		return
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() {
		return
	}
	c.write(name, func(w io.Writer) error { _, err := io.Copy(w, file); return err })
}

// WriteSupportBundle writes Syncthing's standard diagnostic contents to an open
// ZIP. The caller owns closing and storing it. No inclusion options, additional
// credential filtering, upload or configuration-directory backup are applied.
func (s *service) WriteSupportBundle(ctx context.Context, archive *zip.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c := supportCollection{ctx: ctx, archive: archive}
	c.json("config.json.txt", getRedactedConfig(s))
	c.write("log-inmemory.txt", func(w io.Writer) error {
		for _, line := range s.systemLog.Since(time.Time{}) {
			if _, err := fmt.Fprintf(w, "%s: %s\n", line.When.Format(time.RFC3339), line.Message); err != nil {
				return err
			}
		}
		return nil
	})
	if errs := s.guiErrors.Since(time.Time{}); len(errs) > 0 {
		c.json("errors.json.txt", errs)
	}
	paths, err := filepath.Glob(filepath.Join(locations.GetBaseDir(locations.ConfigBaseDir), "panic*"))
	if err == nil {
		for _, path := range paths {
			c.file(filepath.Base(path), path)
		}
	}
	c.file("log-ondisk.txt", locations.Get(locations.LogFile))
	c.json("version-platform.json.txt", map[string]string{
		"now": time.Now().Format(time.RFC3339), "version": build.Version,
		"codename": build.Codename, "longVersion": build.LongVersion,
		"os": runtime.GOOS, "arch": runtime.GOARCH,
	})
	if c.err != nil {
		return c.err
	}
	report, err := s.urService.ReportDataPreview(ctx, ur.Version)
	if err == nil {
		c.json("usage-reporting.json.txt", report)
	} else {
		c.notes = append(c.notes, "usage-reporting.json.txt: unavailable.")
	}
	c.write("metrics.txt", func(w io.Writer) error {
		request, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/metrics", nil)
		promhttp.Handler().ServeHTTP(bufferedResponseWriter{Writer: w}, request)
		return nil
	})
	if c.err != nil {
		return c.err
	}
	c.json("connection-stats.json.txt", s.model.ConnectionStats())
	c.profiles()
	if len(c.notes) > 0 {
		c.write("collection-notes.txt", func(w io.Writer) error { _, err := io.WriteString(w, strings.Join(c.notes, "\n")+"\n"); return err })
	}
	if c.err != nil {
		return c.err
	}
	return ctx.Err()
}

func (c *supportCollection) profileName(kind string) string {
	return fmt.Sprintf("syncthing-%s-%s-%s-%s-%s.pprof", kind, runtime.GOOS, runtime.GOARCH, build.Version, time.Now().Format("150405"))
}
func (c *supportCollection) profiles() {
	if c.err != nil {
		return
	}
	if p := pprof.Lookup("goroutine"); p != nil {
		c.write(c.profileName("goroutines"), func(w io.Writer) error { return p.WriteTo(w, 0) })
	}
	c.write(c.profileName("heap"), func(w io.Writer) error { runtime.GC(); return pprof.WriteHeapProfile(w) })
	if c.err != nil {
		return
	}
	if !supportProfileLock.TryLock() {
		c.notes = append(c.notes, "CPU/block recording unavailable: another support recording is running.")
		return
	}
	defer supportProfileLock.Unlock()
	c.write(c.profileName("cpu"), func(w io.Writer) error {
		if err := pprof.StartCPUProfile(w); err != nil {
			c.notes = append(c.notes, "CPU/block recording unavailable: another CPU recording may be running.")
			return nil
		}
		runtime.SetBlockProfileRate(1)
		defer runtime.SetBlockProfileRate(0)
		timer := time.NewTimer(supportProfilingDuration)
		select {
		case <-c.ctx.Done():
		case <-timer.C:
		}
		timer.Stop()
		pprof.StopCPUProfile()
		if c.ctx.Err() != nil {
			return c.ctx.Err()
		}
		return nil
	})
	if c.err == nil {
		if p := pprof.Lookup("block"); p != nil {
			c.write(c.profileName("block"), func(w io.Writer) error { return p.WriteTo(w, 0) })
		}
	}
}
