// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package model

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	protocolmocks "github.com/syncthing/syncthing/lib/protocol/mocks"
)

// Two real models, SQLite indexes, on-disk folders and actual BEP over a pipe.
// Only provider responses are fake. Never uses installed app data or networking.
type safetyProvider struct {
	fs.Filesystem
	fault atomic.Value
}

func (p *safetyProvider) mode() string {
	if v := p.fault.Load(); v != nil {
		return v.(string)
	}
	return ""
}
func (p *safetyProvider) DirNames(path string) ([]string, error) {
	mode := p.mode()
	if mode == "revoked" || mode == "root-list" && path == "." || mode == "subtree-list" && path == "nested" || mode == "late-enumeration" && path == "zz-last" {
		return nil, os.ErrPermission
	}
	if mode == "empty-root" && path == "." {
		return nil, nil
	}
	names, err := p.Filesystem.DirNames(path)
	if path == "." {
		if mode == "missing-marker" {
			names = slices.DeleteFunc(names, func(n string) bool { return n == ".stfolder" })
		}
		if mode == "silent-partial" {
			names = slices.DeleteFunc(names, func(n string) bool { return n == "kept.txt" })
		}
	}
	if mode == "silent-empty-subtree" && path == "nested" {
		return nil, nil
	}
	return names, err
}
func (p *safetyProvider) Lstat(path string) (fs.FileInfo, error) {
	mode := p.mode()
	if mode == "revoked" || mode == "root-stat" && path == "." || mode == "file-stat" && path == "kept.txt" || mode == "marker-stat" && path == ".stfolder" {
		return nil, os.ErrPermission
	}
	if mode == "silent-partial" && path == "kept.txt" || mode == "silent-empty-subtree" && path == "nested/kept.txt" {
		return nil, fs.ErrNotExist
	}
	return p.Filesystem.Lstat(path)
}
func newSafetyPeer(t *testing.T, id, remote protocol.DeviceID) (*testModel, *sendReceiveFolder, string) {
	t.Helper()
	root := t.TempDir()
	cfg := config.New(id)
	cfg.Options.MinHomeDiskFree.Value = 0
	cfg.Folders = nil
	dev := cfg.Defaults.Device.Copy()
	dev.DeviceID = remote
	dev.RawNumConnections = 1
	cfg.SetDevice(dev)
	f := cfg.Defaults.Folder.Copy()
	f.ID = "provider-safety"
	f.Path = root
	f.FilesystemType = config.FilesystemTypeBasic
	f.Type = config.FolderTypeSendReceive
	f.FSWatcherEnabled = false
	f.RescanIntervalS = 0
	f.PullerDelayS = 0
	f.MinDiskFree.Value = 0
	f.IgnorePerms = true
	f.Devices = []config.FolderDeviceConfiguration{{DeviceID: id}, {DeviceID: remote}}
	cfg.SetFolder(f)
	w := config.Wrap("", cfg, id, events.NoopLogger)
	ctx, cancel := context.WithCancel(t.Context())
	go w.Serve(ctx)
	t.Cleanup(cancel)
	m := newModel(t, w, id, nil)
	m.ServeBackground()
	t.Cleanup(func() { cleanupModel(m) })
	must(t, m.ScanFolder(f.ID))
	r, ok := m.folderRunners.Get(f.ID)
	if !ok {
		t.Fatal("folder not started")
	}
	return m, r.(*sendReceiveFolder), root
}
func connectSafetyPeers(t *testing.T, a, b *testModel) {
	t.Helper()
	left, right := net.Pipe()
	info := func(id string, c net.Conn) *protocolmocks.ConnectionInfo {
		ci := &protocolmocks.ConnectionInfo{}
		ci.ConnectionIDReturns(id)
		ci.RemoteAddrReturns(c.RemoteAddr())
		ci.TransportReturns("pipe")
		ci.TypeReturns("provider-safety")
		return ci
	}
	ca := protocol.NewConnection(b.id, left, left, left, a.model, info("provider-a", left), protocol.CompressionNever, a.keyGen)
	cb := protocol.NewConnection(a.id, right, right, right, b.model, info("provider-b", right), protocol.CompressionNever, b.keyGen)
	t.Cleanup(func() {
		ca.Close(io.EOF)
		cb.Close(io.EOF)
		for _, c := range []protocol.Connection{ca, cb} {
			select {
			case <-c.Closed():
			case <-time.After(10 * time.Second):
				t.Error("BEP did not close")
			}
		}
	})
	a.AddConnection(ca, protocol.Hello{})
	b.AddConnection(cb, protocol.Hello{})
	a.promoteConnections()
	b.promoteConnections()
}
func awaitSafety(t *testing.T, description string, check func() error) {
	t.Helper()
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	poll := time.NewTicker(20 * time.Millisecond)
	defer poll.Stop()
	for {
		err := check()
		if err == nil {
			return
		}
		select {
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		case <-timer.C:
			t.Fatalf("%s timed out: %v", description, err)
		case <-poll.C:
		}
	}
}
func safetyFileState(m *testModel, name string, deleted bool) error {
	fi, ok, err := m.sdb.GetDeviceFile("provider-safety", protocol.LocalDeviceID, name)
	if err != nil {
		return err
	}
	if !ok || fi.Deleted != deleted {
		return fmt.Errorf("%s on %s: present=%v deleted=%v want=%v version=%v", name, m.id, ok, fi.Deleted, deleted, fi.Version)
	}
	return nil
}
func awaitSafetyContents(t *testing.T, peers []*testModel, roots []string, files map[string]string, deleted ...string) {
	t.Helper()
	awaitSafety(t, "both peer contents, versions and deletion records", func() error {
		for i, m := range peers {
			for name, want := range files {
				got, err := os.ReadFile(filepath.Join(roots[i], name))
				if err != nil {
					return err
				}
				if !bytes.Equal(got, []byte(want)) {
					return fmt.Errorf("peer %d %s wrong content", i, name)
				}
				if err := safetyFileState(m, name, false); err != nil {
					return err
				}
			}
			for _, name := range deleted {
				if _, err := os.Stat(filepath.Join(roots[i], name)); !os.IsNotExist(err) {
					return fmt.Errorf("peer %d %s still exists: %v", i, name, err)
				}
				if err := safetyFileState(m, name, true); err != nil {
					return err
				}
			}
		}
		for name := range files {
			a, _, err := peers[0].sdb.GetDeviceFile("provider-safety", protocol.LocalDeviceID, name)
			if err != nil {
				return err
			}
			b, _, err := peers[1].sdb.GetDeviceFile("provider-safety", protocol.LocalDeviceID, name)
			if err != nil {
				return err
			}
			if !a.Version.Equal(b.Version) {
				return fmt.Errorf("%s versions differ: %v / %v", name, a.Version, b.Version)
			}
		}
		return nil
	})
}
func newSafetyFixture(t *testing.T) (*testModel, *testModel, []string, *safetyProvider, map[string]string) {
	t.Helper()
	source, folder, a := newSafetyPeer(t, myID, device1)
	receiver, _, b := newSafetyPeer(t, device1, myID)
	files := map[string]string{"kept.txt": "keep original bytes", "nested/kept.txt": "nested bytes", "zz-last/kept.txt": "last subtree", "pending-delete.txt": "retain until healthy scan", "empty.txt": "", "日本語.txt": "Unicode"}
	for name, data := range files {
		must(t, os.MkdirAll(filepath.Dir(filepath.Join(a, name)), 0700))
		must(t, os.WriteFile(filepath.Join(a, name), []byte(data), 0600))
	}
	provider := &safetyProvider{Filesystem: folder.mtimefs}
	must(t, folder.doInSync(func(context.Context) error {
		folder.mtimefs = fs.NewScanValidationFilesystem(provider, folder.MarkerName)
		return nil
	}))
	must(t, source.ScanFolder("provider-safety"))
	connectSafetyPeers(t, source, receiver)
	roots := []string{a, b}
	awaitSafetyContents(t, []*testModel{source, receiver}, roots, files)
	return source, receiver, roots, provider, files
}
func TestExternalProviderFailuresDoNotDeletePeerData(t *testing.T) {
	for _, mode := range []string{"root-stat", "root-list", "empty-root", "missing-marker", "marker-stat", "file-stat", "subtree-list", "late-enumeration", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			source, receiver, roots, provider, files := newSafetyFixture(t)
			before := make(map[string]protocol.FileInfo)
			for name := range files {
				fi, _, err := source.sdb.GetDeviceFile("provider-safety", protocol.LocalDeviceID, name)
				must(t, err)
				before[name] = fi
			}
			// A real absence must be withheld until enumeration succeeds, even after retries.
			must(t, os.Remove(filepath.Join(roots[0], "pending-delete.txt")))
			provider.fault.Store(mode)
			for range 2 {
				err := source.ScanFolder("provider-safety")
				if !errors.Is(err, fs.ErrIncompleteScan) {
					t.Fatalf("%s: want incomplete scan, got %v", mode, err)
				}
				for name, original := range before {
					fi, ok, err := source.sdb.GetDeviceFile("provider-safety", protocol.LocalDeviceID, name)
					must(t, err)
					if !ok || fi.Deleted || !fi.Version.Equal(original.Version) {
						t.Fatalf("failed scan changed %s: before=%+v after=%+v", name, original, fi)
					}
					if err := safetyFileState(receiver, name, false); err != nil {
						t.Fatal(err)
					}
					got, err := os.ReadFile(filepath.Join(roots[1], name))
					must(t, err)
					if !bytes.Equal(got, []byte(files[name])) {
						t.Fatalf("peer bytes changed: %s", name)
					}
				}
			}
			provider.fault.Store("")
			must(t, source.ScanFolder("provider-safety"))
			delete(files, "pending-delete.txt")
			awaitSafetyContents(t, []*testModel{source, receiver}, roots, files, "pending-delete.txt")
			t.Logf("%s: two failed scans retained index and peer bytes; recovery propagated only intentional deletion", mode)
		})
	}
}
func TestExternalProviderSilentOmissionPropagatesDeletionLimitation(t *testing.T) {
	for _, mode := range []string{"silent-partial", "silent-empty-subtree"} {
		t.Run(mode, func(t *testing.T) {
			source, receiver, roots, provider, files := newSafetyFixture(t)
			missing := "kept.txt"
			if mode == "silent-empty-subtree" {
				missing = "nested/kept.txt"
			}
			provider.fault.Store(mode)
			must(t, source.ScanFolder("provider-safety"))
			awaitSafety(t, "documented silent omission propagates deletion", func() error {
				if err := safetyFileState(source, missing, true); err != nil {
					return err
				}
				if err := safetyFileState(receiver, missing, true); err != nil {
					return err
				}
				if _, err := os.Stat(filepath.Join(roots[1], missing)); !os.IsNotExist(err) {
					return fmt.Errorf("peer file still exists: %v", err)
				}
				return nil
			})
			// This demonstrates a limitation, never claims detection of successful lies.
			got, err := os.ReadFile(filepath.Join(roots[0], missing))
			must(t, err)
			if !bytes.Equal(got, []byte(files[missing])) {
				t.Fatal("source fixture changed")
			}
			provider.fault.Store("")
			must(t, source.ScanFolder("provider-safety"))
			awaitSafetyContents(t, []*testModel{source, receiver}, roots, files)
			t.Logf("LIMITATION %s: silent incomplete listing plus false not-exist deleted peer bytes; healthy rescan restored them", mode)
		})
	}
}
