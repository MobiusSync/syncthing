// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package toplevel

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"
)

func init() {
	if innerProcess && os.Getenv("STBLOCKPROFILE") != "" {
		profiler := pprof.Lookup("block")
		if profiler == nil {
			panic("Couldn't find block profiler")
		}
		l.Debugln("Starting block profiling")
		go func() {
			err := saveBlockingProfiles(profiler) // Only returns on error
			l.Warnln("Block profiler failed:", err)
			panic("Block profiler failed")
		}()
	}
}

func saveBlockingProfiles(profiler *pprof.Profile) error {
	runtime.SetBlockProfileRate(1)

	t0 := time.Now()
	for t := range time.NewTicker(20 * time.Second).C {
		startms := int(t.Sub(t0).Seconds() * 1000)

		fd, err := os.Create(fmt.Sprintf("block-%05d-%07d.pprof", syscall.Getpid(), startms))
		if err != nil {
			return err
		}
		err = profiler.WriteTo(fd, 0)
		if err != nil {
			return err
		}
		err = fd.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
