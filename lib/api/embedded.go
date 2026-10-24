// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package api

import (
	"github.com/syncthing/syncthing/lib/build"
	"net/http"
	"slices"
	"strings"
)

func suppressEmbeddedMethods(next http.Handler, getSpec, postSpec string) http.Handler {
	if build.IsIOS && postSpec == "" {
		postSpec = "/rest/system/restart,/rest/system/shutdown,/rest/system/upgrade"
	}
	getPaths, postPaths := strings.Split(getSpec, ","), strings.Split(postSpec, ",")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.Method == http.MethodGet && slices.Contains(getPaths, r.URL.Path)) ||
			(r.Method == http.MethodPost && slices.Contains(postPaths, r.URL.Path)) {
			http.NotFound(w, r)
			return
		}
		next.ServeHTTP(w, r)
	})
}
