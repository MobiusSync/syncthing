// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSuppressedEmbeddedMethodsDoNotReachEngine(t *testing.T) {
	for _, tc := range []struct {
		method, path string
		blocked      bool
	}{
		{"POST", "/rest/system/restart", true}, {"POST", "/rest/system/shutdown", true}, {"POST", "/rest/system/upgrade", true},
		{"POST", "/rest/db/scan", false}, {"GET", "/rest/system/ping", false}, {"GET", "/rest/system/log", false},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			called := false
			handler := suppressEmbeddedMethods(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called = true; w.WriteHeader(http.StatusOK) }), "", "/rest/system/restart,/rest/system/shutdown,/rest/system/upgrade")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(tc.method, tc.path+"?test=1", nil))
			if called == tc.blocked {
				t.Fatalf("called=%v, blocked=%v", called, tc.blocked)
			}
			if tc.blocked && response.Code != http.StatusNotFound {
				t.Fatal(response.Code)
			}
		})
	}
}
