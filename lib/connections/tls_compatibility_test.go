// Copyright (C) 2026 The Syncthing Authors.
// SPDX-License-Identifier: MPL-2.0
package connections

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/tlsutil"
)

// Exercise a handshake, rather than merely inspecting a MinVersion constant.
// Old peers restricted to TLS 1.2 must remain rejected by the v2 sync policy.
func TestDataSafetyTLSCompatibilityBoundary(t *testing.T) {
	cert, err := tlsutil.NewCertificateInMemory("issue21-disposable", 1)
	if err != nil {
		t.Fatal(err)
	}
	identity := protocol.NewDeviceID(cert.Certificate[0])
	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		t.Run(tls.VersionName(version), func(t *testing.T) {
			left, right := net.Pipe()
			defer left.Close()
			defer right.Close()
			deadline := time.Now().Add(5 * time.Second)
			left.SetDeadline(deadline)
			right.SetDeadline(deadline)
			serverCfg := tlsutil.SecureDefaultTLS13()
			serverCfg.Certificates = []tls.Certificate{cert}
			serverCfg.ClientAuth = tls.RequireAnyClientCert
			clientCfg := &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true, MinVersion: version, MaxVersion: version}
			server, client := tls.Server(left, serverCfg), tls.Client(right, clientCfg)
			ctx, cancel := context.WithDeadline(t.Context(), deadline)
			defer cancel()
			serverDone := make(chan error, 1)
			go func() { serverDone <- server.HandshakeContext(ctx) }()
			clientErr := client.HandshakeContext(ctx)
			serverErr := <-serverDone
			if version == tls.VersionTLS12 {
				if clientErr == nil || serverErr == nil {
					t.Fatalf("TLS1.2 unexpectedly accepted: client=%v server=%v", clientErr, serverErr)
				}
				return
			}
			if clientErr != nil || serverErr != nil {
				t.Fatalf("TLS1.3 handshake: client=%v server=%v", clientErr, serverErr)
			}
			for _, connection := range []*tls.Conn{server, client} {
				state := connection.ConnectionState()
				if state.Version != tls.VersionTLS13 || len(state.PeerCertificates) != 1 {
					t.Fatalf("unexpected negotiated state: %+v", state)
				}
				if got := protocol.NewDeviceID(state.PeerCertificates[0].Raw); got != identity {
					t.Fatalf("connection identity changed: %s != %s", got, identity)
				}
			}
		})
	}
}
