// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package matrix

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
)

// Review finding: our whatsapp.yaml (prod template and load test) lists the bridge's own server in
// double_puppet.servers with the same URL as homeserver.address, and every user has automatic double puppeting
// (as_token: secret). The double puppets must still go through the appservice's capped connection pools.
func TestDoublePuppet_OwnServerInServersUsesAppServicePools(t *testing.T) {
	const hsURL = "http://10.231.0.10:8008"
	as := appservice.Create()
	as.HomeserverDomain = "example.com"
	as.Registration = &appservice.Registration{AppToken: "as_token", SenderLocalpart: "bot"}
	require.NoError(t, as.SetHomeserverURL(hsURL))

	for _, tc := range []struct {
		name    string
		servers map[string]string
	}{
		{"own server listed (our whatsapp.yaml)", map[string]string{"example.com": hsURL}},
		{"own server not listed (upstream example config)", map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			br := &Connector{AS: as, Config: &bridgeconfig.Config{DoublePuppet: bridgeconfig.DoublePuppetConfig{
				Servers: tc.servers,
				Secrets: map[string]string{"example.com": "as_token:dp_secret"},
			}}}
			// useConfigASToken skips the whoami request.
			intent, newToken, err := newDoublePuppetUtil(br).Setup(context.Background(), "@alice:example.com", useConfigASToken)
			require.NoError(t, err)
			assert.Equal(t, useConfigASToken, newToken)
			assert.Equal(t, "dp_secret", intent.Client.AccessToken)
			assert.Same(t, as.HTTPClient, intent.Client.Client)
			tr, ok := intent.Client.Client.Transport.(*mautrix.IntboxLaneTransport)
			require.True(t, ok, "double-puppet requests must use the lane transport")
			assert.Equal(t, mautrix.DefaultIntboxMaxConnsPerPool, tr.MaxConnsPerPool())
		})
	}

	// Double puppets on another listed server share one separate client, also with capped pools.
	br := &Connector{AS: as, Config: &bridgeconfig.Config{DoublePuppet: bridgeconfig.DoublePuppetConfig{
		Servers: map[string]string{"other.example": "https://matrix.other.example"},
	}}}
	dp := newDoublePuppetUtil(br)
	bob, err := dp.newClient(context.Background(), "@bob:other.example", "t1")
	require.NoError(t, err)
	carol, err := dp.newClient(context.Background(), "@carol:other.example", "t2")
	require.NoError(t, err)
	assert.NotSame(t, as.HTTPClient, bob.Client)
	assert.Same(t, bob.Client, carol.Client)
	_, ok := bob.Client.Transport.(*mautrix.IntboxLaneTransport)
	assert.True(t, ok)
}
