// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mxmain

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mau.fi/util/configupgrade"
	"gopkg.in/yaml.v3"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
)

// Existing deployments don't have provisioning.supersede_logins in their config file:
// the config upgrade must fill in the example config's default (true), and keep an explicit false.
func TestConfigUpgrade_SupersedeLoginsDefault(t *testing.T) {
	var base strings.Builder
	require.NoError(t, matrixExampleConfigBaseTemplate.Execute(&base, bridgev2.BridgeName{
		DisplayName:          "Test",
		NetworkURL:           "https://example.com",
		NetworkID:            "test",
		BeeperBridgeType:     "test",
		DefaultPort:          29999,
		DefaultCommandPrefix: "!test",
	}))
	upgrader := configupgrade.MergeUpgraders(base.String(), bridgeconfig.Upgrader)

	for _, tc := range []struct {
		name   string
		config string
		want   bool
	}{
		{"missing", "provisioning:\n    shared_secret: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n", true},
		{"explicit false", "provisioning:\n    shared_secret: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n    supersede_logins: false\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.config), 0600))
			data, _, err := configupgrade.Do(path, false, upgrader)
			require.NoError(t, err)
			var cfg bridgeconfig.Config
			require.NoError(t, yaml.Unmarshal(data, &cfg))
			assert.Equal(t, tc.want, cfg.Provisioning.SupersedeLogins)
		})
	}
}

// homeserver.max_connections_per_pool: existing config files get the example config's default (64),
// explicit values (including 0 = no cap) are kept, and MakeAppService applies the value to the HTTP client.
func TestConfigUpgrade_MaxConnectionsPerPool(t *testing.T) {
	var base strings.Builder
	require.NoError(t, matrixExampleConfigBaseTemplate.Execute(&base, bridgev2.BridgeName{
		DisplayName:          "Test",
		NetworkURL:           "https://example.com",
		NetworkID:            "test",
		BeeperBridgeType:     "test",
		DefaultPort:          29999,
		DefaultCommandPrefix: "!test",
	}))
	upgrader := configupgrade.MergeUpgraders(base.String(), bridgeconfig.Upgrader)

	for _, tc := range []struct {
		name   string
		config string
		want   int
	}{
		{"missing", "homeserver:\n    address: http://hs.example:8008\n    domain: example.com\n", mautrix.DefaultIntboxMaxConnsPerPool},
		{"explicit 128", "homeserver:\n    address: http://hs.example:8008\n    domain: example.com\n    max_connections_per_pool: 128\n", 128},
		{"explicit 0", "homeserver:\n    address: http://hs.example:8008\n    domain: example.com\n    max_connections_per_pool: 0\n", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			require.NoError(t, os.WriteFile(path, []byte(tc.config), 0600))
			data, _, err := configupgrade.Do(path, false, upgrader)
			require.NoError(t, err)
			var cfg bridgeconfig.Config
			require.NoError(t, yaml.Unmarshal(data, &cfg))
			assert.Equal(t, tc.want, cfg.Homeserver.MaxConnectionsPerPool)

			as := cfg.MakeAppService()
			assert.Equal(t, tc.want, as.HomeserverMaxConnsPerPool)
			tr, isLaneTransport := as.HTTPClient.Transport.(*mautrix.IntboxLaneTransport)
			if tc.want == 0 {
				assert.False(t, isLaneTransport)
				assert.Nil(t, as.HTTPClient.Transport)
			} else {
				require.True(t, isLaneTransport)
				assert.Equal(t, tc.want, tr.MaxConnsPerPool())
			}
		})
	}
}
