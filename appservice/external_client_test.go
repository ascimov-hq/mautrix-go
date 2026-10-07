// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appservice

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/id"
)

const testDoublePuppet = id.UserID("@alice:example.com")

func newExternalClientTestAS(t *testing.T, hsURL string, maxConnsPerPool int) *AppService {
	as := Create()
	as.HomeserverDomain = "example.com"
	as.Registration = &Registration{AppToken: "as_token", SenderLocalpart: "bot"}
	as.DefaultHTTPRetries = 0
	as.HomeserverMaxConnsPerPool = maxConnsPerPool
	require.NoError(t, as.SetHomeserverURL(hsURL))
	return as
}

// Review finding: both whatsapp.yaml files list the bridge's own server in double_puppet.servers, with the same URL
// as homeserver.address. NewExternalMautrixClient then gave every double puppet a new client with Go's default
// transport, so all double-puppet traffic bypassed the capped pools. The appservice's own homeserver must keep the
// appservice HTTP client, however the URL is spelled.
func TestNewExternalMautrixClient_OwnHomeserverUsesAppServicePools(t *testing.T) {
	as := newExternalClientTestAS(t, "http://hs.example:8008", mautrix.DefaultIntboxMaxConnsPerPool)
	for _, hsURL := range []string{
		"", // not listed in double_puppet.servers
		"http://hs.example:8008",
		"http://HS.Example:8008/",
		"http://hs.example:8008/prefix",
	} {
		client, err := as.NewExternalMautrixClient(testDoublePuppet, "dp_token", hsURL)
		require.NoError(t, err, hsURL)
		assert.Same(t, as.HTTPClient, client.Client, "URL %q", hsURL)
		tr, ok := client.Client.Transport.(*mautrix.IntboxLaneTransport)
		require.True(t, ok, "URL %q", hsURL)
		assert.Equal(t, mautrix.DefaultIntboxMaxConnsPerPool, tr.MaxConnsPerPool())
		assert.Equal(t, "dp_token", client.AccessToken)
		assert.False(t, client.SetAppServiceUserID)
		assert.True(t, strings.EqualFold("hs.example:8008", client.HomeserverURL.Host), client.HomeserverURL.Host)
	}

	// Default ports count as the same host.
	as = newExternalClientTestAS(t, "https://hs.example", mautrix.DefaultIntboxMaxConnsPerPool)
	client, err := as.NewExternalMautrixClient(testDoublePuppet, "dp_token", "https://hs.example:443")
	require.NoError(t, err)
	assert.Same(t, as.HTTPClient, client.Client)
}

// Other homeservers share one client (there used to be a new one per double puppet), with its own capped pools.
func TestNewExternalMautrixClient_OtherHomeserversShareOneCappedClient(t *testing.T) {
	as := newExternalClientTestAS(t, "http://hs.example:8008", mautrix.DefaultIntboxMaxConnsPerPool)
	var shared *http.Client
	for _, hsURL := range []string{
		"https://hs.example:8008", // other scheme
		"http://hs.example:8448",  // other port
		"http://hs.example",       // port 80
		"https://matrix.other.example",
		"http://hs.example.evil:8008",
	} {
		client, err := as.NewExternalMautrixClient("@bob:other.example", "dp_token", hsURL)
		require.NoError(t, err, hsURL)
		assert.NotSame(t, as.HTTPClient, client.Client, "URL %q", hsURL)
		if shared == nil {
			shared = client.Client
		}
		assert.Same(t, shared, client.Client, "URL %q: one client for all other homeservers", hsURL)
		tr, ok := client.Client.Transport.(*mautrix.IntboxLaneTransport)
		require.True(t, ok, "URL %q", hsURL)
		assert.Equal(t, mautrix.DefaultIntboxMaxConnsPerPool, tr.MaxConnsPerPool())
		assert.Equal(t, 180*time.Second, client.Client.Timeout)
		assert.Nil(t, client.Client.Jar, "as before: no cookie jar for other homeservers")
	}
	_, err := as.NewExternalMautrixClient("@bob:other.example", "dp_token", "http://[::1")
	assert.Error(t, err)

	// 0 (pools disabled): exactly the previous transports, Go's default for both.
	as = newExternalClientTestAS(t, "http://hs.example:8008", 0)
	own, err := as.NewExternalMautrixClient(testDoublePuppet, "dp_token", "http://hs.example:8008")
	require.NoError(t, err)
	assert.Same(t, as.HTTPClient, own.Client)
	assert.Nil(t, own.Client.Transport)
	other, err := as.NewExternalMautrixClient("@bob:other.example", "dp_token", "https://matrix.other.example")
	require.NoError(t, err)
	assert.Nil(t, other.Client.Transport)
	assert.Equal(t, 180*time.Second, other.Client.Timeout)
}

// End to end: double-puppet member writes from the bulk lane, held at the "proxy", never get more than the bulk
// pool's cap of connections when the double puppet's URL is the bridge's own homeserver URL.
func TestNewExternalMautrixClient_DoublePuppetWritesAreCapped(t *testing.T) {
	const capPerPool = 3
	const writes = 10
	var lock sync.Mutex
	inFlight, maxInFlight := 0, 0
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		inFlight++
		maxInFlight = max(maxInFlight, inFlight)
		lock.Unlock()
		<-release
		lock.Lock()
		inFlight--
		lock.Unlock()
		_, _ = w.Write([]byte(`{"event_id":"$x"}`))
	}))
	t.Cleanup(ts.Close)
	t.Cleanup(releaseAll)
	as := newExternalClientTestAS(t, ts.URL, capPerPool)
	t.Cleanup(as.HTTPClient.CloseIdleConnections)
	client, err := as.NewExternalMautrixClient(testDoublePuppet, "dp_token", ts.URL)
	require.NoError(t, err)
	require.Same(t, as.HTTPClient, client.Client)

	ctx := mautrix.WithIntboxLane(context.Background(), mautrix.IntboxLaneBulk)
	var wg sync.WaitGroup
	errs := make(chan error, writes)
	for i := range writes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			url := client.BuildClientURL("v3", "rooms", "!room"+string(rune('a'+i))+":example.com", "state", "m.room.member", testDoublePuppet)
			_, err := client.MakeRequest(ctx, http.MethodPut, url, map[string]string{"membership": "join"}, nil)
			errs <- err
		}()
	}
	current := func() int {
		lock.Lock()
		defer lock.Unlock()
		return inFlight
	}
	require.Eventually(t, func() bool { return current() == capPerPool }, 5*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, capPerPool, current(), "the other double-puppet writes must wait in the bridge")
	releaseAll()
	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(t, err)
	}
	lock.Lock()
	defer lock.Unlock()
	assert.Equal(t, capPerPool, maxInFlight)
}
