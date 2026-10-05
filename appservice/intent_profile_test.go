// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appservice

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/id"
)

const testGhost = id.UserID("@ghost:example.com")

// profileTestServer is a fake homeserver profile API. Like Tuwunel >= 1.8.3, it answers GET for an unset
// profile field with 404 M_NOT_FOUND "The requested profile key does not exist."
type profileTestServer struct {
	lock      sync.Mutex
	fields    map[string]string
	getError  *mautrix.RespError
	puts      []string
	downloads int
}

func (ps *profileTestServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	writeErr := func(e mautrix.RespError) { e.Write(w) }
	profilePrefix := "/_matrix/client/v3/profile/" + testGhost.String() + "/"
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/_matrix/client/v3/register":
		_, _ = fmt.Fprintf(w, `{"user_id":%q}`, testGhost)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, profilePrefix):
		key := path.Base(r.URL.Path)
		if ps.getError != nil {
			writeErr(*ps.getError)
		} else if val, ok := ps.fields[key]; !ok {
			writeErr(mautrix.MNotFound.WithMessage("The requested profile key does not exist."))
		} else {
			_ = json.NewEncoder(w).Encode(map[string]string{key: val})
		}
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, profilePrefix):
		key := path.Base(r.URL.Path)
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body[key] == "" {
			delete(ps.fields, key)
		} else {
			ps.fields[key] = body[key]
		}
		ps.puts = append(ps.puts, key)
		_, _ = w.Write([]byte("{}"))
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/_matrix/client/v1/media/download/"):
		ps.downloads++
		_, _ = w.Write([]byte("png"))
	default:
		writeErr(mautrix.MUnrecognized.WithMessage("Unrecognized request"))
	}
}

func (ps *profileTestServer) putCount() int {
	ps.lock.Lock()
	defer ps.lock.Unlock()
	return len(ps.puts)
}

func newProfileTest(t *testing.T) (*profileTestServer, *IntentAPI) {
	ps := &profileTestServer{fields: make(map[string]string)}
	ts := httptest.NewServer(ps)
	t.Cleanup(ts.Close)
	as := Create()
	as.HomeserverDomain = "example.com"
	as.Registration = &Registration{AppToken: "as_token", SenderLocalpart: "bot"}
	as.DefaultHTTPRetries = 0
	require.NoError(t, as.SetHomeserverURL(ts.URL))
	return ps, as.Intent(testGhost)
}

// P2 from the load-test smoke run: on Tuwunel 1.8.3, ghost, DM and bot avatars were never set,
// because SetAvatarURL gave up when the "current avatar" check got 404 M_NOT_FOUND.
func TestSetAvatarURL_UnsetAvatarIsNotFound(t *testing.T) {
	ps, intent := newProfileTest(t)
	ctx := context.Background()
	avatar := id.ContentURI{Homeserver: "example.com", FileID: "avatar1"}

	require.NoError(t, intent.SetAvatarURL(ctx, avatar))
	assert.Equal(t, []string{"avatar_url"}, ps.puts)
	assert.Equal(t, avatar.String(), ps.fields["avatar_url"])
	assert.Equal(t, 1, ps.downloads)

	// Already set: no second PUT.
	require.NoError(t, intent.SetAvatarURL(ctx, avatar))
	assert.Equal(t, 1, ps.putCount())

	// Removing it works, and removing an avatar that isn't set (404 again) is a no-op.
	require.NoError(t, intent.SetAvatarURL(ctx, id.ContentURI{}))
	assert.Equal(t, 2, ps.putCount())
	_, stillSet := ps.fields["avatar_url"]
	assert.False(t, stillSet)
	require.NoError(t, intent.SetAvatarURL(ctx, id.ContentURI{}))
	assert.Equal(t, 2, ps.putCount())
}

func TestSetDisplayName_UnsetDisplayNameIsNotFound(t *testing.T) {
	ps, intent := newProfileTest(t)
	ctx := context.Background()

	require.NoError(t, intent.SetDisplayName(ctx, "Alice (WhatsApp)"))
	assert.Equal(t, []string{"displayname"}, ps.puts)
	assert.Equal(t, "Alice (WhatsApp)", ps.fields["displayname"])

	require.NoError(t, intent.SetDisplayName(ctx, "Alice (WhatsApp)"))
	assert.Equal(t, 1, ps.putCount())

	require.NoError(t, intent.SetDisplayName(ctx, ""))
	assert.Equal(t, 2, ps.putCount())
	require.NoError(t, intent.SetDisplayName(ctx, ""))
	assert.Equal(t, 2, ps.putCount(), "clearing an unset displayname is a no-op")
}

// Only M_NOT_FOUND means "not set": any other error from the check still aborts the update.
func TestSetProfile_OtherCheckErrorsStillFail(t *testing.T) {
	for _, respErr := range []mautrix.RespError{
		mautrix.MForbidden.WithMessage("nope"),
		mautrix.MUnrecognized.WithStatus(http.StatusNotFound).WithMessage("no such endpoint"),
	} {
		t.Run(respErr.ErrCode, func(t *testing.T) {
			ps, intent := newProfileTest(t)
			ps.getError = &respErr
			ctx := context.Background()
			err := intent.SetAvatarURL(ctx, id.ContentURI{Homeserver: "example.com", FileID: "avatar1"})
			assert.ErrorContains(t, err, "failed to check current avatar URL")
			err = intent.SetDisplayName(ctx, "Alice")
			assert.ErrorContains(t, err, "failed to check current displayname")
			assert.Equal(t, 0, ps.putCount())
		})
	}
}

func TestCreate_HomeserverLaneTransport(t *testing.T) {
	as := Create()
	as.Registration = &Registration{SenderLocalpart: "bot"}
	as.HomeserverDomain = "example.com"
	assert.Equal(t, mautrix.DefaultIntboxMaxConnsPerPool, as.HomeserverMaxConnsPerPool)
	tr, ok := as.HTTPClient.Transport.(*mautrix.IntboxLaneTransport)
	require.True(t, ok, "Create must use the lane transport")
	assert.Equal(t, mautrix.DefaultIntboxMaxConnsPerPool, tr.MaxConnsPerPool())

	as.HomeserverMaxConnsPerPool = 128
	require.NoError(t, as.SetHomeserverURL("http://hs.example:8008"))
	tr, ok = as.HTTPClient.Transport.(*mautrix.IntboxLaneTransport)
	require.True(t, ok)
	assert.Equal(t, 128, tr.MaxConnsPerPool())
	assert.Equal(t, 128, tr.Pool(mautrix.IntboxPoolLive).MaxConnsPerHost)
	assert.Same(t, as.HTTPClient, as.Client("@someone:example.com").Client, "clients share the appservice HTTP client")

	// Unix socket: every pool keeps the unix dialer (TestClient_UnixSocket does a real request).
	require.NoError(t, as.SetHomeserverURL("unix:///tmp/hs.sock"))
	tr, ok = as.HTTPClient.Transport.(*mautrix.IntboxLaneTransport)
	require.True(t, ok)
	assert.NotNil(t, tr.Pool(mautrix.IntboxPoolBulk).DialContext)

	// 0 restores the previous behaviour: Go's default transport (nil), or the plain unix transport.
	as.HomeserverMaxConnsPerPool = 0
	require.NoError(t, as.SetHomeserverURL("http://hs.example:8008"))
	assert.Nil(t, as.HTTPClient.Transport)
	require.NoError(t, as.SetHomeserverURL("unix:///tmp/hs.sock"))
	plain, ok := as.HTTPClient.Transport.(*http.Transport)
	require.True(t, ok)
	assert.Equal(t, 0, plain.MaxConnsPerHost)
}
