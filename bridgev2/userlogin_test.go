// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package bridgev2

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mau.fi/util/dbutil"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

// fakeNetworkAPI is a NetworkAPI that only implements Disconnect. Calling any other method panics.
type fakeNetworkAPI struct {
	NetworkAPI
	disconnects  atomic.Int32
	blockOnClose chan struct{}
}

func (f *fakeNetworkAPI) Disconnect() {
	if f.blockOnClose != nil {
		<-f.blockOnClose
	}
	f.disconnects.Add(1)
}

func newLoginTestBridge(t *testing.T) (*Bridge, *User) {
	ctx := context.Background()
	rawDB, err := sql.Open("sqlite3", ":memory:?_busy_timeout=5000")
	require.NoError(t, err)
	rawDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = rawDB.Close() })
	db, err := dbutil.NewWithDB(rawDB, "sqlite3")
	require.NoError(t, err)
	br := &Bridge{
		ID:             "test",
		Log:            zerolog.Nop(),
		DB:             database.New("test", database.MetaTypes{}, db),
		usersByMXID:    make(map[id.UserID]*User),
		userLoginsByID: make(map[networkid.UserLoginID]*UserLogin),
		BackgroundCtx:  ctx,
	}
	require.NoError(t, br.DB.Upgrade(ctx))
	user := &User{
		User:   &database.User{BridgeID: br.ID, MXID: "@user:example.com"},
		Bridge: br,
		Log:    zerolog.Nop(),
		logins: make(map[networkid.UserLoginID]*UserLogin),
	}
	require.NoError(t, br.DB.User.Insert(ctx, user.User))
	br.usersByMXID[user.MXID] = user
	return br, user
}

func newLoginWithClient(t *testing.T, user *User, client NetworkAPI, loadErr error) (*UserLogin, error) {
	return user.NewLogin(context.Background(), &database.UserLogin{
		ID:         "acc",
		RemoteName: "+15550100001",
		Metadata:   &map[string]any{},
	}, &NewLoginParams{
		LoadUserLogin: func(ctx context.Context, ul *UserLogin) error {
			if client != nil {
				ul.Client = client
			}
			return loadErr
		},
	})
}

func TestNewLogin_DisconnectsReplacedClient(t *testing.T) {
	br, user := newLoginTestBridge(t)
	first := &fakeNetworkAPI{}
	ul, err := newLoginWithClient(t, user, first, nil)
	require.NoError(t, err)
	assert.Same(t, NetworkAPI(first), ul.Client)
	assert.EqualValues(t, 0, first.disconnects.Load())

	// Simulate something having used up the disconnect-once (e.g. an earlier UserLogin.Disconnect call).
	ul.Disconnect()
	assert.EqualValues(t, 1, first.disconnects.Load())

	second := &fakeNetworkAPI{}
	var clientDuringLoad NetworkAPI
	reused, err := user.NewLogin(context.Background(), &database.UserLogin{
		ID:         "acc",
		RemoteName: "+15550100001",
		Metadata:   &map[string]any{},
	}, &NewLoginParams{
		LoadUserLogin: func(ctx context.Context, ul *UserLogin) error {
			// Connectors can still see the old client while loading the new one.
			clientDuringLoad = ul.Client
			ul.Client = second
			return nil
		},
	})
	require.NoError(t, err)
	assert.Same(t, ul, reused, "the existing UserLogin must be reused")
	assert.Same(t, NetworkAPI(first), clientDuringLoad)
	assert.Same(t, NetworkAPI(second), ul.Client)
	assert.EqualValues(t, 2, first.disconnects.Load(), "the replaced client must be disconnected by NewLogin")
	assert.EqualValues(t, 0, second.disconnects.Load())

	// Nothing in the bridge keeps referencing the old client.
	assert.Same(t, ul, br.userLoginsByID["acc"])
	assert.Same(t, ul, user.logins["acc"])
	assert.Len(t, br.userLoginsByID, 1)

	// UserLogin.Disconnect works again and disconnects the new client.
	ul.Disconnect()
	assert.EqualValues(t, 1, second.disconnects.Load())
	assert.EqualValues(t, 2, first.disconnects.Load())
}

func TestNewLogin_KeepsClientWhenLoadFails(t *testing.T) {
	_, user := newLoginTestBridge(t)
	first := &fakeNetworkAPI{}
	ul, err := newLoginWithClient(t, user, first, nil)
	require.NoError(t, err)

	// LoadUserLogin fails before creating a client: the old client stays connected.
	_, err = newLoginWithClient(t, user, nil, errors.New("load failed"))
	require.Error(t, err)
	assert.Same(t, NetworkAPI(first), ul.Client)
	assert.EqualValues(t, 0, first.disconnects.Load())

	// LoadUserLogin clears the client without an error: NewLogin fails, the old client is kept.
	_, err = user.NewLogin(context.Background(), &database.UserLogin{ID: "acc", Metadata: &map[string]any{}}, &NewLoginParams{
		LoadUserLogin: func(ctx context.Context, ul *UserLogin) error {
			ul.Client = nil
			return nil
		},
	})
	require.ErrorContains(t, err, "client not filled")
	assert.Same(t, NetworkAPI(first), ul.Client)
	assert.EqualValues(t, 0, first.disconnects.Load())

	// LoadUserLogin installs a new client but then fails: the old client is replaced, so it's disconnected.
	third := &fakeNetworkAPI{}
	_, err = newLoginWithClient(t, user, third, errors.New("load failed late"))
	require.Error(t, err)
	assert.Same(t, NetworkAPI(third), ul.Client)
	assert.EqualValues(t, 1, first.disconnects.Load())
}

func TestNewLogin_SlowDisconnectDoesNotBlockForever(t *testing.T) {
	oldWait := replacedClientDisconnectWait
	replacedClientDisconnectWait = 50 * time.Millisecond
	t.Cleanup(func() { replacedClientDisconnectWait = oldWait })

	_, user := newLoginTestBridge(t)
	first := &fakeNetworkAPI{blockOnClose: make(chan struct{})}
	_, err := newLoginWithClient(t, user, first, nil)
	require.NoError(t, err)

	start := time.Now()
	_, err = newLoginWithClient(t, user, &fakeNetworkAPI{}, nil)
	require.NoError(t, err)
	assert.Less(t, time.Since(start), 2*time.Second)
	assert.EqualValues(t, 0, first.disconnects.Load(), "disconnect is still running in the background")

	close(first.blockOnClose)
	require.Eventually(t, func() bool { return first.disconnects.Load() == 1 }, 2*time.Second, 5*time.Millisecond)
}

// A freshly paired login (NewLogin, not loadUserLogin) must get a working inPortalCache, so that MarkInPortal,
// which runs for every remote event, does its user_portal upsert and Matrix membership check only once per portal.
func TestNewLogin_InitialisesInPortalCache(t *testing.T) {
	br, user := newLoginTestBridge(t)
	ctx := context.Background()
	ul, err := newLoginWithClient(t, user, &fakeNetworkAPI{}, nil)
	require.NoError(t, err)
	require.NotNil(t, ul.inPortalCache, "NewLogin must initialise inPortalCache like loadUserLogin does")

	key := networkid.PortalKey{ID: "chat1", Receiver: ul.ID}
	require.NoError(t, br.DB.Portal.Insert(ctx, &database.Portal{BridgeID: br.ID, PortalKey: key, Metadata: &map[string]any{}}))
	// No room yet (MXID is empty), so MarkInPortal only touches the database.
	portal := &Portal{Portal: &database.Portal{BridgeID: br.ID, PortalKey: key}, Bridge: br}

	ul.MarkInPortal(ctx, portal)
	assert.True(t, ul.inPortalCache.Has(key))
	up, err := br.DB.UserPortal.Get(ctx, ul.UserLogin, key)
	require.NoError(t, err)
	require.NotNil(t, up, "the first MarkInPortal creates the user_portal row")

	// Remove the row behind the cache's back: a second MarkInPortal must be a cache hit and not recreate it.
	require.NoError(t, br.DB.UserPortal.Delete(ctx, up))
	ul.MarkInPortal(ctx, portal)
	up, err = br.DB.UserPortal.Get(ctx, ul.UserLogin, key)
	require.NoError(t, err)
	assert.Nil(t, up, "the second MarkInPortal must not touch the database (cache miss before the fix)")

	// Re-pairing the same account reuses the UserLogin and keeps its cache.
	reused, err := newLoginWithClient(t, user, &fakeNetworkAPI{}, nil)
	require.NoError(t, err)
	assert.Same(t, ul, reused)
	assert.True(t, reused.inPortalCache.Has(key))
}
