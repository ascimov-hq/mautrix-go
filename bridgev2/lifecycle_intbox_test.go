// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package bridgev2

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

// connectingNetworkAPI is a network client that reports IsLoggedIn only once its connection is up, like a
// WhatsApp client right after StartLogins called Connect.
type connectingNetworkAPI struct {
	NetworkAPI
	loggedIn atomic.Bool
}

func (c *connectingNetworkAPI) IsLoggedIn() bool { return c.loggedIn.Load() }
func (c *connectingNetworkAPI) Disconnect()      {}

func setLifecycleTimings(t *testing.T, grace, maxWait time.Duration) {
	origGrace, origWait, origPoll := LoginStartupGrace, LoginStartupMaxWait, loginStartupPollInterval
	LoginStartupGrace, LoginStartupMaxWait, loginStartupPollInterval = grace, maxWait, 20*time.Millisecond
	t.Cleanup(func() {
		LoginStartupGrace, LoginStartupMaxWait, loginStartupPollInterval = origGrace, origWait, origPoll
	})
}

// newStartupTestPortal is a WhatsApp-style portal (receiver = the user's login) whose login isn't connected yet.
func newStartupTestPortal(t *testing.T) (*Portal, *User, *connectingNetworkAPI) {
	br, user := newLoginTestBridge(t)
	client := &connectingNetworkAPI{}
	login, err := newLoginWithClient(t, user, client, nil)
	require.NoError(t, err)
	portal := &Portal{
		Portal: &database.Portal{
			BridgeID:  br.ID,
			PortalKey: networkid.PortalKey{ID: "120363240000400000@g.us", Receiver: login.ID},
		},
		Bridge: br,
	}
	return portal, user, client
}

func TestFindPreferredLoginForMatrixEvent_WaitsForConnectingLoginAtStartup(t *testing.T) {
	setLifecycleTimings(t, time.Minute, 5*time.Second)
	portal, user, client := newStartupTestPortal(t)
	ctx := context.Background()
	// StartConnectors has run (appservice listener up), StartLogins hasn't finished.
	portal.Bridge.connectorsStartedAt.Store(time.Now().UnixNano())

	_, _, err := portal.FindPreferredLogin(ctx, user, true)
	require.ErrorIs(t, err, ErrNotLoggedIn, "before the fix the event failed here: receiver login is not logged in")

	go func() {
		time.Sleep(300 * time.Millisecond)
		client.loggedIn.Store(true)
	}()
	start := time.Now()
	login, _, err := portal.findPreferredLoginForMatrixEvent(ctx, user, true)
	require.NoError(t, err)
	require.NotNil(t, login)
	assert.Equal(t, portal.Receiver, login.ID)
	assert.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)
	assert.Less(t, time.Since(start), 3*time.Second)
}

func TestFindPreferredLoginForMatrixEvent_GivesUpAfterMaxWait(t *testing.T) {
	setLifecycleTimings(t, time.Minute, 400*time.Millisecond)
	portal, user, _ := newStartupTestPortal(t)
	portal.Bridge.connectorsStartedAt.Store(time.Now().UnixNano())
	portal.Bridge.loginsStartedAt.Store(time.Now().UnixNano())

	start := time.Now()
	_, _, err := portal.findPreferredLoginForMatrixEvent(context.Background(), user, true)
	require.ErrorIs(t, err, ErrNotLoggedIn)
	assert.GreaterOrEqual(t, time.Since(start), 400*time.Millisecond)
	assert.Less(t, time.Since(start), 3*time.Second)
}

func TestFindPreferredLoginForMatrixEvent_NoWaitOutsideStartup(t *testing.T) {
	setLifecycleTimings(t, 200*time.Millisecond, 5*time.Second)
	cases := map[string]func(br *Bridge){
		"never started (tests, tools)": func(br *Bridge) {},
		"startup window over": func(br *Bridge) {
			br.connectorsStartedAt.Store(time.Now().Add(-time.Hour).UnixNano())
			br.loginsStartedAt.Store(time.Now().Add(-time.Second).UnixNano())
		},
		"background mode": func(br *Bridge) {
			br.Background = true
			br.connectorsStartedAt.Store(time.Now().UnixNano())
		},
		"stopping": func(br *Bridge) {
			br.connectorsStartedAt.Store(time.Now().UnixNano())
			br.stopping.Store(true)
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			portal, user, _ := newStartupTestPortal(t)
			setup(portal.Bridge)
			start := time.Now()
			_, _, err := portal.findPreferredLoginForMatrixEvent(context.Background(), user, true)
			require.ErrorIs(t, err, ErrNotLoggedIn)
			assert.Less(t, time.Since(start), 300*time.Millisecond)
		})
	}
	t.Run("synchronous portal events", func(t *testing.T) {
		orig := PortalEventBuffer
		PortalEventBuffer = 0
		t.Cleanup(func() { PortalEventBuffer = orig })
		portal, user, _ := newStartupTestPortal(t)
		portal.Bridge.connectorsStartedAt.Store(time.Now().UnixNano())
		start := time.Now()
		_, _, err := portal.findPreferredLoginForMatrixEvent(context.Background(), user, true)
		require.ErrorIs(t, err, ErrNotLoggedIn)
		assert.Less(t, time.Since(start), 300*time.Millisecond, "waiting would hold up the whole appservice event loop")
	})
	t.Run("user without logins", func(t *testing.T) {
		portal, _, _ := newStartupTestPortal(t)
		portal.Bridge.connectorsStartedAt.Store(time.Now().UnixNano())
		stranger := &User{User: &database.User{MXID: "@stranger:example.com"}, Bridge: portal.Bridge, Log: zerolog.Nop(),
			logins: make(map[networkid.UserLoginID]*UserLogin)}
		start := time.Now()
		_, _, err := portal.findPreferredLoginForMatrixEvent(context.Background(), stranger, true)
		require.ErrorIs(t, err, ErrNotLoggedIn)
		assert.Less(t, time.Since(start), 300*time.Millisecond)
	})
}

func newDrainTestBridge() *Bridge {
	return &Bridge{Log: zerolog.Nop(), portalsByKey: make(map[networkid.PortalKey]*Portal)}
}

func TestDrainPortalEvents_WaitsForRunningAndQueuedEvents(t *testing.T) {
	br := newDrainTestBridge()
	portal := &Portal{events: make(chan portalEvent, 4)}
	br.portalsByKey[networkid.PortalKey{ID: "chat"}] = portal
	portal.events <- &portalMatrixEvent{}
	br.activePortalEvents.Store(1)
	go func() {
		// The running handler stores its message row and finishes, then the loop takes and handles the queued one.
		time.Sleep(150 * time.Millisecond)
		<-portal.events
		br.activePortalEvents.Add(1)
		br.activePortalEvents.Add(-1)
		time.Sleep(150 * time.Millisecond)
		br.activePortalEvents.Add(-1)
	}()
	start := time.Now()
	br.drainPortalEvents(5 * time.Second)
	assert.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)
	assert.Less(t, time.Since(start), 3*time.Second)
	assert.Zero(t, br.activePortalEvents.Load())
	assert.Empty(t, portal.events)
}

func TestDrainPortalEvents_BoundedAndDisabled(t *testing.T) {
	br := newDrainTestBridge()
	br.activePortalEvents.Store(1) // a handler that never finishes
	start := time.Now()
	br.drainPortalEvents(300 * time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)
	assert.Less(t, time.Since(start), 2*time.Second)

	start = time.Now()
	br.drainPortalEvents(0)
	assert.Less(t, time.Since(start), 50*time.Millisecond, "0 disables the wait")
}

// The counter that the drain waits for follows the real handler: it's up while handleSingleEvent runs and back
// down once the handler has returned.
func TestHandleSingleEventWithDelayLogging_CountsRunningEvents(t *testing.T) {
	br := newDrainTestBridge()
	portal := &Portal{Portal: &database.Portal{MXID: "!existing:example.com"}, Bridge: br, Log: zerolog.Nop()}
	portal.backgroundCtx = context.Background()
	started, release := make(chan struct{}), make(chan struct{})
	// A create event for a room that already exists runs only its callback, which blocks here like a slow
	// handler (e.g. one waiting for a Matrix send).
	evt := &portalCreateEvent{ctx: context.Background(), cb: func(err error) {
		close(started)
		<-release
	}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		portal.handleSingleEventWithDelayLogging(0, evt)
	}()
	<-started
	assert.EqualValues(t, 1, br.activePortalEvents.Load(), "running handler is counted")

	drained := make(chan time.Duration)
	go func() {
		start := time.Now()
		br.drainPortalEvents(5 * time.Second)
		drained <- time.Since(start)
	}()
	time.Sleep(200 * time.Millisecond)
	close(release)
	select {
	case waited := <-drained:
		assert.GreaterOrEqual(t, waited, 200*time.Millisecond, "the drain waited for the running handler")
	case <-time.After(5 * time.Second):
		t.Fatal("drain didn't return after the handler finished")
	}
	<-done
	assert.Zero(t, br.activePortalEvents.Load())
}
