// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package bridgev2

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mau.fi/util/exsync"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// pendingMatrixConnector is a MatrixConnector whose appservice still holds accepted but unhandled events. Calling
// any MatrixConnector method other than the ones below panics.
type pendingMatrixConnector struct {
	MatrixConnector
	pending atomic.Int32
	lock    sync.Mutex
	steps   []string
}

func (m *pendingMatrixConnector) PendingMatrixEvents() int { return int(m.pending.Load()) }
func (m *pendingMatrixConnector) PreStop()                 { m.step("prestop") }
func (m *pendingMatrixConnector) Stop()                    { m.step("stop") }

func (m *pendingMatrixConnector) step(name string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.steps = append(m.steps, name)
}

func (m *pendingMatrixConnector) getSteps() []string {
	m.lock.Lock()
	defer m.lock.Unlock()
	return append([]string(nil), m.steps...)
}

// recordingNetworkAPI records its disconnect in the Matrix connector's step list.
type recordingNetworkAPI struct {
	NetworkAPI
	mc *pendingMatrixConnector
}

func (r *recordingNetworkAPI) Disconnect() { r.mc.step("disconnect") }

// Load-test L6-2: a stop returned 4 ms after SIGTERM. Matrix events whose appservice transaction had been answered
// were still in the appservice queue, and the homeserver doesn't resend them. Stop must now let them be handled
// while the logins are still connected (their portals send them to WhatsApp), and only then disconnect.
func TestStop_HandlesAcceptedMatrixEventsBeforeDisconnecting(t *testing.T) {
	br, user := newLoginTestBridge(t)
	mc := &pendingMatrixConnector{}
	br.Matrix = mc
	br.ExternallyManagedDB = true
	br.stopBackfillQueue = exsync.NewEvent()
	_, err := newLoginWithClient(t, user, &recordingNetworkAPI{mc: mc}, nil)
	require.NoError(t, err)

	mc.pending.Store(3)
	go func() {
		for i := 0; i < 3; i++ {
			time.Sleep(100 * time.Millisecond)
			mc.pending.Add(-1)
		}
		mc.step("matrix events handled")
	}()
	start := time.Now()
	br.Stop()
	assert.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond, "stop waited for the queued Matrix events")
	assert.Equal(t, []string{"prestop", "matrix events handled", "disconnect", "stop"}, mc.getSteps())
}

func TestDrainMatrixEvents_BoundedAndDisabled(t *testing.T) {
	br := newDrainTestBridge()
	mc := &pendingMatrixConnector{}
	mc.pending.Store(1) // a handler that never finishes
	br.Matrix = mc
	start := time.Now()
	br.drainMatrixEvents(300 * time.Millisecond)
	assert.GreaterOrEqual(t, time.Since(start), 300*time.Millisecond)
	assert.Less(t, time.Since(start), 2*time.Second)

	start = time.Now()
	br.drainMatrixEvents(0)
	assert.Less(t, time.Since(start), 50*time.Millisecond, "0 disables the wait")

	// A Matrix connector that can't report pending events: no wait.
	br.Matrix = struct{ MatrixConnector }{}
	start = time.Now()
	br.drainMatrixEvents(5 * time.Second)
	assert.Less(t, time.Since(start), 50*time.Millisecond)
}

// MAUTRIX_SHUTDOWN_DRAIN_SECONDS=0 (ShutdownDrainTimeout = 0) disables the Matrix-side wait too.
func TestStop_DrainDisabledDoesNotWaitForMatrixEvents(t *testing.T) {
	orig := ShutdownDrainTimeout
	ShutdownDrainTimeout = 0
	t.Cleanup(func() { ShutdownDrainTimeout = orig })
	br, _ := newLoginTestBridge(t)
	mc := &pendingMatrixConnector{}
	mc.pending.Store(1)
	br.Matrix = mc
	br.ExternallyManagedDB = true
	br.stopBackfillQueue = exsync.NewEvent()
	start := time.Now()
	br.Stop()
	assert.Less(t, time.Since(start), time.Second)
}

// typingNetworkAPI is a connected network client that bridges Matrix typing events slowly and records, in the Matrix
// connector's step list, whether it was still connected when it sent one.
type typingNetworkAPI struct {
	NetworkAPI
	mc           *pendingMatrixConnector
	disconnected atomic.Bool
}

func (c *typingNetworkAPI) IsLoggedIn() bool { return !c.disconnected.Load() }
func (c *typingNetworkAPI) Disconnect() {
	c.disconnected.Store(true)
	c.mc.step("disconnect")
}
func (c *typingNetworkAPI) HandleMatrixTyping(ctx context.Context, msg *MatrixTyping) error {
	time.Sleep(200 * time.Millisecond)
	c.mc.step(fmt.Sprintf("sent to network (connected=%v)", c.IsLoggedIn()))
	return nil
}

// Review fix round 2 (L6-2): an appservice handler only queues a room event into its portal (PortalEventBuffer > 0),
// so the Matrix connector reports it handled at once. Stop must still wait until the portal has handled it, before
// it disconnects the logins: otherwise the event runs with a disconnected client and fails with "not logged in".
func TestStop_HandlesMatrixEventsQueuedInPortalBeforeDisconnecting(t *testing.T) {
	origBuf := PortalEventBuffer
	PortalEventBuffer = 16
	t.Cleanup(func() { PortalEventBuffer = origBuf })
	br, user := newLoginTestBridge(t)
	mc := &pendingMatrixConnector{}
	br.Matrix = mc
	br.ExternallyManagedDB = true
	br.stopBackfillQueue = exsync.NewEvent()
	br.portalsByKey = make(map[networkid.PortalKey]*Portal)
	client := &typingNetworkAPI{mc: mc}
	login, err := newLoginWithClient(t, user, client, nil)
	require.NoError(t, err)
	portal := &Portal{
		Portal: &database.Portal{
			BridgeID:  br.ID,
			PortalKey: networkid.PortalKey{ID: "120363240000400000@g.us", Receiver: login.ID},
			MXID:      "!chat:example.com",
		},
		Bridge:                br,
		Log:                   zerolog.Nop(),
		events:                make(chan portalEvent, PortalEventBuffer),
		currentlyTypingLogins: make(map[id.UserID]*UserLogin),
	}
	var cancel context.CancelFunc
	portal.backgroundCtx, cancel = context.WithCancel(context.Background())
	t.Cleanup(cancel)
	portal.deleted = portal.backgroundCtx.Done()
	br.portalsByKey[portal.PortalKey] = portal
	// The portal's event loop (without the outgoing-message-timeout loop, which needs a network connector).
	go func() {
		for i := 0; ; i++ {
			select {
			case rawEvt := <-portal.events:
				portal.handleSingleEventWithDelayLogging(i, rawEvt)
			case <-portal.deleted:
				return
			}
		}
	}()

	// What the appservice handler does for an accepted transaction: queue the events and return at once.
	// The user starts typing, then stops.
	for _, typing := range [][]id.UserID{{user.MXID}, nil} {
		res := portal.queueEvent(context.Background(), &portalMatrixEvent{sender: user, evt: &event.Event{
			Type:    event.EphemeralEventTyping,
			RoomID:  portal.MXID,
			Mautrix: event.MautrixInfo{EventSource: event.SourceEphemeral},
			Content: event.Content{Parsed: &event.TypingEventContent{UserIDs: typing}},
		}})
		require.True(t, res.Queued)
	}
	assert.EqualValues(t, 2, br.matrixPortalEvents.Load(), "queued Matrix events are counted")
	assert.Zero(t, mc.PendingMatrixEvents(), "the Matrix connector already reports them handled")

	br.Stop()
	assert.Equal(t, []string{"prestop", "sent to network (connected=true)", "sent to network (connected=true)", "disconnect", "stop"},
		mc.getSteps(), "the events queued in the portal were sent while the login was still connected")
	assert.Zero(t, br.matrixPortalEvents.Load())
}
