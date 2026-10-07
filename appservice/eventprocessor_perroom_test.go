// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appservice

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// handledLog records the event IDs a handler saw, per room and overall.
type handledLog struct {
	lock   sync.Mutex
	byRoom map[id.RoomID][]string
	all    []string
}

func (hl *handledLog) add(evt *event.Event) {
	hl.lock.Lock()
	defer hl.lock.Unlock()
	if hl.byRoom == nil {
		hl.byRoom = make(map[id.RoomID][]string)
	}
	hl.byRoom[evt.RoomID] = append(hl.byRoom[evt.RoomID], evt.ID.String())
	hl.all = append(hl.all, evt.ID.String())
}

func (hl *handledLog) count() int {
	hl.lock.Lock()
	defer hl.lock.Unlock()
	return len(hl.all)
}

func (hl *handledLog) room(roomID id.RoomID) []string {
	hl.lock.Lock()
	defer hl.lock.Unlock()
	return append([]string(nil), hl.byRoom[roomID]...)
}

func newPerRoomTestProcessor(t *testing.T, mode ExecMode, maxPending int) (*AppService, *EventProcessor) {
	as := newQueueFilterTestAppService(t)
	ep := NewEventProcessor(as)
	ep.ExecMode = mode
	ep.RoomQueueMaxPending = maxPending
	t.Cleanup(ep.Stop)
	return as, ep
}

func textEvent(roomID id.RoomID, evtID string) *event.Event {
	return &event.Event{
		Type:    event.EventMessage,
		ID:      id.EventID(evtID),
		RoomID:  roomID,
		Sender:  "@user:example.com",
		Content: event.Content{Parsed: &event.MessageEventContent{MsgType: event.MsgText, Body: evtID}},
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	require.Eventually(t, cond, 10*time.Second, time.Millisecond, what)
}

// The L5 load-test shape: 30 chats get 30 sequential messages each at once, and handling takes a random 0-2 ms per
// event (decryption, database). Every room's events must be handled in the order they arrived, and rooms must run
// in parallel.
func TestSyncPerRoom_KeepsOrderWithinEachRoom(t *testing.T) {
	as, ep := newPerRoomTestProcessor(t, SyncPerRoom, 0)
	var log handledLog
	var running, maxRunning int
	var runLock sync.Mutex
	perRoomRunning := make(map[id.RoomID]int)
	ep.On(event.EventMessage, func(ctx context.Context, evt *event.Event) {
		runLock.Lock()
		running++
		maxRunning = max(maxRunning, running)
		perRoomRunning[evt.RoomID]++
		assert.Equal(t, 1, perRoomRunning[evt.RoomID], "two handlers ran at once for %s", evt.RoomID)
		runLock.Unlock()
		time.Sleep(time.Duration(rand.IntN(2000)) * time.Microsecond)
		log.add(evt)
		runLock.Lock()
		running--
		perRoomRunning[evt.RoomID]--
		runLock.Unlock()
	})
	ep.Start(context.Background())

	const rooms, perRoom = 30, 30
	for i := 0; i < perRoom; i++ {
		for r := 0; r < rooms; r++ {
			as.Events <- textEvent(id.RoomID(fmt.Sprintf("!chat%02d:example.com", r)), fmt.Sprintf("$c%02d-m%02d", r, i))
		}
	}
	waitFor(t, "all events handled", func() bool { return log.count() == rooms*perRoom })
	for r := 0; r < rooms; r++ {
		roomID := id.RoomID(fmt.Sprintf("!chat%02d:example.com", r))
		want := make([]string, perRoom)
		for i := range want {
			want[i] = fmt.Sprintf("$c%02d-m%02d", r, i)
		}
		assert.Equal(t, want, log.room(roomID), "order in %s", roomID)
	}
	runLock.Lock()
	assert.Greater(t, maxRunning, 1, "rooms were never handled in parallel")
	runLock.Unlock()
	waitFor(t, "queues empty", func() bool { evts, rms := ep.PendingRoomEvents(); return evts == 0 && rms == 0 })
}

// One room whose handling is stuck (a portal busy with an import's ChatResync, a missing megolm key) must not hold
// up other rooms. With the upstream Sync loop it does: that is the L5 stall (p50 202 s for app -> WhatsApp sends).
func TestSyncPerRoom_StuckRoomDoesNotBlockOthers(t *testing.T) {
	for _, tc := range []struct {
		name          string
		mode          ExecMode
		othersHandled bool
	}{
		{"SyncPerRoom", SyncPerRoom, true},
		{"Sync (upstream, head-of-line blocking)", Sync, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			as, ep := newPerRoomTestProcessor(t, tc.mode, 0)
			release := make(chan struct{})
			var releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			var log handledLog
			ep.On(event.EventMessage, func(ctx context.Context, evt *event.Event) {
				if evt.RoomID == "!importing:example.com" {
					<-release
				}
				log.add(evt)
			})
			ep.Start(context.Background())

			as.Events <- textEvent("!importing:example.com", "$import1")
			as.Events <- textEvent("!importing:example.com", "$import2")
			for i := 0; i < 10; i++ {
				as.Events <- textEvent("!other:example.com", fmt.Sprintf("$other%d", i))
			}
			if tc.othersHandled {
				waitFor(t, "other room handled while the first is stuck", func() bool { return len(log.room("!other:example.com")) == 10 })
				assert.Empty(t, log.room("!importing:example.com"))
				evts, rooms := ep.PendingRoomEvents()
				assert.Equal(t, 2, evts)
				assert.Equal(t, 1, rooms)
			} else {
				time.Sleep(200 * time.Millisecond)
				assert.Zero(t, log.count(), "upstream Sync handled other events behind a stuck one")
			}
			releaseOnce.Do(func() { close(release) })
			waitFor(t, "everything handled after release", func() bool { return log.count() == 12 })
			assert.Equal(t, []string{"$import1", "$import2"}, log.room("!importing:example.com"))
		})
	}
}

// RoomQueueMaxPending bounds what SyncPerRoom takes out of the Events channel: when it's reached, the loop stops
// reading, so the channel fills and the homeserver's transactions are slowed down as with Sync.
func TestSyncPerRoom_MaxPendingAppliesBackpressure(t *testing.T) {
	as, ep := newPerRoomTestProcessor(t, SyncPerRoom, 3)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var log handledLog
	ep.On(event.EventMessage, func(ctx context.Context, evt *event.Event) {
		if evt.RoomID == "!stuck:example.com" {
			<-release
		}
		log.add(evt)
	})
	ep.Start(context.Background())

	for i := 0; i < 3; i++ {
		as.Events <- textEvent("!stuck:example.com", fmt.Sprintf("$stuck%d", i))
	}
	waitFor(t, "3 events pending", func() bool { evts, _ := ep.PendingRoomEvents(); return evts == 3 })
	as.Events <- textEvent("!free:example.com", "$free0")
	as.Events <- textEvent("!free:example.com", "$free1")
	time.Sleep(200 * time.Millisecond)
	assert.Empty(t, log.room("!free:example.com"), "an event past the limit was handled")
	evts, _ := ep.PendingRoomEvents()
	assert.Equal(t, 3, evts)
	assert.NotEmpty(t, as.Events, "the loop kept reading the Events channel past the limit")

	releaseOnce.Do(func() { close(release) })
	waitFor(t, "all handled", func() bool { return log.count() == 5 })
	assert.Equal(t, []string{"$stuck0", "$stuck1", "$stuck2"}, log.room("!stuck:example.com"))
	assert.Equal(t, []string{"$free0", "$free1"}, log.room("!free:example.com"))
}

// The bridge's encrypted-event handler decrypts and then calls Dispatch again with the decrypted event (postDecrypt).
// In SyncPerRoom that nested Dispatch must run inline like in Sync, so a decrypted message is handed on before the
// room's next event, and the order of a room's encrypted messages is kept.
func TestSyncPerRoom_NestedDispatchIsInline(t *testing.T) {
	as, ep := newPerRoomTestProcessor(t, SyncPerRoom, 0)
	var log handledLog
	ep.On(event.EventEncrypted, func(ctx context.Context, evt *event.Event) {
		// decryption takes longer for the first event of a megolm session
		if strings.HasSuffix(evt.ID.String(), "-0") {
			time.Sleep(20 * time.Millisecond)
		}
		ep.Dispatch(ctx, textEvent(evt.RoomID, evt.ID.String()+"-decrypted"))
	})
	ep.On(event.EventMessage, func(ctx context.Context, evt *event.Event) { log.add(evt) })
	ep.Start(context.Background())

	for i := 0; i < 5; i++ {
		as.Events <- &event.Event{Type: event.EventEncrypted, ID: id.EventID(fmt.Sprintf("$e-%d", i)), RoomID: "!r:example.com"}
	}
	waitFor(t, "all handled", func() bool { return log.count() == 5 })
	assert.Equal(t, []string{"$e-0-decrypted", "$e-1-decrypted", "$e-2-decrypted", "$e-3-decrypted", "$e-4-decrypted"}, log.room("!r:example.com"))
}

// End to end through the appservice HTTP handler: the transactions Tuwunel sends for two users' chats, the first of
// which is stuck. The second user's messages are handled in order without waiting, and the stuck room's receipt
// stays behind its message.
func TestSyncPerRoom_TransactionsThroughRouter(t *testing.T) {
	as, ep := newPerRoomTestProcessor(t, SyncPerRoom, 0)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var log handledLog
	handler := func(ctx context.Context, evt *event.Event) {
		if evt.RoomID == "!a:example.com" && evt.Type == event.EventMessage {
			<-release
		}
		if evt.ID == "" {
			evt.ID = id.EventID(evt.Type.Type)
		}
		log.add(evt)
	}
	ep.On(event.EventMessage, handler)
	ep.On(event.EphemeralEventReceipt, handler)
	ep.Start(context.Background())

	putTransaction(t, as, "t1", `{"events": [
		{"type": "m.room.message", "event_id": "$a1", "room_id": "!a:example.com", "sender": "@alice:example.com", "origin_server_ts": 1, "content": {"msgtype": "m.text", "body": "a1"}},
		{"type": "m.room.message", "event_id": "$b1", "room_id": "!b:example.com", "sender": "@bob:example.com", "origin_server_ts": 2, "content": {"msgtype": "m.text", "body": "b1"}}
	]}`)
	// The appservice queues a transaction's ephemeral events before its timeline events, so the receipt for $a1
	// comes in the next transaction, as it does from Tuwunel.
	putTransaction(t, as, "t2", `{"events": [
		{"type": "m.room.message", "event_id": "$b2", "room_id": "!b:example.com", "sender": "@bob:example.com", "origin_server_ts": 4, "content": {"msgtype": "m.text", "body": "b2"}},
		{"type": "m.room.message", "event_id": "$b3", "room_id": "!b:example.com", "sender": "@bob:example.com", "origin_server_ts": 5, "content": {"msgtype": "m.text", "body": "b3"}}
	], "ephemeral": [
		{"type": "m.receipt", "room_id": "!a:example.com", "content": {"$a1": {"m.read": {"@alice:example.com": {"ts": 3}}}}}
	]}`)
	waitFor(t, "room b handled", func() bool { return len(log.room("!b:example.com")) == 3 })
	assert.Equal(t, []string{"$b1", "$b2", "$b3"}, log.room("!b:example.com"))
	assert.Empty(t, log.room("!a:example.com"))

	releaseOnce.Do(func() { close(release) })
	waitFor(t, "room a handled", func() bool { return len(log.room("!a:example.com")) == 2 })
	assert.Equal(t, []string{"$a1", "m.receipt"}, log.room("!a:example.com"))
}

// Stop ends the loop: events sent afterwards are not handled.
func TestSyncPerRoom_Stop(t *testing.T) {
	as := newQueueFilterTestAppService(t)
	ep := NewEventProcessor(as)
	ep.ExecMode = SyncPerRoom
	var log handledLog
	ep.On(event.EventMessage, func(ctx context.Context, evt *event.Event) { log.add(evt) })
	ep.Start(context.Background())
	as.Events <- textEvent("!r:example.com", "$before")
	waitFor(t, "handled before stop", func() bool { return log.count() == 1 })
	ep.Stop()
	time.Sleep(50 * time.Millisecond)
	select {
	case as.Events <- textEvent("!r:example.com", "$after"):
	default:
	}
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, []string{"$before"}, log.room("!r:example.com"))
}
