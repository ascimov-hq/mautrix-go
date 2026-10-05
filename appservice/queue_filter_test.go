// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appservice

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// An import-sized transaction as Tuwunel sends it to a bridge: mostly echoes of the bridge's own ghost messages,
// with a user's message, a ghost's membership change, a read receipt and a to-device event in between.
const echoHeavyTransaction = `{
	"events": [
		{"type": "m.room.message", "event_id": "$echo1", "room_id": "!r:example.com", "sender": "@whatsapp_1:example.com", "origin_server_ts": 1, "content": {"msgtype": "m.text", "body": "from WhatsApp"}},
		{"type": "m.room.encrypted", "event_id": "$echo2", "room_id": "!r:example.com", "sender": "@whatsapp_1:example.com", "origin_server_ts": 2, "content": {"algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "x", "session_id": "s"}},
		{"type": "m.room.message", "event_id": "$user", "room_id": "!r:example.com", "sender": "@user:example.com", "origin_server_ts": 3, "content": {"msgtype": "m.text", "body": "from the app"}},
		{"type": "m.room.member", "event_id": "$join", "room_id": "!r:example.com", "sender": "@whatsapp_1:example.com", "state_key": "@whatsapp_1:example.com", "origin_server_ts": 4, "content": {"membership": "join"}},
		{"type": "m.reaction", "event_id": "$echo3", "room_id": "!r:example.com", "sender": "@whatsappbot:example.com", "origin_server_ts": 5, "content": {"m.relates_to": {"rel_type": "m.annotation", "event_id": "$user", "key": "x"}}}
	],
	"ephemeral": [
		{"type": "m.receipt", "room_id": "!r:example.com", "content": {"$echo1": {"m.read": {"@whatsapp_1:example.com": {"ts": 6}}}}}
	],
	"to_device": [
		{"type": "m.room_key_request", "sender": "@whatsapp_1:example.com", "to_user_id": "@whatsappbot:example.com", "to_device_id": "BOT", "content": {"action": "request_cancellation", "request_id": "1", "requesting_device_id": "D"}}
	]
}`

func newQueueFilterTestAppService(t *testing.T) *AppService {
	as := Create()
	as.Registration = &Registration{ServerToken: "hs_token", AppToken: "as_token", SenderLocalpart: "whatsappbot", EphemeralEvents: true}
	return as
}

func putTransaction(t *testing.T, as *AppService, txnID, body string) {
	req := httptest.NewRequest(http.MethodPut, "/_matrix/app/v1/transactions/"+txnID, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer hs_token")
	rec := httptest.NewRecorder()
	as.Router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func drainEvents(ch chan *event.Event) (ids []string) {
	for {
		select {
		case evt := <-ch:
			if evt.ID != "" {
				ids = append(ids, evt.ID.String())
			} else {
				ids = append(ids, evt.Type.Type)
			}
		default:
			return
		}
	}
}

func TestShouldQueueEvent_DropsOnlyRejectedTimelineEvents(t *testing.T) {
	as := newQueueFilterTestAppService(t)
	var asked []string
	isOwn := func(userID id.UserID) bool {
		return strings.HasPrefix(userID.String(), "@whatsapp_") || userID == "@whatsappbot:example.com"
	}
	as.ShouldQueueEvent = func(evt *event.Event) bool {
		asked = append(asked, evt.Type.Type)
		return evt.Type.IsState() || evt.Type.Class == event.EphemeralEventType || !isOwn(evt.Sender)
	}
	putTransaction(t, as, "1", echoHeavyTransaction)

	assert.Equal(t, []string{"m.receipt", "$user", "$join"}, drainEvents(as.Events),
		"echoes of the bridge's own messages, encrypted messages and reactions are dropped; the user's message, the ghost's membership and the receipt are queued in order")
	assert.Equal(t, []string{"m.room_key_request"}, drainEvents(as.ToDeviceEvents), "to-device events are never filtered")
	assert.NotContains(t, asked, "m.room_key_request", "the filter isn't asked about to-device events")

	// The state store is updated before the filter runs, even for events that are dropped later.
	assert.True(t, as.StateStore.IsInRoom(t.Context(), "!r:example.com", "@whatsapp_1:example.com"))
}

func TestShouldQueueEvent_NilQueuesEverything(t *testing.T) {
	as := newQueueFilterTestAppService(t)
	putTransaction(t, as, "1", echoHeavyTransaction)
	assert.Equal(t, []string{"m.receipt", "$echo1", "$echo2", "$user", "$join", "$echo3"}, drainEvents(as.Events))
	assert.Len(t, drainEvents(as.ToDeviceEvents), 1)
}

// With synchronous handling the transaction response waits until every event is in the channel. 200 queued
// events used to block the response at the 65th event until the consumer caught up; with 1024 slots it returns
// at once.
func TestEventChannelSize_LargeTransactionDoesNotBlock(t *testing.T) {
	assert.Equal(t, 1024, EventChannelSize)
	as := newQueueFilterTestAppService(t)
	require.Equal(t, 1024, cap(as.Events))
	var sb strings.Builder
	sb.WriteString(`{"events": [`)
	for i := 0; i < 200; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`{"type": "m.room.message", "event_id": "$e`)
		sb.WriteString(strings.Repeat("x", i%7))
		sb.WriteString(`", "room_id": "!r:example.com", "sender": "@user:example.com", "origin_server_ts": 1, "content": {"msgtype": "m.text", "body": "hi"}}`)
	}
	sb.WriteString(`]}`)
	done := make(chan int)
	go func() {
		req := httptest.NewRequest(http.MethodPut, "/_matrix/app/v1/transactions/big", strings.NewReader(sb.String()))
		req.Header.Set("Authorization", "Bearer hs_token")
		rec := httptest.NewRecorder()
		as.Router.ServeHTTP(rec, req)
		done <- rec.Code
	}()
	select {
	case code := <-done:
		assert.Equal(t, http.StatusOK, code)
	case <-time.After(5 * time.Second):
		t.Fatal("transaction with 200 events blocked with nobody consuming the channel")
	}
	assert.Len(t, as.Events, 200)
}
