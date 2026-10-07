// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"maunium.net/go/mautrix/event"
)

// PendingEvents is what a bridge stop waits for (load-test L6-2): events whose transaction was answered but that
// weren't handled yet must be counted wherever they are (channel, event loop, room queue, running handler), and
// the count must go back to 0 once every handler has returned.
func TestPendingEvents_CountsUntilHandled(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode ExecMode
	}{
		{"Sync", Sync},
		{"SyncPerRoom", SyncPerRoom},
	} {
		t.Run(tc.name, func(t *testing.T) {
			as, ep := newPerRoomTestProcessor(t, tc.mode, 0)
			release := make(chan struct{})
			entered := make(chan struct{}, 8)
			var log handledLog
			ep.On(event.EventMessage, func(ctx context.Context, evt *event.Event) {
				entered <- struct{}{}
				<-release
				log.add(evt)
			})
			// Accepted before the processor runs (a transaction answered right before the loop starts).
			as.Events <- textEvent("!a:example.com", "$a1")
			as.Events <- textEvent("!a:example.com", "$a2")
			as.Events <- textEvent("!b:example.com", "$b1")
			assert.Equal(t, 3, ep.PendingEvents(), "events in the channel before Start")

			ep.Start(context.Background())
			<-entered
			// Sync: $a1 being handled, $a2 and $b1 in the channel. SyncPerRoom: $a1 and $b1 being handled, $a2 queued.
			if tc.mode == SyncPerRoom {
				<-entered // $b1 runs in parallel with $a1
				waitFor(t, "events moved to the room queues", func() bool { return len(as.Events) == 0 })
			}
			waitFor(t, "3 pending", func() bool { return ep.PendingEvents() == 3 })
			assert.Zero(t, log.count(), "nothing handled yet")

			close(release)
			waitFor(t, "all events handled", func() bool { return log.count() == 3 })
			waitFor(t, "pending back to 0", func() bool { return ep.PendingEvents() == 0 })
		})
	}
}
