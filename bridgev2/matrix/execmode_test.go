// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package matrix

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"maunium.net/go/mautrix/appservice"
)

func TestEventExecMode(t *testing.T) {
	orig := PerRoomEventDispatch
	t.Cleanup(func() { PerRoomEventDispatch = orig })

	PerRoomEventDispatch = true
	assert.Equal(t, appservice.SyncPerRoom, eventExecMode(false), "async_transactions: false (deployed) uses per-room order")
	assert.Equal(t, appservice.AsyncHandlers, eventExecMode(true), "async_transactions: true is unchanged")

	PerRoomEventDispatch = false // MAUTRIX_PER_ROOM_DISPATCH=false
	assert.Equal(t, appservice.Sync, eventExecMode(false), "opt-out gives the upstream single loop")
	assert.Equal(t, appservice.AsyncHandlers, eventExecMode(true))
}
