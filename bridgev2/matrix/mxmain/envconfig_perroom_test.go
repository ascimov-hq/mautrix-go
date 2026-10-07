// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mxmain

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"maunium.net/go/mautrix/bridgev2/matrix"
)

func TestLoadGlobalConfigEnv_PerRoomDispatch(t *testing.T) {
	orig := matrix.PerRoomEventDispatch
	t.Cleanup(func() { matrix.PerRoomEventDispatch = orig })

	matrix.PerRoomEventDispatch = true
	t.Setenv("MAUTRIX_PER_ROOM_DISPATCH", "")
	LoadGlobalConfigEnv()
	assert.True(t, matrix.PerRoomEventDispatch, "on by default")

	t.Setenv("MAUTRIX_PER_ROOM_DISPATCH", "false")
	LoadGlobalConfigEnv()
	assert.False(t, matrix.PerRoomEventDispatch)

	t.Setenv("MAUTRIX_PER_ROOM_DISPATCH", "true")
	LoadGlobalConfigEnv()
	assert.True(t, matrix.PerRoomEventDispatch)
}
