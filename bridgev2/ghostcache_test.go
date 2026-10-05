// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package bridgev2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
)

func makeTestGhost(id networkid.UserID) *Ghost {
	return &Ghost{Ghost: &database.Ghost{ID: id}}
}

func TestGhostCache_LRU(t *testing.T) {
	var evicted []networkid.UserID
	cache := newGhostCache(2, func(ghost *Ghost) {
		evicted = append(evicted, ghost.ID)
	})
	cache.Put(makeTestGhost("a"))
	cache.Put(makeTestGhost("b"))
	_, ok := cache.Get("a")
	assert.True(t, ok)
	// b is now the least recently used ghost
	cache.Put(makeTestGhost("c"))
	assert.Equal(t, []networkid.UserID{"b"}, evicted)
	_, ok = cache.Get("b")
	assert.False(t, ok)
	_, ok = cache.Get("a")
	assert.True(t, ok)
	_, ok = cache.Get("c")
	assert.True(t, ok)
	assert.Equal(t, 2, cache.Len())
}

func TestGhostCache_Unbounded(t *testing.T) {
	cache := newGhostCache(0, func(ghost *Ghost) {
		t.Fatalf("unexpected eviction of %s", ghost.ID)
	})
	for _, id := range []networkid.UserID{"a", "b", "c", "d"} {
		cache.Put(makeTestGhost(id))
	}
	assert.Equal(t, 4, cache.Len())
}
