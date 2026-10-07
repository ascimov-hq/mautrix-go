// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package bridgev2

import (
	"container/list"

	"maunium.net/go/mautrix/bridgev2/networkid"
)

// MatrixConnectorWithGhostEviction is an optional interface for Matrix connectors that cache per-ghost
// objects (e.g. appservice intents and HTTP clients). It's called when a ghost is evicted from the
// bridge's ghost cache so that the connector can drop its own cached objects for that ghost too.
type MatrixConnectorWithGhostEviction interface {
	MatrixConnector
	ForgetGhost(userID networkid.UserID)
}

// ghostCache is the in-memory ghost cache of the bridge. If maxSize is positive, the least recently
// used ghosts are evicted once the cache is full. Otherwise, the cache grows without bounds like a plain map.
//
// All methods must be called while holding the bridge's cacheLock.
type ghostCache struct {
	maxSize int
	items   map[networkid.UserID]*list.Element
	order   *list.List
	onEvict func(*Ghost)
}

func newGhostCache(maxSize int, onEvict func(*Ghost)) *ghostCache {
	return &ghostCache{
		maxSize: maxSize,
		items:   make(map[networkid.UserID]*list.Element),
		order:   list.New(),
		onEvict: onEvict,
	}
}

func (gc *ghostCache) Get(id networkid.UserID) (*Ghost, bool) {
	elem, ok := gc.items[id]
	if !ok {
		return nil, false
	}
	if gc.maxSize > 0 {
		gc.order.MoveToFront(elem)
	}
	return elem.Value.(*Ghost), true
}

func (gc *ghostCache) Put(ghost *Ghost) {
	if elem, ok := gc.items[ghost.ID]; ok {
		elem.Value = ghost
		gc.order.MoveToFront(elem)
		return
	}
	gc.items[ghost.ID] = gc.order.PushFront(ghost)
	for gc.maxSize > 0 && gc.order.Len() > gc.maxSize {
		oldest := gc.order.Back()
		gc.order.Remove(oldest)
		evicted := oldest.Value.(*Ghost)
		delete(gc.items, evicted.ID)
		if gc.onEvict != nil {
			gc.onEvict(evicted)
		}
	}
}

func (gc *ghostCache) Len() int {
	return len(gc.items)
}
