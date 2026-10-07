// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appservice

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransactionIDCache_RemembersRecentTransactions(t *testing.T) {
	cache := NewTransactionIDCache(16)
	for i := 0; i < 100; i++ {
		cache.MarkProcessed(fmt.Sprintf("txn%d", i))
	}
	// The newest entries must always be remembered
	for i := 90; i < 100; i++ {
		assert.True(t, cache.IsProcessed(fmt.Sprintf("txn%d", i)), "txn%d", i)
	}
	assert.False(t, cache.IsProcessed("txn0"))
	assert.LessOrEqual(t, len(cache.hash), 16)
}

func TestTransactionIDCache_InFlight(t *testing.T) {
	cache := NewTransactionIDCache(16)
	wait, processed := cache.StartProcessing("txn1")
	assert.Nil(t, wait)
	assert.False(t, processed)

	// A retry while the first attempt is still running must wait
	wait, processed = cache.StartProcessing("txn1")
	assert.NotNil(t, wait)
	assert.False(t, processed)
	select {
	case <-wait:
		t.Fatal("wait channel closed before processing finished")
	default:
	}

	cache.MarkProcessed("txn1")
	cache.FinishProcessing("txn1")
	select {
	case <-wait:
	default:
		t.Fatal("wait channel not closed after processing finished")
	}
	wait, processed = cache.StartProcessing("txn1")
	assert.Nil(t, wait)
	assert.True(t, processed)

	// If the first attempt finishes without marking the transaction as processed, the next attempt takes over
	wait, _ = cache.StartProcessing("txn2")
	assert.Nil(t, wait)
	cache.FinishProcessing("txn2")
	wait, processed = cache.StartProcessing("txn2")
	assert.Nil(t, wait)
	assert.False(t, processed)
}
