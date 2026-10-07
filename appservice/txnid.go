// Copyright (c) 2021 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appservice

import "sync"

type TransactionIDCache struct {
	array    []string
	arrayPtr int
	hash     map[string]struct{}
	// inFlight contains transactions that are currently being processed.
	// The channel is closed when processing finishes.
	inFlight map[string]chan struct{}
	lock     sync.RWMutex
}

func NewTransactionIDCache(size int) *TransactionIDCache {
	return &TransactionIDCache{
		array:    make([]string, size),
		hash:     make(map[string]struct{}),
		inFlight: make(map[string]chan struct{}),
	}
}

func (txnIDC *TransactionIDCache) IsProcessed(txnID string) bool {
	txnIDC.lock.RLock()
	_, exists := txnIDC.hash[txnID]
	txnIDC.lock.RUnlock()
	return exists
}

// StartProcessing marks the given transaction as being processed.
//
// If the transaction has already been processed, alreadyProcessed is true. If another call is still
// processing the same transaction, the returned channel is closed once that call finishes. Otherwise,
// the caller is now responsible for processing the transaction and must call FinishProcessing afterwards.
func (txnIDC *TransactionIDCache) StartProcessing(txnID string) (wait <-chan struct{}, alreadyProcessed bool) {
	txnIDC.lock.Lock()
	defer txnIDC.lock.Unlock()
	if _, exists := txnIDC.hash[txnID]; exists {
		return nil, true
	} else if ch, ok := txnIDC.inFlight[txnID]; ok {
		return ch, false
	}
	txnIDC.inFlight[txnID] = make(chan struct{})
	return nil, false
}

// FinishProcessing wakes up anyone waiting for the given transaction in StartProcessing.
// It doesn't mark the transaction as processed, MarkProcessed must be called separately.
func (txnIDC *TransactionIDCache) FinishProcessing(txnID string) {
	txnIDC.lock.Lock()
	if ch, ok := txnIDC.inFlight[txnID]; ok {
		close(ch)
		delete(txnIDC.inFlight, txnID)
	}
	txnIDC.lock.Unlock()
}

func (txnIDC *TransactionIDCache) MarkProcessed(txnID string) {
	txnIDC.lock.Lock()
	defer txnIDC.lock.Unlock()
	if _, exists := txnIDC.hash[txnID]; exists {
		return
	}
	txnIDC.hash[txnID] = struct{}{}
	if txnIDC.array[txnIDC.arrayPtr] != "" {
		// The ring buffer is full: forget the oldest 1/8 of the entries (at least the one being overwritten).
		for i := 0; i < max(len(txnIDC.array)/8, 1) && txnIDC.arrayPtr+i < len(txnIDC.array); i++ {
			delete(txnIDC.hash, txnIDC.array[txnIDC.arrayPtr+i])
			txnIDC.array[txnIDC.arrayPtr+i] = ""
		}
	}
	txnIDC.array[txnIDC.arrayPtr] = txnID
	// Previously the pointer was never advanced, so only the latest transaction ID was remembered.
	txnIDC.arrayPtr = (txnIDC.arrayPtr + 1) % len(txnIDC.array)
}
