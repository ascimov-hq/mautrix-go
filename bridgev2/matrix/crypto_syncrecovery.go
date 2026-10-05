// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build cgo && !nocrypto

package matrix

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/id"
)

// intbox: recovery for a bridge bot to-device /sync that is stuck on one since token (load-test P1).
//
// After a large import the bot's /sync over the token range it had missed never finished: every attempt with the
// same since token ran into the client timeout (Tuwunel 1.8.0 answered a direct probe with 408 after 255 s), and the
// crypto syncer retried the same token every 10 s forever. Tuwunel's cost is per joined room (load_joined_room
// runs for each of the bot's 6,378 rooms) times the token range, even though the crypto filter drops every room
// event. No to-device events were processed until the token was reset by hand: app devices' key requests went
// unanswered and new devices never got room keys.
//
// After cryptoSyncStuckThreshold timeouts in a row, the syncer makes one sync from the same token with every room
// excluded ("rooms": []). Tuwunel (1.8.0 and 1.8.3) applies the room filter before its per-room work, so that sync
// is cheap; it still returns every to-device event after the token, the bot's own device list changes and the
// one-time key counts. Room members' device list changes in the range come from /keys/changes, which only scans
// the per-room key change index. The response is then processed like a normal sync and the token moves forward, so
// nothing is skipped (the manual reset in the load test did skip the to-device events in between).
//
// encryption.appservice (MSC4203 to-device in transactions, Phase 2) removes the bot /sync altogether; this only
// matters while a bridge still runs the syncer.

// cryptoSyncStuckThreshold is the number of consecutive timed-out /sync attempts after which the crypto syncer
// stops retrying the same request and recovers with a narrowed sync.
const cryptoSyncStuckThreshold = 3

// cryptoSyncRetryDelay is the wait between failed /sync attempts (upstream's fixed 10 s; a variable for tests).
var cryptoSyncRetryDelay = 10 * time.Second

// cryptoSyncRecoveryTimeout bounds the whole recovery (narrowed sync and /keys/changes).
var cryptoSyncRecoveryTimeout = 3 * time.Minute

var errCryptoSyncStuck = errors.New("bridge bot /sync timed out repeatedly with the same since token")

// narrowedCryptoSyncFilter is the crypto syncer's filter (see cryptoSyncer.GetFilterJSON) with every room excluded.
// It is passed inline because mautrix.RoomFilter.Rooms is omitempty and can't express an empty list.
const narrowedCryptoSyncFilter = `{"presence":{"not_types":["*"]},"account_data":{"not_types":["*"]},` +
	`"room":{"rooms":[],"include_leave":false,"ephemeral":{"not_types":["*"]},"account_data":{"not_types":["*"]},` +
	`"state":{"not_types":["*"]},"timeline":{"not_types":["*"]}}}`

// isSyncTimeout reports whether a failed /sync timed out, on the client or on the server.
func isSyncTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var httpErr mautrix.HTTPError
	if errors.As(err, &httpErr) && httpErr.Response != nil {
		switch httpErr.Response.StatusCode {
		case http.StatusRequestTimeout, http.StatusGatewayTimeout:
			return true
		}
	}
	return false
}

// recoverStuckSync makes one narrowed sync from the stored token, adds the device list changes of the range from
// /keys/changes, saves the new token and processes the response through the normal syncer. On any error the token
// is left unchanged, so nothing is lost, and the normal sync loop resumes.
func (helper *CryptoHelper) recoverStuckSync(parentCtx context.Context) error {
	ctx, cancel := context.WithTimeout(parentCtx, cryptoSyncRecoveryTimeout)
	defer cancel()
	cli := helper.client
	since, err := cli.Store.LoadNextBatch(ctx, cli.UserID)
	if err != nil {
		return fmt.Errorf("failed to load sync token: %w", err)
	}
	start := time.Now()
	resp, err := cli.FullSyncRequest(ctx, mautrix.ReqSync{
		Timeout:  0,
		Since:    since,
		FilterID: narrowedCryptoSyncFilter,
	})
	if err != nil {
		return fmt.Errorf("narrowed sync failed: %w", err)
	} else if resp.NextBatch == "" {
		return fmt.Errorf("narrowed sync returned no next_batch")
	}
	syncDuration := time.Since(start)
	var keyChanges *mautrix.RespKeyChanges
	if since != "" {
		keyChanges, err = cli.GetKeyChanges(ctx, since, resp.NextBatch)
		if err != nil {
			return fmt.Errorf("failed to get device list changes between %s and %s: %w", since, resp.NextBatch, err)
		}
		resp.DeviceLists.Changed = mergeUserIDs(resp.DeviceLists.Changed, keyChanges.Changed)
		resp.DeviceLists.Left = mergeUserIDs(resp.DeviceLists.Left, keyChanges.Left)
	}
	if err = cli.Store.SaveNextBatch(ctx, cli.UserID, resp.NextBatch); err != nil {
		return fmt.Errorf("failed to save sync token: %w", err)
	}
	helper.log.Warn().
		Str("since", since).
		Str("next_batch", resp.NextBatch).
		Dur("narrowed_sync_duration", syncDuration).
		Int("to_device_events", len(resp.ToDevice.Events)).
		Int("device_list_changes", len(resp.DeviceLists.Changed)).
		Msg("Recovered stuck bridge bot /sync with a narrowed sync, continuing from the new token")
	// Processing runs on the parent context: ProcessResponse may leave it running in the background.
	return cli.Syncer.ProcessResponse(parentCtx, resp, since)
}

func mergeUserIDs(a, b []id.UserID) []id.UserID {
	if len(b) == 0 {
		return a
	}
	merged := append(slices.Clone(a), b...)
	slices.Sort(merged)
	return slices.Compact(merged)
}
