// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build cgo && !nocrypto

package matrix

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

const testBotUserID = id.UserID("@whatsappbot:example.com")

type noEncryptionStateStore struct{}

func (noEncryptionStateStore) IsEncrypted(context.Context, id.RoomID) (bool, error) {
	return false, nil
}
func (noEncryptionStateStore) GetEncryptionEvent(context.Context, id.RoomID) (*event.EncryptionEventContent, error) {
	return nil, nil
}
func (noEncryptionStateStore) FindSharedRooms(context.Context, id.UserID) ([]id.RoomID, error) {
	return nil, nil
}

// fakeBotHomeserver plays Tuwunel for the bridge bot's crypto /sync. Each /sync attempt gets the next scripted
// reaction; "hang" holds the request until the client gives up, like Tuwunel walking 6,378 rooms over a long
// token range.
type fakeBotHomeserver struct {
	t      *testing.T
	script []string

	lock             sync.Mutex
	syncs            []hsRequest
	keyChanges       []hsRequest
	keyChangesStatus int
}

type hsRequest struct {
	since, filter, timeout, from, to string
}

func (hs *fakeBotHomeserver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	switch {
	case strings.HasSuffix(r.URL.Path, "/filter"):
		_, _ = w.Write([]byte(`{"filter_id": "f1"}`))
	case strings.HasSuffix(r.URL.Path, "/keys/upload"):
		_, _ = w.Write([]byte(`{"one_time_key_counts": {"signed_curve25519": 50}}`))
	case strings.HasSuffix(r.URL.Path, "/keys/changes"):
		hs.lock.Lock()
		hs.keyChanges = append(hs.keyChanges, hsRequest{from: q.Get("from"), to: q.Get("to")})
		status := hs.keyChangesStatus
		hs.lock.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"errcode": "M_INVALID_PARAM", "error": "Invalid from"}`))
			return
		}
		_, _ = w.Write([]byte(`{"changed": ["@app2:example.com", "@app1:example.com"], "left": ["@gone:example.com"]}`))
	case strings.HasSuffix(r.URL.Path, "/sync"):
		hs.lock.Lock()
		n := len(hs.syncs)
		hs.syncs = append(hs.syncs, hsRequest{since: q.Get("since"), filter: q.Get("filter"), timeout: q.Get("timeout")})
		hs.lock.Unlock()
		action := "hang"
		if n < len(hs.script) {
			action = hs.script[n]
		}
		if strings.Contains(q.Get("filter"), `"rooms":[]`) {
			action = "narrowed"
		}
		switch action {
		case "hang":
			<-r.Context().Done()
		case "502":
			w.WriteHeader(http.StatusBadGateway)
		case "408":
			w.WriteHeader(http.StatusRequestTimeout)
			_, _ = w.Write([]byte(`{"errcode": "M_UNKNOWN", "error": "Request timed out"}`))
		case "ok":
			_, _ = w.Write([]byte(`{"next_batch": "10338638", "device_one_time_keys_count": {"signed_curve25519": 50}}`))
		case "narrowed":
			_, _ = fmt.Fprintf(w, `{"next_batch": "10530891", "to_device": {"events": [
				{"type": "m.room_key_request", "sender": "@app1:example.com", "content": {"action": "request_cancellation", "request_id": "r1", "requesting_device_id": "APPDEV"}}
			]}, "device_lists": {"changed": ["@app1:example.com", "%s"]}, "device_one_time_keys_count": {"signed_curve25519": 50}}`, testBotUserID)
		}
	default:
		hs.t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}
}

func (hs *fakeBotHomeserver) syncSinces() (sinces []string) {
	hs.lock.Lock()
	defer hs.lock.Unlock()
	for _, s := range hs.syncs {
		sinces = append(sinces, s.since)
	}
	return
}

func newFakeBotClient(t *testing.T, hs *fakeBotHomeserver, since string) *mautrix.Client {
	srv := httptest.NewServer(hs)
	t.Cleanup(srv.Close)
	cli, err := mautrix.NewClient(srv.URL, testBotUserID, "as_token")
	require.NoError(t, err)
	cli.DeviceID = "BOTDEVICE"
	cli.Client = &http.Client{Timeout: 300 * time.Millisecond}
	cli.Store = mautrix.NewMemorySyncStore()
	require.NoError(t, cli.Store.SaveNextBatch(context.Background(), testBotUserID, since))
	return cli
}

func setFastRetry(t *testing.T) {
	orig := cryptoSyncRetryDelay
	cryptoSyncRetryDelay = 10 * time.Millisecond
	t.Cleanup(func() { cryptoSyncRetryDelay = orig })
}

func TestIsSyncTimeout(t *testing.T) {
	hs := &fakeBotHomeserver{t: t, script: []string{"hang", "408", "502"}}
	cli := newFakeBotClient(t, hs, "1")
	ctx := context.Background()
	_, clientTimeout := cli.FullSyncRequest(ctx, mautrix.ReqSync{Since: "1"})
	_, serverTimeout := cli.FullSyncRequest(ctx, mautrix.ReqSync{Since: "1"})
	_, badGateway := cli.FullSyncRequest(ctx, mautrix.ReqSync{Since: "1"})
	require.Error(t, clientTimeout)
	assert.Contains(t, clientTimeout.Error(), "Client.Timeout exceeded", "the error the bridge logged in the load test")
	assert.True(t, isSyncTimeout(clientTimeout), "client timeout: %v", clientTimeout)
	assert.True(t, isSyncTimeout(serverTimeout), "HTTP 408 from Tuwunel: %v", serverTimeout)
	assert.False(t, isSyncTimeout(badGateway), "HTTP 502 is not a timeout: %v", badGateway)
	assert.True(t, isSyncTimeout(fmt.Errorf("wrapped: %w", context.DeadlineExceeded)))
	assert.False(t, isSyncTimeout(context.Canceled))
	assert.False(t, isSyncTimeout(mautrix.MUnknownToken))
}

func newTestCryptoSyncer(t *testing.T, cli *mautrix.Client) *cryptoSyncer {
	log := zerolog.Nop()
	mach := crypto.NewOlmMachine(cli, &log, crypto.NewMemoryStore(nil), noEncryptionStateStore{})
	require.NoError(t, mach.Load(context.Background()))
	return &cryptoSyncer{OlmMachine: mach}
}

// The load-test failure: every attempt with the same token times out. Upstream retried it forever; now the sync
// loop ends with errCryptoSyncStuck after cryptoSyncStuckThreshold timeouts in a row. Other errors (502) don't
// count, and a successful sync starts the count again.
func TestCryptoSyncer_RepeatedTimeoutsEndTheSyncLoop(t *testing.T) {
	setFastRetry(t)
	hs := &fakeBotHomeserver{t: t, script: []string{"hang", "hang", "ok", "hang", "408", "502", "hang"}}
	cli := newFakeBotClient(t, hs, "3579192")
	cli.Syncer = newTestCryptoSyncer(t, cli)

	done := make(chan error, 1)
	go func() { done <- cli.SyncWithContext(context.Background()) }()
	var err error
	select {
	case err = <-done:
	case <-time.After(20 * time.Second):
		cli.StopSync()
		t.Fatalf("sync loop kept retrying: %v", hs.syncSinces())
	}
	require.ErrorIs(t, err, errCryptoSyncStuck)
	assert.Contains(t, err.Error(), "Client.Timeout exceeded")
	assert.Equal(t, []string{"3579192", "3579192", "3579192", "10338638", "10338638", "10338638", "10338638"}, hs.syncSinces(),
		"2 timeouts, success, then timeout + 408 + 502 (not counted) + timeout = 3 timeouts with the new token")
}

// recordingSyncer stands in for cryptoSyncer to see what the recovery hands to the normal sync processing.
type recordingSyncer struct {
	mautrix.Syncer
	lock  sync.Mutex
	resps []*mautrix.RespSync
	since []string
}

func (s *recordingSyncer) ProcessResponse(_ context.Context, resp *mautrix.RespSync, since string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.resps = append(s.resps, resp)
	s.since = append(s.since, since)
	return nil
}

func newTestHelper(cli *mautrix.Client) *CryptoHelper {
	log := zerolog.Nop()
	return &CryptoHelper{client: cli, log: &log}
}

func TestRecoverStuckSync_NarrowedSyncKeepsToDeviceAndDeviceLists(t *testing.T) {
	hs := &fakeBotHomeserver{t: t}
	cli := newFakeBotClient(t, hs, "3579192")
	rec := &recordingSyncer{}
	cli.Syncer = rec

	require.NoError(t, newTestHelper(cli).recoverStuckSync(context.Background()))

	require.Len(t, hs.syncs, 1)
	assert.Equal(t, "3579192", hs.syncs[0].since, "same token: nothing after it is skipped")
	assert.Equal(t, "0", hs.syncs[0].timeout)
	var filter map[string]any
	require.NoError(t, json.Unmarshal([]byte(hs.syncs[0].filter), &filter), "inline filter must be valid JSON")
	room := filter["room"].(map[string]any)
	assert.Equal(t, []any{}, room["rooms"], "every room excluded, so Tuwunel skips its per-room work")
	for _, key := range []string{"state", "timeline", "ephemeral", "account_data"} {
		assert.Equal(t, map[string]any{"not_types": []any{"*"}}, room[key], key)
	}
	assert.Equal(t, []hsRequest{{from: "3579192", to: "10530891"}}, hs.keyChanges, "room members' device list changes over the skipped range")

	next, err := cli.Store.LoadNextBatch(context.Background(), testBotUserID)
	require.NoError(t, err)
	assert.Equal(t, "10530891", next, "the token moves forward, so the normal sync is cheap again")

	require.Len(t, rec.resps, 1)
	assert.Equal(t, "3579192", rec.since[0])
	resp := rec.resps[0]
	require.Len(t, resp.ToDevice.Events, 1, "to-device events after the token are processed, not dropped")
	assert.Equal(t, event.ToDeviceRoomKeyRequest.Type, resp.ToDevice.Events[0].Type.Type)
	assert.Equal(t, []id.UserID{"@app1:example.com", "@app2:example.com", testBotUserID}, resp.DeviceLists.Changed)
	assert.Equal(t, []id.UserID{"@gone:example.com"}, resp.DeviceLists.Left)
}

func TestRecoverStuckSync_KeepsTokenWhenKeyChangesFail(t *testing.T) {
	hs := &fakeBotHomeserver{t: t, keyChangesStatus: http.StatusBadRequest}
	cli := newFakeBotClient(t, hs, "3579192")
	rec := &recordingSyncer{}
	cli.Syncer = rec

	err := newTestHelper(cli).recoverStuckSync(context.Background())
	require.Error(t, err)
	assert.True(t, errors.Is(err, mautrix.MInvalidParam), "%v", err)
	next, err := cli.Store.LoadNextBatch(context.Background(), testBotUserID)
	require.NoError(t, err)
	assert.Equal(t, "3579192", next, "without the device list changes the token must not move (the next recovery retries)")
	assert.Empty(t, rec.resps)
}
