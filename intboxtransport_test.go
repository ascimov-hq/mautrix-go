// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mautrix

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntboxLaneTransport_PoolConfig(t *testing.T) {
	defaultTransport := http.DefaultTransport.(*http.Transport)
	defaultMaxConns := defaultTransport.MaxConnsPerHost

	tr := NewIntboxLaneTransport(nil, 5)
	assert.Equal(t, 5, tr.MaxConnsPerPool())
	assert.Equal(t, IntboxMaxPoolWait, tr.MaxPoolWait)
	seen := make(map[*http.Transport]string)
	for _, name := range []string{IntboxPoolLive, IntboxPoolBulk, IntboxPoolBackfill, IntboxPoolCheap} {
		pt := tr.Pool(name)
		require.NotNil(t, pt, name)
		assert.NotContains(t, seen, pt, "every pool has its own transport")
		seen[pt] = name
		assert.Equal(t, 5, pt.MaxConnsPerHost)
		assert.Equal(t, 5, pt.MaxIdleConnsPerHost)
		assert.Equal(t, IntboxIdleConnTimeout, pt.IdleConnTimeout)
		assert.NotNil(t, pt.DialContext, "clones of the default transport keep its dialer")
	}
	assert.Nil(t, tr.Pool("other"))
	assert.Equal(t, defaultMaxConns, defaultTransport.MaxConnsPerHost, "the base transport must not be modified")

	// A base with a shorter idle timeout keeps it. No cap: separate pools, the base's idle settings.
	base := &http.Transport{IdleConnTimeout: 10 * time.Second, MaxIdleConnsPerHost: 7}
	tr = NewIntboxLaneTransport(base, 0)
	assert.Equal(t, 0, tr.MaxConnsPerPool())
	assert.Equal(t, 0, tr.Pool(IntboxPoolBulk).MaxConnsPerHost)
	assert.Equal(t, 7, tr.Pool(IntboxPoolBulk).MaxIdleConnsPerHost)
	assert.Equal(t, 10*time.Second, tr.Pool(IntboxPoolBulk).IdleConnTimeout)
	assert.Equal(t, 0, NewIntboxLaneTransport(nil, -3).MaxConnsPerPool())
}

func TestIntboxLaneTransport_PoolFor(t *testing.T) {
	tr := NewIntboxLaneTransport(nil, 2)
	const hs = "http://hs.example:8008"
	bulkCtx := WithIntboxLane(context.Background(), IntboxLaneBulk)
	for _, tc := range []struct {
		name   string
		ctx    context.Context
		lane   string // X-Intbox-Lane header
		method string
		path   string
		want   string
	}{
		{"default lane is live", context.Background(), "", http.MethodPut, "/_matrix/client/v3/rooms/!r/send/m.room.message/1", IntboxPoolLive},
		{"context lane without header", bulkCtx, "", http.MethodPut, "/_matrix/client/v3/rooms/!r/state/m.room.member/@g:hs", IntboxPoolBulk},
		{"header wins over context (it's what the proxy sees)", bulkCtx, "live", http.MethodPut, "/_matrix/client/v3/profile/@g:hs/avatar_url", IntboxPoolLive},
		{"live read", context.Background(), "live", http.MethodGet, "/_matrix/client/v1/media/download/hs/abc", IntboxPoolLive},
		{"live registration", context.Background(), "live", http.MethodPost, "/_matrix/client/v3/register", IntboxPoolLive},
		{"bulk write", context.Background(), "bulk", http.MethodPut, "/_matrix/client/v3/profile/@g:hs/avatar_url", IntboxPoolBulk},
		{"bulk room creation", context.Background(), "bulk", http.MethodPost, "/_matrix/client/v3/createRoom", IntboxPoolBulk},
		{"bulk registration", context.Background(), "bulk", http.MethodPost, "/_matrix/client/v3/register", IntboxPoolCheap},
		{"bulk profile read", context.Background(), "bulk", http.MethodGet, "/_matrix/client/v3/profile/@g:hs/avatar_url", IntboxPoolCheap},
		{"bulk media read", context.Background(), "bulk", http.MethodGet, "/_matrix/client/v1/media/download/hs/abc", IntboxPoolCheap},
		{"backfill send", context.Background(), "backfill", http.MethodPut, "/_matrix/client/v3/rooms/!r/send/m.room.encrypted/1", IntboxPoolBackfill},
		{"backfill media upload", context.Background(), "backfill", http.MethodPost, "/_matrix/media/v3/upload", IntboxPoolBackfill},
		{"backfill read", context.Background(), "backfill", http.MethodHead, "/_matrix/client/v1/media/download/hs/abc", IntboxPoolCheap},
		{"crypto in backfill lane", context.Background(), "backfill", http.MethodPost, "/_matrix/client/v3/keys/claim", IntboxPoolLive},
		{"crypto in bulk lane", context.Background(), "bulk", http.MethodPost, "/_matrix/client/v3/keys/query", IntboxPoolLive},
		{"to-device in backfill lane", context.Background(), "backfill", http.MethodPut, "/_matrix/client/v3/sendToDevice/m.room.encrypted/1", IntboxPoolLive},
		{"unstable keys path", context.Background(), "bulk", http.MethodPost, "/_matrix/client/unstable/keys/query", IntboxPoolLive},
		{"room key backup is not crypto", context.Background(), "bulk", http.MethodPut, "/_matrix/client/v3/room_keys/keys", IntboxPoolBulk},
		{"bot sync in bulk lane", context.Background(), "bulk", http.MethodGet, "/_matrix/client/v3/sync", IntboxPoolLive},
		{"batch send in live lane", context.Background(), "live", http.MethodPost, "/_matrix/client/unstable/com.beeper.backfill/rooms/!r/batch_send", IntboxPoolBackfill},
		{"unknown lane write", context.Background(), "turbo", http.MethodPut, "/_matrix/client/v3/rooms/!r/state/m.room.member/@g:hs", IntboxPoolBulk},
		{"unknown lane read", context.Background(), "turbo", http.MethodGet, "/_matrix/client/v3/profile/@g:hs", IntboxPoolCheap},
		{"homeserver behind a path prefix", context.Background(), "backfill", http.MethodPost, "/prefix/_matrix/client/v3/keys/upload", IntboxPoolLive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(tc.ctx, tc.method, hs+tc.path, nil)
			require.NoError(t, err)
			if tc.lane != "" {
				req.Header.Set(IntboxLaneHeader, tc.lane)
			}
			assert.Equal(t, tc.want, tr.PoolFor(req))
		})
	}
}

// laneTestServer counts requests in flight per "<lane> <method> <path>" key and new TCP connections.
// Requests whose key is in blocked wait until release is closed (like requests delayed by nginx limit_req).
type laneTestServer struct {
	*httptest.Server
	blocked     map[string]bool
	release     chan struct{}
	releaseOnce sync.Once
	newConns    atomic.Int32

	lock        sync.Mutex
	inflight    map[string]int
	maxInflight map[string]int
	arrived     map[string]int
}

func requestKey(lane IntboxLane, method, endpoint string) string {
	return string(lane) + " " + method + " /_matrix/client/v3/" + endpoint
}

func newLaneTestServer(t *testing.T, blocked ...string) *laneTestServer {
	lts := &laneTestServer{
		blocked:     make(map[string]bool),
		release:     make(chan struct{}),
		inflight:    make(map[string]int),
		maxInflight: make(map[string]int),
		arrived:     make(map[string]int),
	}
	for _, key := range blocked {
		lts.blocked[key] = true
	}
	lts.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get(IntboxLaneHeader) + " " + r.Method + " " + r.URL.Path
		lts.lock.Lock()
		lts.inflight[key]++
		lts.arrived[key]++
		lts.maxInflight[key] = max(lts.maxInflight[key], lts.inflight[key])
		lts.lock.Unlock()
		if lts.blocked[key] {
			<-lts.release
		}
		lts.lock.Lock()
		lts.inflight[key]--
		lts.lock.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}"))
	}))
	lts.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			lts.newConns.Add(1)
		}
	}
	lts.Start()
	t.Cleanup(lts.Close)
	// Cleanups run in reverse order: unblock the handlers first, or Close would wait for them forever
	// when an assertion fails before the test releases them.
	t.Cleanup(lts.releaseAll)
	return lts
}

func (lts *laneTestServer) releaseAll() {
	lts.releaseOnce.Do(func() { close(lts.release) })
}

func (lts *laneTestServer) inFlight(key string) int {
	lts.lock.Lock()
	defer lts.lock.Unlock()
	return lts.inflight[key]
}

func (lts *laneTestServer) maxInFlight(key string) int {
	lts.lock.Lock()
	defer lts.lock.Unlock()
	return lts.maxInflight[key]
}

// arrivals returns how many requests with the key reached the server.
func (lts *laneTestServer) arrivals(key string) int {
	lts.lock.Lock()
	defer lts.lock.Unlock()
	return lts.arrived[key]
}

func newLaneTestClient(t *testing.T, lts *laneTestServer, transport http.RoundTripper) *Client {
	cli, err := NewClient(lts.URL, "@bridge:example.com", "token")
	require.NoError(t, err)
	cli.Client = &http.Client{Transport: transport, Timeout: 30 * time.Second}
	cli.DefaultHTTPRetries = 0
	return cli
}

// Bulk and backfill writes are held at the "proxy" (like nginx limit_req delays). Each write pool never gets more
// than its cap of connections and the rest of its requests wait in the client, while live requests, encryption
// requests from any lane, and ghost registrations and reads from the bulk lane still go through at once.
func TestIntboxLaneTransport_CapsEachPoolAndNeverStarvesLive(t *testing.T) {
	const capPerPool = 3
	const writesPerLane = 12
	bulkWrite := requestKey(IntboxLaneBulk, http.MethodPut, "bulk_write")
	backfillWrite := requestKey(IntboxLaneBackfill, http.MethodPut, "backfill_write")
	lts := newLaneTestServer(t, bulkWrite, backfillWrite)
	tr := NewIntboxLaneTransport(nil, capPerPool)
	t.Cleanup(tr.CloseIdleConnections)
	cli := newLaneTestClient(t, lts, tr)

	var wg sync.WaitGroup
	writeErrs := make(chan error, 2*writesPerLane)
	for _, lane := range []IntboxLane{IntboxLaneBulk, IntboxLaneBackfill} {
		ctx := WithIntboxLane(context.Background(), lane)
		url := cli.BuildClientURL("v3", string(lane)+"_write")
		for range writesPerLane {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := cli.MakeRequest(ctx, http.MethodPut, url, struct{}{}, nil)
				writeErrs <- err
			}()
		}
	}
	require.Eventually(t, func() bool {
		return lts.inFlight(bulkWrite) == capPerPool && lts.inFlight(backfillWrite) == capPerPool
	}, 5*time.Second, 5*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, capPerPool, lts.inFlight(bulkWrite), "the other bulk writes must wait in the client")
	assert.Equal(t, capPerPool, lts.inFlight(backfillWrite), "the other backfill writes must wait in the client")

	bulkCtx := WithIntboxLane(context.Background(), IntboxLaneBulk)
	backfillCtx := WithIntboxLane(context.Background(), IntboxLaneBackfill)
	for _, probe := range []struct {
		name   string
		ctx    context.Context
		method string
		path   []any
	}{
		{"live send", context.Background(), http.MethodPut, []any{"v3", "rooms", "!r:hs", "send", "m.room.message", "1"}},
		{"live read", context.Background(), http.MethodGet, []any{"v3", "joined_members"}},
		{"key claim from the backfill lane", backfillCtx, http.MethodPost, []any{"v3", "keys", "claim"}},
		{"to-device from the backfill lane", backfillCtx, http.MethodPut, []any{"v3", "sendToDevice", "m.room.encrypted", "1"}},
		{"ghost registration from the bulk lane", bulkCtx, http.MethodPost, []any{"v3", "register"}},
		{"profile read from the bulk lane", bulkCtx, http.MethodGet, []any{"v3", "profile", "@g:hs", "avatar_url"}},
	} {
		start := time.Now()
		ctx, cancel := context.WithTimeout(probe.ctx, 5*time.Second)
		var body any
		if probe.method != http.MethodGet {
			body = struct{}{}
		}
		_, err := cli.MakeRequest(ctx, probe.method, cli.BuildClientURL(probe.path...), body, nil)
		cancel()
		require.NoError(t, err, probe.name)
		assert.Less(t, time.Since(start), 2*time.Second, probe.name)
	}

	lts.releaseAll()
	wg.Wait()
	close(writeErrs)
	for err := range writeErrs {
		assert.NoError(t, err)
	}
	assert.Equal(t, capPerPool, lts.maxInFlight(bulkWrite))
	assert.Equal(t, capPerPool, lts.maxInFlight(backfillWrite))
	// 2 x 12 writes reused their pools' 3 connections each (the idle pool keeps all of them); the probes used
	// at most one connection each in the live and cheap pools.
	assert.LessOrEqual(t, int(lts.newConns.Load()), 2*capPerPool+2)
}

// Without the transport (Go's default), the same burst opens one connection per request: the 4 Oct failure mode.
func TestIntboxLaneTransport_DefaultTransportIsUncapped(t *testing.T) {
	const writes = 12
	bulkWrite := requestKey(IntboxLaneBulk, http.MethodPut, "bulk_write")
	lts := newLaneTestServer(t, bulkWrite)
	tr := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(tr.CloseIdleConnections)
	cli := newLaneTestClient(t, lts, tr)
	url := cli.BuildClientURL("v3", "bulk_write")
	bulkCtx := WithIntboxLane(context.Background(), IntboxLaneBulk)
	var wg sync.WaitGroup
	for range writes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = cli.MakeRequest(bulkCtx, http.MethodPut, url, struct{}{}, nil)
		}()
	}
	require.Eventually(t, func() bool { return lts.inFlight(bulkWrite) == writes }, 5*time.Second, 5*time.Millisecond)
	lts.releaseAll()
	wg.Wait()
	assert.Equal(t, writes, int(lts.newConns.Load()))
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// Some programs replace http.DefaultTransport with a wrapper; the lane transport must not panic on that.
func TestIntboxLaneTransport_ReplacedDefaultTransport(t *testing.T) {
	orig := http.DefaultTransport
	http.DefaultTransport = roundTripperFunc(func(*http.Request) (*http.Response, error) { return nil, nil })
	defer func() { http.DefaultTransport = orig }()
	tr := NewIntboxLaneTransport(nil, 4)
	pt := tr.Pool(IntboxPoolLive)
	assert.Equal(t, 4, pt.MaxConnsPerHost)
	assert.NotNil(t, pt.Proxy)
	assert.NotNil(t, pt.DialContext)
	assert.Equal(t, IntboxIdleConnTimeout, pt.IdleConnTimeout)
}

// closeTracker is a request body that records whether it was closed.
type closeTracker struct {
	io.Reader
	closed atomic.Bool
}

func (c *closeTracker) Close() error {
	c.closed.Store(true)
	return nil
}

// Review finding "the wait for a pooled connection counts toward the 180 s client timeout": a request that gets no
// slot of its pool within MaxPoolWait fails with ErrIntboxPoolWait and is never sent, and its body is closed.
func TestIntboxLaneTransport_PoolWaitIsBounded(t *testing.T) {
	bulkWrite := requestKey(IntboxLaneBulk, http.MethodPut, "bulk_write")
	createRoom := requestKey(IntboxLaneBulk, http.MethodPost, "createRoom")
	liveSend := requestKey(IntboxLaneLive, http.MethodPut, "live_send")
	lts := newLaneTestServer(t, bulkWrite)
	tr := NewIntboxLaneTransport(nil, 2)
	tr.MaxPoolWait = 300 * time.Millisecond
	t.Cleanup(tr.CloseIdleConnections)
	cli := newLaneTestClient(t, lts, tr)
	bulkCtx := WithIntboxLane(context.Background(), IntboxLaneBulk)

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = cli.MakeRequest(bulkCtx, http.MethodPut, cli.BuildClientURL("v3", "bulk_write"), struct{}{}, nil)
		}()
	}
	require.Eventually(t, func() bool { return lts.inFlight(bulkWrite) == 2 }, 5*time.Second, 5*time.Millisecond)

	start := time.Now()
	_, err := cli.MakeRequest(bulkCtx, http.MethodPost, cli.BuildClientURL("v3", "createRoom"), struct{}{}, nil)
	waited := time.Since(start)
	require.ErrorIs(t, err, ErrIntboxPoolWait)
	assert.GreaterOrEqual(t, waited, tr.MaxPoolWait)
	assert.Less(t, waited, 3*time.Second)
	assert.Equal(t, 0, lts.arrivals(createRoom), "a request that gave up waiting for a slot must never be sent")

	// Other pools still work while the bulk pool is full.
	_, err = cli.MakeRequest(context.Background(), http.MethodPut, cli.BuildClientURL("v3", "live_send"), struct{}{}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, lts.arrivals(liveSend))

	// RoundTrip closes the request body when it gives up (http.RoundTripper contract).
	body := &closeTracker{Reader: strings.NewReader("{}")}
	req, err := http.NewRequestWithContext(bulkCtx, http.MethodPost, cli.BuildClientURL("v3", "createRoom"), body)
	require.NoError(t, err)
	req.Header.Set(IntboxLaneHeader, string(IntboxLaneBulk))
	_, err = tr.RoundTrip(req)
	require.ErrorIs(t, err, ErrIntboxPoolWait)
	assert.True(t, body.closed.Load())
	assert.Equal(t, 0, lts.arrivals(createRoom))

	lts.releaseAll()
	wg.Wait()
	_, err = cli.MakeRequest(bulkCtx, http.MethodPost, cli.BuildClientURL("v3", "createRoom"), struct{}{}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, lts.arrivals(createRoom))
}

// The point of MaxPoolWait. The pool's only slot is held for the holder's whole client timeout, and requests keep
// arriving behind it. With the limit, each of them is either given up unsent or sent with at least
// Timeout - MaxPoolWait of its budget left. Without it (the first version of the transport), a request that waited
// in the bridge was sent with almost nothing left: the server runs it, the client times out, and Client retries it,
// so a createRoom or a media upload would run twice.
func TestIntboxLaneTransport_NeverSendsWithLittleBudgetLeft(t *testing.T) {
	const timeout = 2 * time.Second
	const writes = 10
	const stagger = 200 * time.Millisecond
	for _, tc := range []struct {
		name    string
		maxWait time.Duration
	}{
		{"limited", 600 * time.Millisecond},
		{"unlimited (before the fix)", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			holding := make(chan struct{}, 1)
			release := make(chan struct{})
			// Per write (monotonic clock, same process): when it started, when it got its slot (the pool's
			// transport asks for a connection right after that), and when the server saw it.
			var lock sync.Mutex
			starts := make([]time.Time, writes)
			slotAt := make([]time.Duration, writes)
			lags := make([]time.Duration, writes)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/hold" {
					holding <- struct{}{}
					select {
					case <-release:
					case <-r.Context().Done():
					}
					return
				}
				i, _ := strconv.Atoi(r.Header.Get("X-Test-Write"))
				lock.Lock()
				lags[i] = time.Since(starts[i])
				lock.Unlock()
				_, _ = w.Write([]byte("{}"))
			}))
			t.Cleanup(ts.Close)
			t.Cleanup(func() { close(release) }) // runs before ts.Close
			tr := NewIntboxLaneTransport(nil, 1)
			tr.MaxPoolWait = tc.maxWait
			t.Cleanup(tr.CloseIdleConnections)
			client := &http.Client{Transport: tr, Timeout: timeout}
			post := func(path string, i int) error {
				ctx := context.Background()
				if i >= 0 {
					ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{GetConn: func(string) {
						lock.Lock()
						slotAt[i] = time.Since(starts[i])
						lock.Unlock()
					}})
				}
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+path, strings.NewReader("{}"))
				if err != nil {
					return err
				}
				req.Header.Set(IntboxLaneHeader, string(IntboxLaneBulk))
				if i >= 0 {
					req.Header.Set("X-Test-Write", strconv.Itoa(i))
					lock.Lock()
					starts[i] = time.Now()
					lock.Unlock()
				}
				resp, err := client.Do(req)
				if err != nil {
					return err
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				return resp.Body.Close()
			}

			holdErr := make(chan error, 1)
			go func() { holdErr <- post("/hold", -1) }()
			select {
			case <-holding:
			case <-time.After(5 * time.Second):
				t.Fatal("the holder never reached the server")
			}
			errs := make([]error, writes)
			var wg sync.WaitGroup
			for i := range writes {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs[i] = post("/write", i)
				}()
				time.Sleep(stagger)
			}
			wg.Wait()
			assert.Error(t, <-holdErr, "the holder itself times out")

			lock.Lock()
			defer lock.Unlock()
			var maxLag time.Duration
			sent, poolWaits := 0, 0
			var detail strings.Builder
			for i := range writes {
				if lags[i] > 0 {
					sent++
					maxLag = max(maxLag, lags[i])
				}
				if errors.Is(errs[i], ErrIntboxPoolWait) {
					poolWaits++
				}
				_, _ = fmt.Fprintf(&detail, "\nwrite %d: slot after %s, at server after %s, err %v",
					i, slotAt[i].Round(time.Millisecond), lags[i].Round(time.Millisecond), errs[i])
			}
			if tc.maxWait > 0 {
				for i, err := range errs {
					if err != nil {
						assert.ErrorIs(t, err, ErrIntboxPoolWait, "write %d: the only failure mode is giving up unsent", i)
					}
				}
				// The first writes wait longer than MaxPoolWait (the slot frees only after 2 s) and give up; the last
				// ones get the slot when the holder times out.
				assert.GreaterOrEqual(t, poolWaits, 4, detail.String())
				assert.GreaterOrEqual(t, sent, 2, detail.String())
				assert.Equal(t, writes, poolWaits+sent, detail.String())
				assert.LessOrEqual(t, maxLag, tc.maxWait+400*time.Millisecond,
					"every request is sent with at least Timeout - MaxPoolWait of its budget left%s", detail.String())
			} else {
				assert.Equal(t, 0, poolWaits, detail.String())
				assert.Greater(t, maxLag, timeout-600*time.Millisecond,
					"without the limit a request is sent with less than 0.6 s of its 2 s budget left%s", detail.String())
			}
		})
	}
}

// A request that gave up waiting is retried by Client like a 503 from the proxy, and runs exactly once.
func TestIntboxLaneTransport_ClientRetriesPoolWait(t *testing.T) {
	bulkWrite := requestKey(IntboxLaneBulk, http.MethodPut, "bulk_write")
	createRoom := requestKey(IntboxLaneBulk, http.MethodPost, "createRoom")
	lts := newLaneTestServer(t, bulkWrite)
	tr := NewIntboxLaneTransport(nil, 1)
	tr.MaxPoolWait = 200 * time.Millisecond
	t.Cleanup(tr.CloseIdleConnections)
	cli := newLaneTestClient(t, lts, tr)
	cli.DefaultHTTPRetries = 2
	cli.DefaultHTTPBackoff = 300 * time.Millisecond
	bulkCtx := WithIntboxLane(context.Background(), IntboxLaneBulk)

	holdDone := make(chan error, 1)
	go func() {
		_, err := cli.MakeRequest(bulkCtx, http.MethodPut, cli.BuildClientURL("v3", "bulk_write"), struct{}{}, nil)
		holdDone <- err
	}()
	require.Eventually(t, func() bool { return lts.inFlight(bulkWrite) == 1 }, 5*time.Second, 5*time.Millisecond)
	// Attempt 1 gives up at 200 ms, the hold ends at 300 ms, attempt 2 runs at about 500 ms.
	time.AfterFunc(300*time.Millisecond, lts.releaseAll)
	_, err := cli.MakeRequest(bulkCtx, http.MethodPost, cli.BuildClientURL("v3", "createRoom"), struct{}{}, nil)
	require.NoError(t, err)
	require.NoError(t, <-holdDone)
	assert.Equal(t, 1, lts.arrivals(createRoom))
}

// The slot is given back when the body is read to the end without Close, when it's closed unread, for bodiless
// responses, on transport errors, and at once for a protocol switch (websocket mode), whose body stays writable.
// A request whose context ends while it waits returns the context's error, not ErrIntboxPoolWait.
func TestIntboxLaneTransport_SlotRelease(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/empty":
			w.WriteHeader(http.StatusNoContent)
		case "/broken":
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
		case "/upgrade":
			conn, brw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer conn.Close()
			_, _ = brw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: lt-test\r\nConnection: Upgrade\r\n\r\n")
			_ = brw.Flush()
			buf := make([]byte, 4)
			if _, err = io.ReadFull(brw, buf); err == nil {
				_, _ = conn.Write(buf)
			}
			_, _ = io.Copy(io.Discard, brw)
		default:
			_, _ = w.Write([]byte(`{"ok":true}`))
		}
	}))
	t.Cleanup(ts.Close)
	tr := NewIntboxLaneTransport(nil, 1)
	tr.MaxPoolWait = 5 * time.Second
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr}
	do := func(ctx context.Context, method, path string) (*http.Response, error) {
		var body io.Reader
		if method == http.MethodPost {
			body = strings.NewReader("{}")
		}
		req, err := http.NewRequestWithContext(ctx, method, ts.URL+path, body)
		require.NoError(t, err)
		req.Header.Set(IntboxLaneHeader, string(IntboxLaneBulk))
		return client.Do(req)
	}
	// With one slot, a leaked slot makes this time out after 1 s (well before MaxPoolWait).
	requireSlotFree := func(step string) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, err := do(ctx, http.MethodPost, "/json")
		require.NoError(t, err, "after: %s", step)
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	resp, err := do(context.Background(), http.MethodPost, "/json")
	require.NoError(t, err)
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, `{"ok":true}`, string(data))
	requireSlotFree("body read to EOF, not closed")
	_ = resp.Body.Close()

	resp, err = do(context.Background(), http.MethodPost, "/json")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	requireSlotFree("body closed unread")

	resp, err = do(context.Background(), http.MethodPost, "/empty")
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	requireSlotFree("204 without Close")
	_ = resp.Body.Close()

	_, err = do(context.Background(), http.MethodPost, "/broken")
	require.Error(t, err)
	requireSlotFree("transport error")

	// Context canceled while waiting: the context's error, and the request body is closed.
	held, err := do(context.Background(), http.MethodPost, "/json")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	body := &closeTracker{Reader: strings.NewReader("{}")}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.URL+"/json", body)
	require.NoError(t, err)
	req.Header.Set(IntboxLaneHeader, string(IntboxLaneBulk))
	_, err = tr.RoundTrip(req)
	require.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, ErrIntboxPoolWait)
	assert.True(t, body.closed.Load())
	require.NoError(t, held.Body.Close())
	requireSlotFree("held body closed")

	// Protocol switch (GET from the bulk lane: the cheap pool, also capped at 1).
	req, err = http.NewRequest(http.MethodGet, ts.URL+"/upgrade", nil)
	require.NoError(t, err)
	req.Header.Set(IntboxLaneHeader, string(IntboxLaneBulk))
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "lt-test")
	assert.Equal(t, IntboxPoolCheap, tr.PoolFor(req))
	resp, err = client.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusSwitchingProtocols, resp.StatusCode)
	rwc, ok := resp.Body.(io.ReadWriteCloser)
	require.True(t, ok, "the upgraded body must stay writable")
	_, err = rwc.Write([]byte("ping"))
	require.NoError(t, err)
	echo := make([]byte, 4)
	_, err = io.ReadFull(bufio.NewReader(rwc), echo)
	require.NoError(t, err)
	assert.Equal(t, "ping", string(echo))
	getCtx, getCancel := context.WithTimeout(context.Background(), time.Second)
	defer getCancel()
	resp2, err := do(getCtx, http.MethodGet, "/json")
	require.NoError(t, err, "the upgraded connection must not hold the cheap pool's slot")
	_ = resp2.Body.Close()
	require.NoError(t, rwc.Close())
}
