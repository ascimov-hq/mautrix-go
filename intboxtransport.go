// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mautrix

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// DefaultIntboxMaxConnsPerPool is the default cap on concurrent connections to one homeserver host, per connection
// pool of an [IntboxLaneTransport], used by appservice.Create and by the bridge config
// (homeserver.max_connections_per_pool).
//
// Why 64: the reverse proxy paces the bridge's background work (50-70 requests/s for bulk and backfill writes,
// 300/s for ghost registration, profile and other reads) and Tuwunel answers those in about 5-40 ms, so a handful of
// requests in flight per pool already keeps every pacing zone busy, and the live lane needs even fewer (production
// peaks below 1 request/s). More connections only lengthen the queue held open at the proxy: on 4 Oct 2026 an
// uncapped burst of ghost registrations and profile requests opened about 4,500 connections and exhausted the
// gateway's file descriptors (soft limit 1,024 per nginx worker). With 64 per pool the bridge holds at most
// 4 x 64 = 256 connections to the homeserver (512 descriptors at the gateway even if one nginx worker got all of
// them and proxied every one), at most 64 requests per pool wait at the proxy (about 1.3 s of queue at 50 requests/s,
// well under the pacing zones' nginx burst sizes of 600-2,000), and the extra requests wait inside the bridge, for at
// most [IntboxMaxPoolWait].
const DefaultIntboxMaxConnsPerPool = 64

// IntboxMaxPoolWait is the longest a request waits inside the bridge for a free slot of its pool (see
// [IntboxLaneTransport.MaxPoolWait]). After that it fails with [ErrIntboxPoolWait] before any byte is sent, and
// [Client] retries it with backoff, the way it retries a 503 from the proxy's pacing zones.
//
// Why a limit, and why 60 s: the slot wait happens inside http.Client.Do, so it counts toward the client's 180 s
// timeout. Without a limit, a long backlog would hand each freed connection to a request with only seconds of its
// budget left: the proxy and Tuwunel would still run it (a createRoom, a media upload, ...), the client would time
// out, and the retry would run it again. With 60 s, every request that is sent still has at least 120 s of a 180 s
// budget, which is far more than the proxy's own queue in front of it (at most one pool, 64 requests, about 1-2 s at
// the paced rates) plus Tuwunel's time. And 60 s still absorbs every normal onboarding burst: 60 s at the bulk zone's
// 50 requests/s is about 3,000 queued writes per pool, against at most 142 queued at once in the 30 Sep incident.
const IntboxMaxPoolWait = 60 * time.Second

// ErrIntboxPoolWait means a request waited [IntboxLaneTransport.MaxPoolWait] for a free slot of its connection pool
// and was given up before anything was sent, so it is always safe to retry. http.Client returns it wrapped in a
// *url.Error, and [Client] retries it like any other transport error (it is not context.Canceled).
var ErrIntboxPoolWait = errors.New("timed out waiting for a free homeserver connection in the bridge")

// IntboxIdleConnTimeout is how long an idle pooled connection to the homeserver is kept. It is below nginx's stock
// keepalive_timeout (65 s), so that the bridge closes idle connections before the proxy does, instead of
// occasionally sending a request on a connection that the proxy is closing at that moment.
const IntboxIdleConnTimeout = 55 * time.Second

// Connection pools of an [IntboxLaneTransport]. They follow the request classes that the reverse proxy paces
// separately (ascimov-matrix nginx/default.conf, "lane table").
const (
	// IntboxPoolLive carries everything the proxy never paces: the live lane, plus end-to-end encryption requests
	// (keys/*, sendToDevice) and the bot's /sync from any lane. Encryption requests run while the crypto machine
	// holds its locks, so they must never queue behind paced background work.
	IntboxPoolLive = "live"
	// IntboxPoolBulk carries bulk-lane writes (member state, profile PUTs, room creation, ...): the bulk zone.
	IntboxPoolBulk = "bulk"
	// IntboxPoolBackfill carries backfill-lane writes (history sends, media uploads, batch sends): the backfill zone.
	IntboxPoolBackfill = "backfill"
	// IntboxPoolCheap carries bulk- and backfill-lane reads and ghost registrations (GET/HEAD and POST /register):
	// the proxy's fast "cheap" zone (300 requests/s). Keeping them out of the write pools keeps the bridge from
	// putting a ghost registration (also those triggered by a live message) behind seconds of queued writes.
	IntboxPoolCheap = "cheap"
)

var intboxPools = [...]string{IntboxPoolLive, IntboxPoolBulk, IntboxPoolBackfill, IntboxPoolCheap}

// IntboxLaneTransport is an [http.RoundTripper] with one connection pool per request class that the reverse proxy
// paces separately (see the IntboxPool* constants), each capped at a maximum number of connections per host.
//
// Requests over a pool's cap wait in the bridge (in order) for a free slot of their own pool, instead of opening
// more sockets to the proxy. Bulk and backfill writes are deliberately delayed by the proxy, and keep their
// connection while they wait there, so they can only ever fill their own pool: live requests never queue behind them.
//
// A slot is one request in flight on one host, from the moment it is sent until its response body is read to the
// end or closed (HTTP/1.1: one connection). The wait for a slot is limited by MaxPoolWait; a request that waited that
// long fails with [ErrIntboxPoolWait] without being sent. Each pool's transport also has MaxConnsPerHost set to the
// cap, so the number of sockets never exceeds it either.
//
// The lane is taken from the X-Intbox-Lane header (which [Client] always sets from the request context), or from
// the request context if the header is missing. Unknown lane values are treated like the bulk lane, as the proxy
// does (it paces them like header-less traffic).
type IntboxLaneTransport struct {
	// MaxPoolWait is the longest a request waits for a free slot of its pool before it fails with
	// [ErrIntboxPoolWait] (nothing is sent). NewIntboxLaneTransport sets it to [IntboxMaxPoolWait]; <= 0 means no
	// limit other than the request's context. Set it before the first request.
	MaxPoolWait time.Duration

	maxConnsPerPool int
	pools           map[string]*http.Transport

	slotsLock sync.Mutex
	slots     map[intboxSlotKey]chan struct{}
}

// intboxSlotKey identifies one pool's slots for one host (the cap is per host, like MaxConnsPerHost).
type intboxSlotKey struct {
	pool, scheme, addr string
}

var _ http.RoundTripper = (*IntboxLaneTransport)(nil)

// NewIntboxLaneTransport creates an [IntboxLaneTransport]. Each pool is a clone of base ([http.DefaultTransport]
// if nil) with MaxConnsPerHost and MaxIdleConnsPerHost set to maxConnsPerPool, and IdleConnTimeout capped at
// [IntboxIdleConnTimeout]. maxConnsPerPool <= 0 means no cap (the pools are still separate).
func NewIntboxLaneTransport(base *http.Transport, maxConnsPerPool int) *IntboxLaneTransport {
	if base == nil {
		var ok bool
		if base, ok = http.DefaultTransport.(*http.Transport); !ok {
			// http.DefaultTransport was replaced with something else: use Go's default settings.
			base = &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			}
		}
	}
	if maxConnsPerPool < 0 {
		maxConnsPerPool = 0
	}
	t := &IntboxLaneTransport{
		MaxPoolWait:     IntboxMaxPoolWait,
		maxConnsPerPool: maxConnsPerPool,
		pools:           make(map[string]*http.Transport, len(intboxPools)),
		slots:           make(map[intboxSlotKey]chan struct{}),
	}
	for _, pool := range intboxPools {
		pt := base.Clone()
		pt.MaxConnsPerHost = maxConnsPerPool
		if maxConnsPerPool > 0 {
			// Keep the whole pool warm after a burst: Go's default keeps only 2 idle connections per host,
			// so every burst used to end with the bridge closing (and later reopening) all the others.
			pt.MaxIdleConnsPerHost = maxConnsPerPool
			if pt.MaxIdleConns > 0 && pt.MaxIdleConns < maxConnsPerPool {
				pt.MaxIdleConns = maxConnsPerPool
			}
		}
		if pt.IdleConnTimeout <= 0 || pt.IdleConnTimeout > IntboxIdleConnTimeout {
			pt.IdleConnTimeout = IntboxIdleConnTimeout
		}
		t.pools[pool] = pt
	}
	return t
}

// MaxConnsPerPool returns the per-pool, per-host connection cap (0 means no cap).
func (t *IntboxLaneTransport) MaxConnsPerPool() int {
	return t.maxConnsPerPool
}

// Pool returns the underlying transport of the given pool (one of the IntboxPool* constants), or nil.
func (t *IntboxLaneTransport) Pool(name string) *http.Transport {
	return t.pools[name]
}

// PoolFor returns the name of the pool that the given request uses.
func (t *IntboxLaneTransport) PoolFor(req *http.Request) string {
	lane := IntboxLane(req.Header.Get(IntboxLaneHeader))
	if lane == "" {
		lane = IntboxLaneFromContext(req.Context())
	}
	endpoint := clientEndpoint(req.URL.Path)
	switch {
	case strings.HasPrefix(endpoint, "keys/"), strings.HasPrefix(endpoint, "sendToDevice/"), endpoint == "sync":
		// Never paced by the proxy in any lane (the bot's /sync has its own zone and only one request in flight).
		return IntboxPoolLive
	case strings.Contains(req.URL.Path, "/com.beeper.backfill/") && strings.HasSuffix(req.URL.Path, "/batch_send"):
		// Paced in the proxy's batch zone whatever the lane, so never in the live pool.
		return IntboxPoolBackfill
	case lane == IntboxLaneLive:
		return IntboxPoolLive
	case req.Method == http.MethodGet || req.Method == http.MethodHead ||
		(req.Method == http.MethodPost && endpoint == "register"):
		return IntboxPoolCheap
	case lane == IntboxLaneBackfill:
		return IntboxPoolBackfill
	default:
		return IntboxPoolBulk
	}
}

// clientEndpoint returns the part of a client-server API path after /_matrix/client/<version>/,
// e.g. "keys/query" for /_matrix/client/v3/keys/query, or "" for other paths.
func clientEndpoint(path string) string {
	_, rest, ok := strings.Cut(path, "/_matrix/client/")
	if !ok {
		return ""
	}
	_, endpoint, ok := strings.Cut(rest, "/")
	if !ok {
		return ""
	}
	return endpoint
}

// RoundTrip sends the request through its pool. With a cap, it first waits (at most MaxPoolWait) for a free slot
// of that pool for the request's host, and holds the slot until the response body is read to the end or closed.
func (t *IntboxLaneTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	pool := t.PoolFor(req)
	pt := t.pools[pool]
	if t.maxConnsPerPool <= 0 {
		return pt.RoundTrip(req)
	}
	slots := t.slotsFor(pool, req.URL)
	if err := t.acquireSlot(req.Context(), slots, pool); err != nil {
		// A RoundTripper must close the request body, also on errors.
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, err
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { <-slots }) }
	resp, err := pt.RoundTrip(req)
	if err != nil {
		release()
		return nil, err
	}
	switch {
	case resp.StatusCode == http.StatusSwitchingProtocols:
		// The connection was taken over (websocket mode). It no longer serves requests, and the caller needs the
		// writable body that the transport returns, so don't wrap it.
		release()
	case resp.Body == nil || resp.Body == http.NoBody:
		// Nothing to read: the transport has already returned the connection to the pool.
		release()
	default:
		resp.Body = &intboxSlotBody{ReadCloser: resp.Body, release: release}
	}
	return resp, nil
}

// slotsFor returns the slot semaphore of the given pool and the URL's host.
func (t *IntboxLaneTransport) slotsFor(pool string, u *url.URL) chan struct{} {
	scheme := strings.ToLower(u.Scheme)
	port := u.Port()
	if port == "" {
		switch scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}
	key := intboxSlotKey{pool: pool, scheme: scheme, addr: net.JoinHostPort(strings.ToLower(u.Hostname()), port)}
	t.slotsLock.Lock()
	defer t.slotsLock.Unlock()
	slots, ok := t.slots[key]
	if !ok {
		slots = make(chan struct{}, t.maxConnsPerPool)
		t.slots[key] = slots
	}
	return slots
}

// acquireSlot takes a slot, waiting for one at most MaxPoolWait. Waiting requests get slots in arrival order (a
// receive from a full buffered channel hands the freed place to the longest-waiting sender).
func (t *IntboxLaneTransport) acquireSlot(ctx context.Context, slots chan struct{}, pool string) error {
	select {
	case slots <- struct{}{}:
		return nil
	default:
	}
	var expired <-chan time.Time
	if t.MaxPoolWait > 0 {
		timer := time.NewTimer(t.MaxPoolWait)
		defer timer.Stop()
		expired = timer.C
	}
	start := time.Now()
	select {
	case slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-expired:
		return fmt.Errorf("%w (pool %s, %d slots busy, waited %s)",
			ErrIntboxPoolWait, pool, cap(slots), time.Since(start).Round(time.Millisecond))
	}
}

// intboxSlotBody releases the request's pool slot once the body has been read to the end (or failed) or is closed,
// whichever comes first. Those are also the points where the transport reuses or closes the connection.
type intboxSlotBody struct {
	io.ReadCloser
	release func()
}

func (b *intboxSlotBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.release()
	}
	return n, err
}

func (b *intboxSlotBody) Close() error {
	err := b.ReadCloser.Close()
	b.release()
	return err
}

// CloseIdleConnections closes the idle connections of every pool (called by [http.Client.CloseIdleConnections]).
func (t *IntboxLaneTransport) CloseIdleConnections() {
	for _, pt := range t.pools {
		pt.CloseIdleConnections()
	}
}
