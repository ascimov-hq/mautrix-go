// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package bridgev2

import (
	"context"
	"errors"
	"time"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/status"
)

// intbox: bridge start and stop around in-flight events (load-test U1 and U2).
//
// Start: the appservice listener starts in StartConnectors, before StartLogins connects the network clients, and a
// WhatsApp client reports IsLoggedIn only once its connection is up (19 s for 40 logins in the load test). A Matrix
// message that arrived in that window failed at once with "not logged in (receiver login is not logged in)", and
// the appservice transaction had already been answered, so nothing retried it: app messages sent in the first
// ~20 s after a bridge (re)start were dropped. Now such an event waits, for a bounded time, for its login to connect.
//
// Stop: Stop disconnected the logins and closed the database without waiting for the portal event loops, and the
// process exits right after. An incoming message whose handler had already sent the Matrix event but not yet stored
// the message row was redelivered by WhatsApp after the restart and bridged a second time; events still queued in a
// portal (already acknowledged to WhatsApp) were lost. Now Stop waits, for a bounded time, until the portal queues
// are empty and no handler is running, before it stops the Matrix connector and closes the database.

// LoginStartupGrace is how long after StartLogins has started every login a Matrix event still waits for a login
// that isn't connected yet. Events that arrive before StartLogins has finished always wait.
var LoginStartupGrace = 60 * time.Second

// LoginStartupMaxWait is the longest a single Matrix event waits for its login to connect during startup. The
// event's portal handles nothing else meanwhile; other portals are unaffected.
var LoginStartupMaxWait = 45 * time.Second

var loginStartupPollInterval = 250 * time.Millisecond

// ShutdownDrainTimeout is how long Stop waits for the portal event queues to drain after disconnecting the logins.
// It must stay below the container's stop grace period (Docker's default is 10 s; our compose sets 30 s for the
// bridge). 0 disables the wait.
var ShutdownDrainTimeout = 15 * time.Second

var shutdownDrainPollInterval = 50 * time.Millisecond

// ShutdownMatrixDrainTimeout is how long Stop waits, after the appservice listener stopped (which waits for running
// transactions) and before it disconnects the logins, for the Matrix events that were accepted but not handled yet
// (MatrixConnectorWithPendingEvents), and for those queued in or being handled by their portal. The homeserver doesn't resend them: before this wait they were dropped at
// every stop. It's capped by ShutdownDrainTimeout, so MAUTRIX_SHUTDOWN_DRAIN_SECONDS=0 disables it too.
var ShutdownMatrixDrainTimeout = 5 * time.Second

// MatrixConnectorWithPendingEvents is an optional MatrixConnector interface (intbox) that reports how many Matrix
// events were accepted from the homeserver but not handled (passed to their portal) yet. Stop also waits for the
// Matrix events that are queued in or being handled by a portal (Bridge.matrixPortalEvents).
type MatrixConnectorWithPendingEvents interface {
	PendingMatrixEvents() int
}

// drainMatrixEvents waits until every accepted Matrix event has been handled by its portal, or until the timeout:
// the Matrix connector has no accepted but unhandled events (an appservice handler only queues a room event into its
// portal), and no Matrix event is queued in a portal or being handled there (matrixPortalEvents). Stop calls it
// before it disconnects the logins, so these events are still sent to the network (load-test L6-2, fix round 2).
func (br *Bridge) drainMatrixEvents(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	mc, _ := br.Matrix.(MatrixConnectorWithPendingEvents)
	start := time.Now()
	deadline := start.Add(timeout)
	for {
		pending := 0
		if mc != nil {
			pending = mc.PendingMatrixEvents()
		}
		inPortals := br.matrixPortalEvents.Load()
		if pending <= 0 && inPortals <= 0 {
			if waited := time.Since(start); waited > shutdownDrainPollInterval {
				br.Log.Info().Stringer("waited", waited).Msg("Queued Matrix events handled before shutdown")
			}
			return
		} else if time.Now().After(deadline) {
			br.Log.Warn().
				Int("pending", pending).
				Int64("in_portals", inPortals).
				Stringer("waited", time.Since(start)).
				Msg("Timed out waiting for queued Matrix events before shutdown")
			return
		}
		time.Sleep(shutdownDrainPollInterval)
	}
}

// loginsStarting reports whether the bridge is in its startup window: StartConnectors has run, and StartLogins
// hasn't finished or finished less than LoginStartupGrace ago. A bridge that was never started through Start
// (tests, RunOnce in background mode) is never in it.
func (br *Bridge) loginsStarting() bool {
	if br.Background || br.connectorsStartedAt.Load() == 0 {
		return false
	}
	startedAt := br.loginsStartedAt.Load()
	return startedAt == 0 || time.Since(time.Unix(0, startedAt)) < LoginStartupGrace
}

// loginStillConnecting reports whether the login that would handle the user's events in this portal exists but
// isn't connected yet, and hasn't reported that it's logged out.
func (portal *Portal) loginStillConnecting(user *User) bool {
	isConnecting := func(login *UserLogin) bool {
		if login == nil || login.Client == nil || login.Client.IsLoggedIn() {
			return false
		}
		switch login.BridgeState.GetPrevUnsent().StateEvent {
		case status.StateBadCredentials, status.StateLoggedOut:
			return false
		}
		return true
	}
	if portal.Receiver != "" {
		login := portal.Bridge.GetCachedUserLoginByID(portal.Receiver)
		return login != nil && login.UserMXID == user.MXID && isConnecting(login)
	}
	for _, login := range user.GetUserLogins() {
		if isConnecting(login) {
			return true
		}
	}
	return false
}

// findPreferredLoginForMatrixEvent is FindPreferredLogin for a Matrix event. During the startup window, if the
// login isn't connected yet, it waits up to LoginStartupMaxWait for it instead of failing at once.
// It doesn't wait with PortalEventBuffer = 0: the event is then handled inside the appservice event loop, and
// waiting would hold up every other portal's events too.
func (portal *Portal) findPreferredLoginForMatrixEvent(ctx context.Context, user *User, allowRelay bool) (*UserLogin, *database.UserPortal, error) {
	login, up, err := portal.FindPreferredLogin(ctx, user, allowRelay)
	if err == nil || !errors.Is(err, ErrNotLoggedIn) || PortalEventBuffer == 0 || portal.Bridge.IsStopping() ||
		!portal.Bridge.loginsStarting() || !portal.loginStillConnecting(user) {
		return login, up, err
	}
	log := zerolog.Ctx(ctx)
	start := time.Now()
	deadline := start.Add(LoginStartupMaxWait)
	log.Debug().Err(err).Msg("Login is still connecting after bridge start, waiting for it before handling Matrix event")
	ticker := time.NewTicker(loginStartupPollInterval)
	defer ticker.Stop()
	for time.Now().Before(deadline) && !portal.Bridge.IsStopping() {
		select {
		case <-ctx.Done():
			return login, up, err
		case <-ticker.C:
		}
		login, up, err = portal.FindPreferredLogin(ctx, user, allowRelay)
		if err == nil || !errors.Is(err, ErrNotLoggedIn) {
			log.Info().
				Stringer("waited", time.Since(start)).
				Msg("Login connected after bridge start, handling Matrix event")
			return login, up, err
		} else if !portal.loginStillConnecting(user) {
			break
		}
	}
	log.Warn().
		Stringer("waited", time.Since(start)).
		Msg("Login didn't connect after bridge start, failing Matrix event")
	return login, up, err
}

// queuedPortalEvents is the number of events waiting in the queues of the cached portals.
func (br *Bridge) queuedPortalEvents() (queued int) {
	br.cacheLock.Lock()
	defer br.cacheLock.Unlock()
	for _, portal := range br.portalsByKey {
		if portal.events != nil {
			queued += len(portal.events)
		}
	}
	return
}

// drainPortalEvents waits until no portal event is queued or being handled, or until the timeout.
func (br *Bridge) drainPortalEvents(timeout time.Duration) {
	if timeout <= 0 {
		return
	}
	start := time.Now()
	deadline := start.Add(timeout)
	for {
		active, queued := br.activePortalEvents.Load(), br.queuedPortalEvents()
		if active <= 0 && queued == 0 {
			if waited := time.Since(start); waited > shutdownDrainPollInterval {
				br.Log.Info().Stringer("waited", waited).Msg("Portal events finished before shutdown")
			}
			return
		} else if time.Now().After(deadline) {
			br.Log.Warn().
				Int64("handling", active).
				Int("queued", queued).
				Stringer("waited", time.Since(start)).
				Msg("Timed out waiting for portal events to finish before shutdown")
			return
		}
		time.Sleep(shutdownDrainPollInterval)
	}
}
