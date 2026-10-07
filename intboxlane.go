// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mautrix

import (
	"context"
	"fmt"
	"hash/crc32"
	"net/http"
)

// IntboxLane is the request lane that is sent to the homeserver in the X-Intbox-Lane header.
//
// The reverse proxy in front of the homeserver uses the lane to pace bulk work (portal creation,
// member sync, ghost registration and profiles) and history backfill separately from live traffic,
// so that one account's onboarding doesn't delay everyone else's live messages.
type IntboxLane string

const (
	// IntboxLaneLive is the default lane: live messages, edits, reactions, redactions, receipts,
	// typing notifications, crypto and everything else that isn't explicitly tagged.
	IntboxLaneLive IntboxLane = "live"
	// IntboxLaneBulk is used for portal creation and sync, member sync, ghost registration and
	// profile updates, space membership and other room metadata changes.
	IntboxLaneBulk IntboxLane = "bulk"
	// IntboxLaneBackfill is used for history backfill sends (both legacy and batch send).
	IntboxLaneBackfill IntboxLane = "backfill"
)

const (
	// IntboxLaneHeader is the HTTP header that carries the request lane.
	IntboxLaneHeader = "X-Intbox-Lane"
	// IntboxLoginHeader is the HTTP header that carries the hashed user login ID
	// (8 lowercase hex characters of the IEEE CRC32 of the login ID).
	IntboxLoginHeader = "X-Intbox-Login"
)

type intboxContextKey int

const (
	intboxLaneContextKey intboxContextKey = iota
	intboxLoginContextKey
)

// WithIntboxLane returns a copy of the context where all homeserver requests are tagged with the given lane.
// An empty lane removes the override, which means the default live lane is used.
func WithIntboxLane(ctx context.Context, lane IntboxLane) context.Context {
	if IntboxLaneFromContext(ctx) == lane {
		return ctx
	}
	return context.WithValue(ctx, intboxLaneContextKey, lane)
}

// IntboxLaneFromContext returns the lane set in the context with [WithIntboxLane], or [IntboxLaneLive] if none is set.
func IntboxLaneFromContext(ctx context.Context) IntboxLane {
	lane, _ := ctx.Value(intboxLaneContextKey).(IntboxLane)
	if lane == "" {
		return IntboxLaneLive
	}
	return lane
}

// HashIntboxLogin returns the value used in the X-Intbox-Login header for the given login ID.
// The login ID itself (which may contain a phone number) is never sent.
func HashIntboxLogin(loginID string) string {
	return fmt.Sprintf("%08x", crc32.ChecksumIEEE([]byte(loginID)))
}

// WithIntboxLogin returns a copy of the context where all homeserver requests are tagged with the given login.
// An empty login ID removes the tag.
func WithIntboxLogin(ctx context.Context, loginID string) context.Context {
	var hashed string
	if loginID != "" {
		hashed = HashIntboxLogin(loginID)
	}
	if existing, _ := ctx.Value(intboxLoginContextKey).(string); existing == hashed {
		return ctx
	}
	return context.WithValue(ctx, intboxLoginContextKey, hashed)
}

// IntboxLoginFromContext returns the hashed login ID set in the context with [WithIntboxLogin], or an empty string.
func IntboxLoginFromContext(ctx context.Context) string {
	login, _ := ctx.Value(intboxLoginContextKey).(string)
	return login
}

func setIntboxHeaders(ctx context.Context, req *http.Request) {
	req.Header.Set(IntboxLaneHeader, string(IntboxLaneFromContext(ctx)))
	if login := IntboxLoginFromContext(ctx); login != "" {
		req.Header.Set(IntboxLoginHeader, login)
	}
}
