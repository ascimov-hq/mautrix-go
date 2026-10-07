// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package bridgev2

import (
	"context"

	"maunium.net/go/mautrix"
)

// WithIntboxLane tags all homeserver requests made with the returned context with the given request lane
// (X-Intbox-Lane header), and with the given user login (X-Intbox-Login header) if it's not nil.
//
// The bridge tags portal creation, member and room info sync, ghost profile updates and space membership as bulk,
// and backfill as backfill. Everything else uses the live lane. Network connectors can use this to tag their own
// bulk work, e.g. contact list syncs.
func WithIntboxLane(ctx context.Context, lane mautrix.IntboxLane, login *UserLogin) context.Context {
	ctx = mautrix.WithIntboxLane(ctx, lane)
	if login != nil {
		ctx = mautrix.WithIntboxLogin(ctx, string(login.ID))
	}
	return ctx
}
