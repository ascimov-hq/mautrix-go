// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package mautrix

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntboxHeaders(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
	setIntboxHeaders(context.Background(), req)
	assert.Equal(t, "live", req.Header.Get(IntboxLaneHeader))
	assert.Empty(t, req.Header.Get(IntboxLoginHeader))

	ctx := WithIntboxLogin(WithIntboxLane(context.Background(), IntboxLaneBackfill), "919999999999")
	req, _ = http.NewRequest(http.MethodGet, "https://example.com", nil)
	setIntboxHeaders(ctx, req)
	assert.Equal(t, "backfill", req.Header.Get(IntboxLaneHeader))
	login := req.Header.Get(IntboxLoginHeader)
	assert.Len(t, login, 8)
	assert.Equal(t, HashIntboxLogin("919999999999"), login)
	assert.NotContains(t, login, "9199")

	// Later tags override earlier ones
	ctx = WithIntboxLane(ctx, IntboxLaneBulk)
	assert.Equal(t, IntboxLaneBulk, IntboxLaneFromContext(ctx))
}
