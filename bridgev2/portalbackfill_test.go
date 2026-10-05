// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package bridgev2

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"maunium.net/go/mautrix"
)

func httpErrWithStatus(statusCode int) error {
	return fmt.Errorf("failed to send message part to Matrix: %w", mautrix.HTTPError{
		Response: &http.Response{StatusCode: statusCode},
	})
}

func dbErr() error {
	return fmt.Errorf("%w: failed to save message part to database: %w", ErrDatabaseError, errors.New("disk full"))
}

func TestIsSkippableBackfillError(t *testing.T) {
	assert.False(t, isSkippableBackfillError(nil))
	assert.True(t, isSkippableBackfillError(errors.Join(httpErrWithStatus(http.StatusForbidden))))
	assert.True(t, isSkippableBackfillError(errors.Join(httpErrWithStatus(http.StatusRequestEntityTooLarge))))
	for _, statusCode := range []int{http.StatusUnauthorized, http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusBadGateway} {
		assert.False(t, isSkippableBackfillError(errors.Join(httpErrWithStatus(statusCode))), "status %d", statusCode)
	}
	// Network errors have no response
	assert.False(t, isSkippableBackfillError(errors.Join(fmt.Errorf("failed to send: %w", mautrix.HTTPError{}))))
	assert.False(t, isSkippableBackfillError(errors.Join(context.Canceled)))

	assert.True(t, isSkippableBackfillError(errors.Join(dbErr())))
	assert.True(t, isSkippableBackfillError(errors.Join(httpErrWithStatus(http.StatusForbidden), dbErr())))
	// One transient error in any part aborts the batch, even if another part only failed to save
	assert.False(t, isSkippableBackfillError(errors.Join(dbErr(), httpErrWithStatus(http.StatusBadGateway))))
	assert.False(t, isSkippableBackfillError(errors.Join(httpErrWithStatus(http.StatusBadGateway), dbErr())))
}
