// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package matrix

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/rs/xid"
	"github.com/rs/zerolog"
	"go.mau.fi/util/exhttp"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/status"
)

type ProvLogin struct {
	ID       string
	Process  bridgev2.LoginProcess
	PrevStep *bridgev2.LoginStep
	NextStep *bridgev2.LoginStep
	Override *bridgev2.UserLogin
	// User is the Matrix user who started the login process.
	User *bridgev2.User
	// Lock serializes the step requests of this process. A request that arrives while another step
	// of the same process is running waits for it and then gets its result (see doLoginStep).
	Lock sync.Mutex

	Ctx       context.Context
	CancelCtx context.CancelFunc

	// startSeq is the position of the start request that created this process, in arrival order. It's set
	// before CreateLogin and Start run, and never changes. Superseding uses it to decide which of two
	// processes of the same user is newer, because Start can take a while (e.g. connecting to the remote
	// network and waiting for the first QR code), so processes aren't registered in the order they were requested.
	startSeq uint64

	// The fields below are protected by ProvisioningAPI.loginsLock.
	state        provLoginState
	finalStep    *bridgev2.LoginStep
	finalErr     error
	retentionGen int
}

type provLoginState int

const (
	// provLoginActive: the process is in progress.
	provLoginActive provLoginState = iota
	// provLoginCompleted: a step returned the complete step. The process stays in the logins map for
	// the retention period and answers every step request with the same complete step.
	provLoginCompleted
	// provLoginSuperseded: the process was cancelled because a newer login process of the same user was
	// started, or the network connector reported that a newer login of the same remote account replaced it.
	// It stays in the logins map for the retention period and answers every step request with HTTP 410.
	provLoginSuperseded
	// provLoginFailed: a step returned an error or the process timed out. The process is removed from the
	// logins map right away, so later requests get 404 like before, but requests that were already
	// waiting for the step lock get the same error as the request that failed.
	provLoginFailed
	// provLoginCancelled: the process was cancelled with the cancel endpoint. It's removed from the logins
	// map right away, requests that were waiting for the step lock get ErrLoginCancelled.
	provLoginCancelled
)

// DefaultFinishedLoginRetention is how long completed and superseded login processes are kept, so that a
// repeated step request (e.g. a client retrying a poll whose response it never received) gets the final
// result instead of a 404.
const DefaultFinishedLoginRetention = 5 * time.Minute

var ErrNilStep = errors.New("bridge returned nil step with no error")
var ErrTooManyLogins = bridgev2.RespError{ErrCode: "FI.MAU.BRIDGE.TOO_MANY_LOGINS", Err: "Maximum number of logins exceeded"}
var ErrLoginCancelled = bridgev2.RespError{ErrCode: "FI.MAU.BRIDGE.LOGIN_CANCELLED", Err: "Login process was cancelled"}
var ErrLoginTimedOut = bridgev2.RespError{ErrCode: "FI.MAU.BRIDGE.LOGIN_TIMED_OUT", Err: "Login process timed out"}
var ErrLoginAlreadyFinished = bridgev2.RespError{ErrCode: "FI.MAU.BRIDGE.LOGIN_ALREADY_FINISHED", Err: "Login process was already finished"}
var ErrLoginSuperseded = bridgev2.ErrLoginSuperseded

// errStepMismatch is returned when a step request doesn't match the current step of the login process.
// It used to be a plain M_BAD_STATE, which has no status code and was therefore sent as HTTP 500.
func errStepMismatch(msg string) error {
	return mautrix.RespError{ErrCode: mautrix.MBadState.ErrCode, Err: msg, StatusCode: http.StatusConflict}
}

func (prov *ProvisioningAPI) PostLoginStart(w http.ResponseWriter, r *http.Request) {
	// Numbered on arrival, before the possibly slow CreateLogin and Start, see registerLogin.
	startSeq := prov.loginStartSeq.Add(1)
	overrideLogin, failed := prov.GetExplicitLoginForRequest(w, r)
	if failed {
		return
	}
	user := prov.GetUser(r)
	if overrideLogin == nil && user.HasTooManyLogins() {
		ErrTooManyLogins.AppendMessage(" (%d)", user.Permissions.MaxLogins).Write(w)
		return
	}
	login, err := prov.net.CreateLogin(r.Context(), user, r.PathValue("flowID"))
	if err != nil {
		zerolog.Ctx(r.Context()).Err(err).Msg("Failed to create login process")
		RespondWithError(w, err, "Internal error creating login process")
		return
	}
	var firstStep *bridgev2.LoginStep
	overridable, ok := login.(bridgev2.LoginProcessWithOverride)
	if ok && overrideLogin != nil {
		firstStep, err = overridable.StartWithOverride(r.Context(), overrideLogin)
	} else {
		firstStep, err = login.Start(r.Context())
	}
	if err == nil && firstStep == nil {
		err = ErrNilStep
	}
	if err != nil {
		zerolog.Ctx(r.Context()).Err(err).Msg("Failed to start login")
		RespondWithError(w, err, "Internal error starting login")
		return
	}
	loginID := xid.New().String()
	ctx, cancel := context.WithTimeout(prov.br.Bridge.BackgroundCtx, 30*time.Minute)
	ctx = user.Log.With().
		Str("login_id", loginID).
		Logger().WithContext(ctx)
	provLogin := &ProvLogin{
		ID:        loginID,
		Process:   login,
		NextStep:  firstStep,
		Override:  overrideLogin,
		User:      user,
		Ctx:       ctx,
		CancelCtx: cancel,
		startSeq:  startSeq,
	}
	go prov.handleLoginTimeout(provLogin)
	superseded, newer := prov.registerLogin(provLogin)
	if newer != nil {
		// A start request of the same user that arrived after this one has already registered its process.
		// That one is what the client uses now, so this process is the one that gets cancelled.
		zerolog.Ctx(r.Context()).Info().
			Str("login_id", loginID).
			Str("newer_login_id", newer.ID).
			Msg("Cancelling new login process, a newer login process of the same user was started meanwhile")
		go func() {
			login.Cancel()
			cancel()
		}()
		ErrLoginSuperseded.Write(w)
		return
	}
	zerolog.Ctx(r.Context()).Info().
		Str("login_id", loginID).
		Any("first_step", firstStep).
		Msg("Created login process")
	for _, oldLogin := range superseded {
		zerolog.Ctx(r.Context()).Info().
			Str("login_id", loginID).
			Str("superseded_login_id", oldLogin.ID).
			Msg("Cancelling older login process of the same user")
		// Cancelling may close network connections, so don't make the new login wait for it.
		// The old process is already marked as superseded, so its requests get 410 from now on.
		go func() {
			oldLogin.Process.Cancel()
			oldLogin.CancelCtx()
		}()
	}
	exhttp.WriteJSONResponse(w, http.StatusOK, &RespSubmitLogin{LoginID: loginID, LoginStep: firstStep})
}

// registerLogin adds a new login process to the logins map. If provisioning.supersede_logins is enabled,
// the user's newest login wins. "Newest" is the order in which the start requests arrived (startSeq), not the
// order in which their Start calls returned:
//   - The user's older active processes are marked as superseded and returned, so that the caller can cancel
//     them outside the lock.
//   - If the user already has a newer process that is active or completed (its start request arrived later,
//     but its Start returned first), the new process itself is marked as superseded and that newer process
//     is returned as newer. Nothing else is touched. The caller cancels the new process and answers 410.
//     A newer process that the network connector already cancelled (bridgev2.LoginProcessSuperseded) doesn't
//     count: e.g. a WhatsApp re-login auto-submits the phone number inside Start, and the code that WhatsApp
//     issued last wins there, which can be the code of this older request.
//
// Processes that report IsCompleting are never superseded.
func (prov *ProvisioningAPI) registerLogin(login *ProvLogin) (superseded []*ProvLogin, newer *ProvLogin) {
	prov.loginsLock.Lock()
	defer prov.loginsLock.Unlock()
	if !prov.br.Config.Provisioning.SupersedeLogins {
		prov.logins[login.ID] = login
		return nil, nil
	}
	var older []*ProvLogin
	for _, other := range prov.logins {
		if other.User == nil || other.User.MXID != login.User.MXID {
			continue
		}
		if other.startSeq > login.startSeq {
			if (other.state == provLoginCompleted || (other.state == provLoginActive && !isLoginSupersededByNetwork(other))) &&
				(newer == nil || other.startSeq > newer.startSeq) {
				newer = other
			}
		} else if other.state == provLoginActive {
			older = append(older, other)
		}
	}
	if newer != nil {
		if isLoginCompleting(login) {
			// Can't happen with the connectors we know (nothing completes during Start), but never cancel a
			// completing process. The newer process already superseded everything older than itself.
			zerolog.Ctx(login.Ctx).Info().
				Str("newer_login_id", newer.ID).
				Msg("Not cancelling new login process although a newer one exists, as it's already completing")
			prov.logins[login.ID] = login
			return nil, nil
		}
		login.state = provLoginSuperseded
		login.finalErr = ErrLoginSuperseded
		prov.retainFinishedLoginLocked(login)
		return nil, newer
	}
	for _, other := range older {
		if isLoginCompleting(other) {
			zerolog.Ctx(login.Ctx).Info().
				Str("other_login_id", other.ID).
				Msg("Not cancelling older login process of the same user as it's already completing")
			continue
		}
		other.state = provLoginSuperseded
		other.finalErr = ErrLoginSuperseded
		prov.retainFinishedLoginLocked(other)
		superseded = append(superseded, other)
	}
	prov.logins[login.ID] = login
	return superseded, nil
}

// isLoginCompleting returns true if the network connector reports that the login process is already completing
// (see bridgev2.LoginProcessCompleting).
func isLoginCompleting(login *ProvLogin) bool {
	completing, ok := login.Process.(bridgev2.LoginProcessCompleting)
	return ok && completing.IsCompleting()
}

// isLoginSupersededByNetwork returns true if the network connector reports that it already cancelled the login
// process because a newer login replaced it (see bridgev2.LoginProcessSuperseded).
func isLoginSupersededByNetwork(login *ProvLogin) bool {
	superseded, ok := login.Process.(bridgev2.LoginProcessSuperseded)
	return ok && superseded.IsSuperseded()
}

// getLogin returns the login process with the given ID if it belongs to the given user.
func (prov *ProvisioningAPI) getLogin(loginID string, user *bridgev2.User) *ProvLogin {
	prov.loginsLock.RLock()
	defer prov.loginsLock.RUnlock()
	return prov.getLoginLocked(loginID, user)
}

func (prov *ProvisioningAPI) getLoginLocked(loginID string, user *bridgev2.User) *ProvLogin {
	login, ok := prov.logins[loginID]
	if !ok || (login.User != nil && login.User.MXID != user.MXID) {
		return nil
	}
	return login
}

func (prov *ProvisioningAPI) PostLoginStep(w http.ResponseWriter, r *http.Request) {
	loginID := r.PathValue("loginProcessID")
	login := prov.getLogin(loginID, prov.GetUser(r))
	if login == nil {
		zerolog.Ctx(r.Context()).Warn().Str("login_id", loginID).Msg("Login not found")
		mautrix.MNotFound.WithMessage("Login not found").Write(w)
		return
	}
	stepID := r.PathValue("stepID")
	stepType := bridgev2.LoginStepType(r.PathValue("stepType"))
	switch stepType {
	case bridgev2.LoginStepTypeUserInput, bridgev2.LoginStepTypeCookies, bridgev2.LoginStepTypeDisplayAndWait:
	case bridgev2.LoginStepTypeComplete:
		ErrLoginAlreadyFinished.Write(w)
		return
	default:
		mautrix.MUnrecognized.WithMessage("Invalid step type %q", r.PathValue("stepType")).Write(w)
		return
	}
	var resp *bridgev2.LoginStep
	var err error
	if finished, finalStep, finalErr := prov.finishedResult(login); finished {
		// Completed and superseded processes are kept for a while, so that retried requests get the same
		// final result (200 with the complete step, or 410) instead of 404.
		zerolog.Ctx(r.Context()).Debug().
			Str("login_id", login.ID).
			Str("step_id", stepID).
			Msg("Returning final result of finished login process")
		resp, err = finalStep, finalErr
	} else {
		var params map[string]string
		if stepType == bridgev2.LoginStepTypeUserInput || stepType == bridgev2.LoginStepTypeCookies {
			err = json.NewDecoder(r.Body).Decode(&params)
			if err != nil {
				zerolog.Ctx(r.Context()).Err(err).Msg("Failed to decode request body")
				mautrix.MNotJSON.WithMessage("Failed to decode request body").Write(w)
				return
			}
		}
		resp, err = prov.doLoginStep(r.Context(), login, stepType, stepID, params)
	}
	if err != nil {
		zerolog.Ctx(r.Context()).Err(err).Msg("Failed to complete login step")
		RespondWithError(w, err, "Internal error in login step")
	} else {
		exhttp.WriteJSONResponse(w, http.StatusOK, &RespSubmitLogin{LoginID: login.ID, LoginStep: resp})
	}
}

// PostLoginCancel cancels a login process. It's idempotent: cancelling an unknown, already cancelled,
// completed or superseded process returns 200 without doing anything.
func (prov *ProvisioningAPI) PostLoginCancel(w http.ResponseWriter, r *http.Request) {
	loginID := r.PathValue("loginProcessID")
	prov.loginsLock.Lock()
	login := prov.getLoginLocked(loginID, prov.GetUser(r))
	if login == nil || login.state != provLoginActive {
		prov.loginsLock.Unlock()
		zerolog.Ctx(r.Context()).Debug().
			Str("login_id", loginID).
			Bool("found", login != nil).
			Msg("Login to cancel not found or already finished, nothing to do")
		exhttp.WriteEmptyJSONResponse(w, http.StatusOK)
		return
	}
	login.state = provLoginCancelled
	prov.removeLoginLocked(login)
	prov.loginsLock.Unlock()
	zerolog.Ctx(r.Context()).Info().Str("login_id", loginID).Msg("Cancelling login process")
	login.Process.Cancel()
	login.CancelCtx()
	exhttp.WriteEmptyJSONResponse(w, http.StatusOK)
}

// doLoginStep runs one step of a login process.
//
// Only one step of a process runs at a time. If a request arrives while another request for the same process
// is running (e.g. a client sending a second display_and_wait poll while the first one is still waiting),
// the second request waits for the first one to finish, and then:
//   - if the first request finished the process, it gets the same final result (the complete step,
//     the same error, or 410 if the process was superseded meanwhile),
//   - if it asked for the step that the first request just answered, it gets the same next step,
//   - otherwise the normal step ID and type checks apply (409 M_BAD_STATE on mismatch).
func (prov *ProvisioningAPI) doLoginStep(
	ctx context.Context,
	login *ProvLogin,
	expectedType bridgev2.LoginStepType,
	expectedID string,
	params map[string]string,
) (*bridgev2.LoginStep, error) {
	log := zerolog.Ctx(ctx).With().Str("login_id", login.ID).Logger()
	var returnPrevIfMatch bool
	if !login.Lock.TryLock() {
		log.Warn().Msg("Failed to acquire login lock immediately, waiting for the running step")
		returnPrevIfMatch = true
		login.Lock.Lock()
	}
	defer login.Lock.Unlock()
	if finished, finalStep, finalErr := prov.finishedResult(login); finished {
		log.Debug().Bool("waited_for_lock", returnPrevIfMatch).Msg("Login process is already finished, returning its final result")
		return finalStep, finalErr
	} else if login.Ctx.Err() != nil {
		// The process timed out, but handleLoginTimeout hasn't marked it as finished yet.
		if errors.Is(login.Ctx.Err(), context.DeadlineExceeded) {
			return nil, ErrLoginTimedOut
		}
		return nil, ErrLoginCancelled
	}

	if returnPrevIfMatch && login.PrevStep != nil && login.PrevStep.StepID == expectedID {
		log.Debug().
			Str("prev_step_id", login.PrevStep.StepID).
			Any("next_step", login.NextStep).
			Msg("Login step that failed to acquire lock requested previous ID, returning last response")
		return login.NextStep, nil
	}
	if login.NextStep.StepID != expectedID {
		log.Warn().
			Str("request_step_id", expectedID).
			Str("expected_step_id", login.NextStep.StepID).
			Msg("Step ID does not match")
		return nil, errStepMismatch("Step ID does not match")
	}
	if login.NextStep.Type != expectedType {
		log.Warn().
			Str("request_step_type", string(expectedType)).
			Str("expected_step_type", string(login.NextStep.Type)).
			Msg("Step type does not match")
		return nil, errStepMismatch("Step type does not match")
	}
	log.Debug().
		Str("step_id", login.NextStep.StepID).
		Str("step_type", string(login.NextStep.Type)).
		Msg("Submitting login step")
	var nextStep *bridgev2.LoginStep
	var err error
	switch login.NextStep.Type {
	case bridgev2.LoginStepTypeUserInput:
		nextStep, err = login.Process.(bridgev2.LoginProcessUserInput).SubmitUserInput(login.Ctx, params)
	case bridgev2.LoginStepTypeCookies:
		nextStep, err = login.Process.(bridgev2.LoginProcessCookies).SubmitCookies(login.Ctx, params)
	case bridgev2.LoginStepTypeDisplayAndWait:
		nextStep, err = login.Process.(bridgev2.LoginProcessDisplayAndWait).Wait(login.Ctx)
	default:
		panic("Impossible state")
	}
	if err == nil && nextStep == nil {
		err = ErrNilStep
	}
	if err != nil {
		return nil, prov.handleStepError(login, err)
	}
	login.PrevStep = login.NextStep
	login.NextStep = nextStep
	if nextStep.Type == bridgev2.LoginStepTypeComplete {
		prov.handleCompleteStep(login, nextStep)
		return nextStep, nil
	} else if finished, _, finalErr := prov.finishedResult(login); finished {
		// The process was superseded or cancelled while the step was running:
		// don't hand out a step (e.g. a pairing code) that can't work anymore.
		log.Debug().Err(finalErr).Msg("Login process was cancelled while the step was running")
		return nil, finalErr
	}
	log.Debug().Any("next_step", nextStep).Msg("Returning next login step")
	return nextStep, nil
}

// handleStepError marks the login process as finished after a step returned an error
// and returns the error that should be sent to the client.
func (prov *ProvisioningAPI) handleStepError(login *ProvLogin, err error) error {
	prov.loginsLock.Lock()
	if finished, _, finalErr := login.finishedResultLocked(); finished {
		// The process was superseded, cancelled or timed out while the step was running, so err is most
		// likely just the cancelled context. Process.Cancel was already called by whoever finished it.
		prov.loginsLock.Unlock()
		zerolog.Ctx(login.Ctx).Debug().AnErr("step_error", err).Msg("Login step failed after the process was finished")
		return finalErr
	}
	if supersededErr, ok := asSupersededError(err); ok {
		// The network connector cancelled the process because a newer login replaced it
		// (e.g. a newer pairing code request for the same phone number).
		login.state = provLoginSuperseded
		login.finalErr = supersededErr
		prov.retainFinishedLoginLocked(login)
		err = supersededErr
	} else {
		if errors.Is(err, context.DeadlineExceeded) && errors.Is(login.Ctx.Err(), context.DeadlineExceeded) {
			err = ErrLoginTimedOut
		}
		login.state = provLoginFailed
		login.finalErr = err
		prov.removeLoginLocked(login)
	}
	prov.loginsLock.Unlock()
	login.Process.Cancel()
	login.CancelCtx()
	return err
}

// asSupersededError checks if a network connector returned an error with the LOGIN_SUPERSEDED errcode,
// and returns it as a RespError with the right status code.
func asSupersededError(err error) (bridgev2.RespError, bool) {
	var respErr mautrix.RespError
	var bridgeRespErr bridgev2.RespError
	if errors.As(err, &bridgeRespErr) {
		respErr = mautrix.RespError(bridgeRespErr)
	} else if !errors.As(err, &respErr) {
		return bridgev2.RespError{}, false
	}
	if respErr.ErrCode != ErrLoginSuperseded.ErrCode {
		return bridgev2.RespError{}, false
	}
	out := ErrLoginSuperseded
	if respErr.Err != "" {
		out.Err = respErr.Err
	}
	return out, true
}

func (prov *ProvisioningAPI) handleCompleteStep(login *ProvLogin, step *bridgev2.LoginStep) {
	ctx := login.Ctx
	zerolog.Ctx(ctx).Info().
		Str("step_id", step.StepID).
		Str("user_login_id", string(step.CompleteParams.UserLoginID)).
		Msg("Login completed successfully")
	defer login.CancelCtx()
	prov.loginsLock.Lock()
	if login.state != provLoginActive {
		zerolog.Ctx(ctx).Warn().
			Int("previous_state", int(login.state)).
			Msg("Login process completed after it was cancelled, keeping the completed result")
	}
	// Keep the process for a while, so that a retried request gets the complete step instead of 404.
	login.state = provLoginCompleted
	login.finalStep = step
	login.finalErr = nil
	prov.retainFinishedLoginLocked(login)
	prov.loginsLock.Unlock()
	if login.Override == nil || login.Override.ID == step.CompleteParams.UserLoginID {
		return
	}
	zerolog.Ctx(ctx).Info().
		Str("old_login_id", string(login.Override.ID)).
		Str("new_login_id", string(step.CompleteParams.UserLoginID)).
		Msg("Login resulted in different remote ID than what was being overridden. Deleting previous login")
	login.Override.Delete(ctx, status.BridgeState{
		StateEvent: status.StateLoggedOut,
		Reason:     "LOGIN_OVERRIDDEN",
	}, bridgev2.DeleteOpts{LogoutRemote: true})
}

func (prov *ProvisioningAPI) handleLoginTimeout(login *ProvLogin) {
	<-login.Ctx.Done()
	if !errors.Is(login.Ctx.Err(), context.DeadlineExceeded) {
		return
	}
	prov.loginsLock.Lock()
	wasActive := login.state == provLoginActive
	if wasActive {
		login.state = provLoginFailed
		login.finalErr = ErrLoginTimedOut
		prov.removeLoginLocked(login)
	}
	prov.loginsLock.Unlock()
	if wasActive {
		zerolog.Ctx(login.Ctx).Warn().Msg("Login process timed out, deleting")
		login.Process.Cancel()
	}
}

func (prov *ProvisioningAPI) finishedResult(login *ProvLogin) (finished bool, step *bridgev2.LoginStep, err error) {
	prov.loginsLock.RLock()
	defer prov.loginsLock.RUnlock()
	return login.finishedResultLocked()
}

// finishedResultLocked returns the final result of a login process that isn't active anymore.
// The caller must hold ProvisioningAPI.loginsLock.
func (login *ProvLogin) finishedResultLocked() (finished bool, step *bridgev2.LoginStep, err error) {
	switch login.state {
	case provLoginActive:
		return false, nil, nil
	case provLoginCompleted:
		return true, login.finalStep, nil
	case provLoginCancelled:
		return true, nil, ErrLoginCancelled
	default:
		return true, nil, login.finalErr
	}
}

func (prov *ProvisioningAPI) getFinishedLoginRetention() time.Duration {
	if prov.finishedLoginRetention > 0 {
		return prov.finishedLoginRetention
	}
	return DefaultFinishedLoginRetention
}

// retainFinishedLoginLocked keeps a finished login process in the logins map for the retention period.
// The caller must hold ProvisioningAPI.loginsLock.
func (prov *ProvisioningAPI) retainFinishedLoginLocked(login *ProvLogin) {
	prov.logins[login.ID] = login
	login.retentionGen++
	gen := login.retentionGen
	time.AfterFunc(prov.getFinishedLoginRetention(), func() {
		prov.loginsLock.Lock()
		defer prov.loginsLock.Unlock()
		if login.retentionGen == gen {
			prov.removeLoginLocked(login)
		}
	})
}

// removeLoginLocked removes a login process from the logins map.
// The caller must hold ProvisioningAPI.loginsLock.
func (prov *ProvisioningAPI) removeLoginLocked(login *ProvLogin) {
	if prov.logins[login.ID] == login {
		delete(prov.logins, login.ID)
	}
}
