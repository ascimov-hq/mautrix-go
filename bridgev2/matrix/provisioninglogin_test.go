// Copyright (c) 2026 Ascimov Technologies
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package matrix

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

const (
	testStepPhone    = "test.phone"
	testStepCode     = "test.code"
	testStepComplete = "test.complete"

	testUserA id.UserID = "@alice:example.com"
	testUserB id.UserID = "@bob:example.com"

	testRetention = 300 * time.Millisecond
	testTimeout   = 3 * time.Second
)

type testLoginResult struct {
	step *bridgev2.LoginStep
	err  error
}

// testLoginProcess is a phone-code style login: user_input (phone) -> display_and_wait (code) -> complete.
// The outcome of each Wait call is controlled by the test through waitResults.
type testLoginProcess struct {
	waitResults chan testLoginResult
	waitEntered chan struct{}
	// If set, SubmitUserInput blocks until the channel is closed and ignores the context,
	// like a connector step that doesn't notice cancellation.
	submitGate    chan struct{}
	submitEntered chan struct{}
	// If startGate is set, Start signals startEntered and then blocks until startGate is closed,
	// like a QR login waiting for the first code. startDelay makes Start sleep instead.
	startGate    chan struct{}
	startEntered chan struct{}
	startDelay   time.Duration
	cancelled    atomic.Int32
	completing   atomic.Bool
	// supersededByNetwork is what IsSuperseded reports, like a connector that cancelled the process itself.
	supersededByNetwork atomic.Bool
}

var (
	_ bridgev2.LoginProcessUserInput      = (*testLoginProcess)(nil)
	_ bridgev2.LoginProcessDisplayAndWait = (*testLoginProcess)(nil)
	_ bridgev2.LoginProcessCompleting     = (*testLoginProcess)(nil)
	_ bridgev2.LoginProcessSuperseded     = (*testLoginProcess)(nil)
)

func newTestLoginProcess() *testLoginProcess {
	return &testLoginProcess{
		waitResults:   make(chan testLoginResult, 1),
		waitEntered:   make(chan struct{}, 10),
		submitEntered: make(chan struct{}, 10),
	}
}

func codeStep(code string) *bridgev2.LoginStep {
	return &bridgev2.LoginStep{
		Type:   bridgev2.LoginStepTypeDisplayAndWait,
		StepID: testStepCode,
		DisplayAndWaitParams: &bridgev2.LoginDisplayAndWaitParams{
			Type: bridgev2.LoginDisplayTypeCode,
			Data: code,
		},
	}
}

func completeStep(loginID string) *bridgev2.LoginStep {
	return &bridgev2.LoginStep{
		Type:         bridgev2.LoginStepTypeComplete,
		StepID:       testStepComplete,
		Instructions: "Successfully logged in",
		CompleteParams: &bridgev2.LoginCompleteParams{
			UserLoginID: networkid.UserLoginID("acc-" + loginID),
		},
	}
}

func (p *testLoginProcess) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	if p.startGate != nil {
		p.startEntered <- struct{}{}
		<-p.startGate
	}
	if p.startDelay > 0 {
		time.Sleep(p.startDelay)
	}
	return &bridgev2.LoginStep{
		Type:   bridgev2.LoginStepTypeUserInput,
		StepID: testStepPhone,
		UserInputParams: &bridgev2.LoginUserInputParams{
			Fields: []bridgev2.LoginInputDataField{{Type: bridgev2.LoginInputFieldTypePhoneNumber, ID: "phone_number"}},
		},
	}, nil
}

func (p *testLoginProcess) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	p.submitEntered <- struct{}{}
	if p.submitGate != nil {
		<-p.submitGate
	}
	return codeStep("CODE-" + input["phone_number"]), nil
}

func (p *testLoginProcess) Wait(ctx context.Context) (*bridgev2.LoginStep, error) {
	p.waitEntered <- struct{}{}
	select {
	case res := <-p.waitResults:
		return res.step, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *testLoginProcess) Cancel() {
	p.cancelled.Add(1)
}

func (p *testLoginProcess) IsCompleting() bool {
	return p.completing.Load()
}

func (p *testLoginProcess) IsSuperseded() bool {
	return p.supersededByNetwork.Load()
}

type userInputAndWait interface {
	bridgev2.LoginProcessUserInput
	Wait(ctx context.Context) (*bridgev2.LoginStep, error)
}

// plainLoginProcess wraps testLoginProcess without exposing IsCompleting,
// like network connectors that don't implement LoginProcessCompleting.
type plainLoginProcess struct {
	userInputAndWait
}

type testNetwork struct {
	bridgev2.NetworkConnector
	lock      sync.Mutex
	processes []*testLoginProcess
	// Processes of the "slow" flow get startGate and startEntered.
	slowStartGate    chan struct{}
	slowStartEntered chan struct{}
	// If set, every process gets a Start delay from it.
	startDelay func() time.Duration
}

func (n *testNetwork) CreateLogin(ctx context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	proc := newTestLoginProcess()
	n.lock.Lock()
	n.processes = append(n.processes, proc)
	if flowID == "slow" {
		proc.startGate = n.slowStartGate
		proc.startEntered = n.slowStartEntered
	}
	if n.startDelay != nil {
		proc.startDelay = n.startDelay()
	}
	n.lock.Unlock()
	if flowID == "plain" {
		return plainLoginProcess{proc}, nil
	}
	return proc, nil
}

func (n *testNetwork) last() *testLoginProcess {
	n.lock.Lock()
	defer n.lock.Unlock()
	return n.processes[len(n.processes)-1]
}

func (n *testNetwork) all() []*testLoginProcess {
	n.lock.Lock()
	defer n.lock.Unlock()
	return slices.Clone(n.processes)
}

type syncBuffer struct {
	lock sync.Mutex
	buf  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.buf.String()
}

type provTest struct {
	t       *testing.T
	prov    *ProvisioningAPI
	net     *testNetwork
	handler http.Handler
	logs    *syncBuffer

	usersLock sync.Mutex
	users     map[id.UserID]*bridgev2.User
}

func newProvTest(t *testing.T, supersede bool) *provTest {
	bgCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pt := &provTest{
		t:     t,
		net:   &testNetwork{},
		logs:  &syncBuffer{},
		users: make(map[id.UserID]*bridgev2.User),
	}
	pt.prov = &ProvisioningAPI{
		br: &Connector{
			Config: &bridgeconfig.Config{
				Provisioning: bridgeconfig.ProvisioningConfig{SupersedeLogins: supersede},
			},
			Bridge: &bridgev2.Bridge{BackgroundCtx: bgCtx},
		},
		net:                    pt.net,
		logins:                 make(map[string]*ProvLogin),
		finishedLoginRetention: testRetention,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v3/login/start/{flowID}", pt.prov.PostLoginStart)
	mux.HandleFunc("POST /v3/login/step/{loginProcessID}/{stepID}/{stepType}", pt.prov.PostLoginStep)
	mux.HandleFunc("POST /v3/login/cancel/{loginProcessID}", pt.prov.PostLoginCancel)
	pt.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), provisioningUserKey, pt.user(id.UserID(r.URL.Query().Get("user_id"))))
		ctx = zerolog.New(pt.logs).WithContext(ctx)
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
	return pt
}

func (pt *provTest) user(userID id.UserID) *bridgev2.User {
	pt.usersLock.Lock()
	defer pt.usersLock.Unlock()
	user, ok := pt.users[userID]
	if !ok {
		user = &bridgev2.User{User: &database.User{MXID: userID}, Log: zerolog.Nop()}
		pt.users[userID] = user
	}
	return user
}

type provResp struct {
	Status int    `json:"-"`
	Body   []byte `json:"-"`

	LoginID string `json:"login_id"`
	Type    string `json:"type"`
	StepID  string `json:"step_id"`
	ErrCode string `json:"errcode"`
	Error   string `json:"error"`

	DisplayAndWait *bridgev2.LoginDisplayAndWaitParams `json:"display_and_wait"`
	Complete       *bridgev2.LoginCompleteParams       `json:"complete"`
}

func (pt *provTest) do(userID id.UserID, path string, body any) provResp {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		require.NoError(pt.t, err)
		reader = bytes.NewReader(data)
	}
	req := httptest.NewRequest(http.MethodPost, path+"?user_id="+url.QueryEscape(string(userID)), reader)
	w := httptest.NewRecorder()
	pt.handler.ServeHTTP(w, req)
	resp := provResp{Status: w.Code, Body: w.Body.Bytes()}
	if len(resp.Body) > 0 {
		require.NoError(pt.t, json.Unmarshal(resp.Body, &resp), "body: %s", resp.Body)
	}
	return resp
}

func (pt *provTest) start(userID id.UserID, flowID string) (string, *testLoginProcess) {
	resp := pt.do(userID, "/v3/login/start/"+flowID, nil)
	require.Equal(pt.t, http.StatusOK, resp.Status, "start: %s", resp.Body)
	require.Equal(pt.t, testStepPhone, resp.StepID)
	require.NotEmpty(pt.t, resp.LoginID)
	return resp.LoginID, pt.net.last()
}

func (pt *provTest) submitPhone(userID id.UserID, loginID string) provResp {
	return pt.do(userID, "/v3/login/step/"+loginID+"/"+testStepPhone+"/user_input", map[string]string{"phone_number": "+15550100001"})
}

func (pt *provTest) startAndSubmit(userID id.UserID, flowID string) (string, *testLoginProcess) {
	loginID, proc := pt.start(userID, flowID)
	resp := pt.submitPhone(userID, loginID)
	require.Equal(pt.t, http.StatusOK, resp.Status, "submit: %s", resp.Body)
	require.Equal(pt.t, testStepCode, resp.StepID)
	return loginID, proc
}

func (pt *provTest) poll(userID id.UserID, loginID string) provResp {
	return pt.do(userID, "/v3/login/step/"+loginID+"/"+testStepCode+"/display_and_wait", nil)
}

func (pt *provTest) pollAsync(userID id.UserID, loginID string) <-chan provResp {
	ch := make(chan provResp, 1)
	go func() {
		ch <- pt.poll(userID, loginID)
	}()
	return ch
}

func (pt *provTest) cancel(userID id.UserID, loginID string) provResp {
	return pt.do(userID, "/v3/login/cancel/"+loginID, nil)
}

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(testTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitResp(t *testing.T, ch <-chan provResp) provResp {
	t.Helper()
	select {
	case resp := <-ch:
		return resp
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for response")
		return provResp{}
	}
}

func (pt *provTest) waitForLog(substr string) {
	pt.t.Helper()
	require.Eventually(pt.t, func() bool {
		return strings.Contains(pt.logs.String(), substr)
	}, testTimeout, 5*time.Millisecond, "log line %q", substr)
}

func (pt *provTest) waitGone(userID id.UserID, loginID string) {
	pt.t.Helper()
	require.Eventually(pt.t, func() bool {
		return pt.poll(userID, loginID).Status == http.StatusNotFound
	}, testTimeout, 20*time.Millisecond, "login process %s should be forgotten after the retention period", loginID)
}

func TestProvisioning_CompletedLoginIsIdempotent(t *testing.T) {
	pt := newProvTest(t, true)
	loginID, proc := pt.startAndSubmit(testUserA, "phone")

	pending := pt.pollAsync(testUserA, loginID)
	waitSignal(t, proc.waitEntered, "Wait")
	proc.waitResults <- testLoginResult{step: completeStep(loginID)}
	first := waitResp(t, pending)
	require.Equal(t, http.StatusOK, first.Status, "%s", first.Body)
	assert.Equal(t, string(bridgev2.LoginStepTypeComplete), first.Type)
	require.NotNil(t, first.Complete)
	assert.Equal(t, "acc-"+loginID, string(first.Complete.UserLoginID))

	// A retried poll (e.g. the app returning from WhatsApp) gets the same complete step instead of 404.
	again := pt.poll(testUserA, loginID)
	assert.Equal(t, http.StatusOK, again.Status)
	assert.Equal(t, string(first.Body), string(again.Body))
	// So does any other step request of the process.
	resubmit := pt.submitPhone(testUserA, loginID)
	assert.Equal(t, http.StatusOK, resubmit.Status)
	assert.Equal(t, string(first.Body), string(resubmit.Body))
	assert.Len(t, proc.waitEntered, 0, "Wait must not be called again for a completed process")

	// Cancelling a completed process is a no-op.
	assert.Equal(t, http.StatusOK, pt.cancel(testUserA, loginID).Status)
	assert.Equal(t, http.StatusOK, pt.poll(testUserA, loginID).Status)
	assert.EqualValues(t, 0, proc.cancelled.Load(), "a completed process must not be cancelled")

	pt.waitGone(testUserA, loginID)
}

func TestProvisioning_FailedLoginIsRemoved(t *testing.T) {
	pt := newProvTest(t, true)
	loginID, proc := pt.startAndSubmit(testUserA, "phone")

	pending := pt.pollAsync(testUserA, loginID)
	waitSignal(t, proc.waitEntered, "Wait")
	proc.waitResults <- testLoginResult{err: bridgev2.RespError{ErrCode: "FI.MAU.TEST.LOGIN_TIMEOUT", Err: "Login timed out", StatusCode: http.StatusBadRequest}}
	resp := waitResp(t, pending)
	assert.Equal(t, http.StatusBadRequest, resp.Status)
	assert.Equal(t, "FI.MAU.TEST.LOGIN_TIMEOUT", resp.ErrCode)
	assert.EqualValues(t, 1, proc.cancelled.Load())

	// Unchanged behaviour: a failed process is forgotten immediately.
	again := pt.poll(testUserA, loginID)
	assert.Equal(t, http.StatusNotFound, again.Status)
	assert.Equal(t, mautrix.MNotFound.ErrCode, again.ErrCode)
}

func TestProvisioning_ConcurrentPollsOfSameProcess(t *testing.T) {
	for _, tc := range []struct {
		name       string
		result     testLoginResult
		wantStatus int
		wantCode   string
		wantType   string
	}{{
		name:       "error",
		result:     testLoginResult{err: bridgev2.RespError{ErrCode: "FI.MAU.TEST.PAIR_ERROR", Err: "pairing failed", StatusCode: http.StatusBadRequest}},
		wantStatus: http.StatusBadRequest,
		wantCode:   "FI.MAU.TEST.PAIR_ERROR",
	}, {
		name:       "complete",
		result:     testLoginResult{step: completeStep("x")},
		wantStatus: http.StatusOK,
		wantType:   string(bridgev2.LoginStepTypeComplete),
	}, {
		name:       "refreshed code",
		result:     testLoginResult{step: codeStep("NEW-CODE")},
		wantStatus: http.StatusOK,
		wantType:   string(bridgev2.LoginStepTypeDisplayAndWait),
	}} {
		t.Run(tc.name, func(t *testing.T) {
			pt := newProvTest(t, true)
			loginID, proc := pt.startAndSubmit(testUserA, "phone")
			first := pt.pollAsync(testUserA, loginID)
			waitSignal(t, proc.waitEntered, "Wait")
			second := pt.pollAsync(testUserA, loginID)
			pt.waitForLog("Failed to acquire login lock immediately")
			proc.waitResults <- tc.result
			for i, resp := range []provResp{waitResp(t, first), waitResp(t, second)} {
				assert.Equal(t, tc.wantStatus, resp.Status, "response %d: %s", i, resp.Body)
				assert.Equal(t, tc.wantCode, resp.ErrCode, "response %d", i)
				assert.Equal(t, tc.wantType, resp.Type, "response %d", i)
				if tc.name == "refreshed code" {
					require.NotNil(t, resp.DisplayAndWait)
					assert.Equal(t, "NEW-CODE", resp.DisplayAndWait.Data)
				}
			}
			assert.Len(t, proc.waitEntered, 0, "the second poll must reuse the first poll's result")
		})
	}
}

func TestProvisioning_StepMismatchIsConflict(t *testing.T) {
	pt := newProvTest(t, true)
	loginID, _ := pt.startAndSubmit(testUserA, "phone")
	resp := pt.do(testUserA, "/v3/login/step/"+loginID+"/wrong.step/display_and_wait", nil)
	assert.Equal(t, http.StatusConflict, resp.Status)
	assert.Equal(t, mautrix.MBadState.ErrCode, resp.ErrCode)
	resp = pt.do(testUserA, "/v3/login/step/"+loginID+"/"+testStepCode+"/user_input", map[string]string{})
	assert.Equal(t, http.StatusConflict, resp.Status)
	assert.Equal(t, mautrix.MBadState.ErrCode, resp.ErrCode)
}

func TestProvisioning_NewLoginSupersedesOlderProcess(t *testing.T) {
	pt := newProvTest(t, true)
	oldID, oldProc := pt.startAndSubmit(testUserA, "phone")
	pending := pt.pollAsync(testUserA, oldID)
	waitSignal(t, oldProc.waitEntered, "Wait")

	newID, newProc := pt.start(testUserA, "phone")

	resp := waitResp(t, pending)
	assert.Equal(t, http.StatusGone, resp.Status, "%s", resp.Body)
	assert.Equal(t, "FI.MAU.BRIDGE.LOGIN_SUPERSEDED", resp.ErrCode)
	assert.EqualValues(t, 1, oldProc.cancelled.Load())

	// Every later request for the old process gets 410 too, and cancelling it is a no-op.
	assert.Equal(t, http.StatusGone, pt.poll(testUserA, oldID).Status)
	assert.Equal(t, http.StatusGone, pt.submitPhone(testUserA, oldID).Status)
	assert.Equal(t, http.StatusOK, pt.cancel(testUserA, oldID).Status)
	assert.Equal(t, http.StatusGone, pt.poll(testUserA, oldID).Status)
	assert.EqualValues(t, 1, oldProc.cancelled.Load())

	// The new process works, and another user's login doesn't supersede it.
	_, _ = pt.start(testUserB, "phone")
	submit := pt.submitPhone(testUserA, newID)
	require.Equal(t, http.StatusOK, submit.Status, "%s", submit.Body)
	pending = pt.pollAsync(testUserA, newID)
	waitSignal(t, newProc.waitEntered, "Wait")
	newProc.waitResults <- testLoginResult{step: completeStep(newID)}
	assert.Equal(t, http.StatusOK, waitResp(t, pending).Status)
	assert.EqualValues(t, 0, newProc.cancelled.Load())

	pt.waitGone(testUserA, oldID)
}

func TestProvisioning_SupersedeSkipsCompletingProcess(t *testing.T) {
	pt := newProvTest(t, true)
	oldID, oldProc := pt.startAndSubmit(testUserA, "phone")
	pending := pt.pollAsync(testUserA, oldID)
	waitSignal(t, oldProc.waitEntered, "Wait")
	oldProc.completing.Store(true)

	_, newProc := pt.start(testUserA, "phone")
	assert.EqualValues(t, 0, oldProc.cancelled.Load(), "a completing process must not be cancelled")

	oldProc.waitResults <- testLoginResult{step: completeStep(oldID)}
	resp := waitResp(t, pending)
	assert.Equal(t, http.StatusOK, resp.Status, "%s", resp.Body)
	assert.Equal(t, string(bridgev2.LoginStepTypeComplete), resp.Type)
	assert.EqualValues(t, 0, newProc.cancelled.Load())
}

func TestProvisioning_SupersedeProcessWithoutCompletingInterface(t *testing.T) {
	pt := newProvTest(t, true)
	oldID, oldProc := pt.startAndSubmit(testUserA, "plain")
	pending := pt.pollAsync(testUserA, oldID)
	waitSignal(t, oldProc.waitEntered, "Wait")
	oldProc.completing.Store(true) // not visible through plainLoginProcess

	_, _ = pt.start(testUserA, "plain")
	assert.Equal(t, http.StatusGone, waitResp(t, pending).Status)
	assert.EqualValues(t, 1, oldProc.cancelled.Load())
}

func TestProvisioning_SupersedeDisabled(t *testing.T) {
	pt := newProvTest(t, false)
	oldID, oldProc := pt.startAndSubmit(testUserA, "phone")
	pending := pt.pollAsync(testUserA, oldID)
	waitSignal(t, oldProc.waitEntered, "Wait")

	_, _ = pt.start(testUserA, "phone")
	assert.EqualValues(t, 0, oldProc.cancelled.Load())
	oldProc.waitResults <- testLoginResult{step: completeStep(oldID)}
	assert.Equal(t, http.StatusOK, waitResp(t, pending).Status)
}

func TestProvisioning_SupersededWhileStepRunning(t *testing.T) {
	pt := newProvTest(t, true)
	oldID, oldProc := pt.start(testUserA, "phone")
	oldProc.submitGate = make(chan struct{})
	pending := make(chan provResp, 1)
	go func() {
		pending <- pt.submitPhone(testUserA, oldID)
	}()
	waitSignal(t, oldProc.submitEntered, "SubmitUserInput")

	_, _ = pt.start(testUserA, "phone")
	// The connector step ignores the cancellation and returns a pairing code anyway:
	// it must not be handed to the client, since the process is gone.
	close(oldProc.submitGate)
	resp := waitResp(t, pending)
	assert.Equal(t, http.StatusGone, resp.Status, "%s", resp.Body)
	assert.Equal(t, "FI.MAU.BRIDGE.LOGIN_SUPERSEDED", resp.ErrCode)
}

func TestProvisioning_SupersededByConnector(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{{
		name: "bridgev2 RespError without status",
		err:  bridgev2.RespError{ErrCode: "FI.MAU.BRIDGE.LOGIN_SUPERSEDED", Err: "A newer pairing code was requested for this phone number"},
	}, {
		name: "mautrix RespError",
		err:  mautrix.RespError{ErrCode: "FI.MAU.BRIDGE.LOGIN_SUPERSEDED", Err: "A newer pairing code was requested for this phone number", StatusCode: http.StatusBadRequest},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			pt := newProvTest(t, true)
			loginID, proc := pt.startAndSubmit(testUserA, "phone")
			pending := pt.pollAsync(testUserA, loginID)
			waitSignal(t, proc.waitEntered, "Wait")
			proc.waitResults <- testLoginResult{err: tc.err}
			for _, resp := range []provResp{waitResp(t, pending), pt.poll(testUserA, loginID)} {
				assert.Equal(t, http.StatusGone, resp.Status, "%s", resp.Body)
				assert.Equal(t, "FI.MAU.BRIDGE.LOGIN_SUPERSEDED", resp.ErrCode)
				assert.Equal(t, "A newer pairing code was requested for this phone number", resp.Error)
			}
			pt.waitGone(testUserA, loginID)
		})
	}
}

func TestProvisioning_CancelIsIdempotent(t *testing.T) {
	pt := newProvTest(t, true)
	assert.Equal(t, http.StatusOK, pt.cancel(testUserA, "unknown").Status)

	loginID, proc := pt.startAndSubmit(testUserA, "phone")
	pending := pt.pollAsync(testUserA, loginID)
	waitSignal(t, proc.waitEntered, "Wait")

	assert.Equal(t, http.StatusOK, pt.cancel(testUserA, loginID).Status)
	resp := waitResp(t, pending)
	assert.Equal(t, ErrLoginCancelled.ErrCode, resp.ErrCode, "%s", resp.Body)
	assert.EqualValues(t, 1, proc.cancelled.Load())

	assert.Equal(t, http.StatusOK, pt.cancel(testUserA, loginID).Status)
	assert.Equal(t, http.StatusNotFound, pt.poll(testUserA, loginID).Status)
	assert.EqualValues(t, 1, proc.cancelled.Load())
}

func TestProvisioning_LoginProcessBelongsToItsUser(t *testing.T) {
	pt := newProvTest(t, true)
	loginID, proc := pt.start(testUserA, "phone")

	assert.Equal(t, http.StatusNotFound, pt.submitPhone(testUserB, loginID).Status)
	assert.Equal(t, http.StatusOK, pt.cancel(testUserB, loginID).Status)
	assert.EqualValues(t, 0, proc.cancelled.Load(), "another user must not be able to cancel the process")

	resp := pt.submitPhone(testUserA, loginID)
	assert.Equal(t, http.StatusOK, resp.Status, "%s", resp.Body)
}

// startSlowAsync starts a login of the "slow" flow, whose Start blocks until pt.net.slowStartGate is closed.
// It returns once the request is inside Start, i.e. after its arrival was numbered.
func (pt *provTest) startSlowAsync(userID id.UserID) (<-chan provResp, *testLoginProcess) {
	pt.t.Helper()
	if pt.net.slowStartGate == nil {
		pt.net.slowStartGate = make(chan struct{})
		pt.net.slowStartEntered = make(chan struct{}, 1)
	}
	ch := make(chan provResp, 1)
	go func() {
		ch <- pt.do(userID, "/v3/login/start/slow", nil)
	}()
	waitSignal(pt.t, pt.net.slowStartEntered, "slow Start")
	return ch, pt.net.last()
}

func waitCancelled(t *testing.T, proc *testLoginProcess, what string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return proc.cancelled.Load() == 1
	}, testTimeout, 5*time.Millisecond, "%s should be cancelled once", what)
}

// Superseding follows the order in which start requests arrived, not the order in which their (possibly slow)
// Start calls returned: a start request whose Start returns after a newer request of the same user registered
// its process must not cancel that newer process.
func TestProvisioning_SupersedeUsesRequestOrder(t *testing.T) {
	t.Run("newer process registered first", func(t *testing.T) {
		pt := newProvTest(t, true)
		// e.g. a QR start waiting for the first code, then the user switches to the pairing code flow
		slowResp, slowProc := pt.startSlowAsync(testUserA)
		newID, newProc := pt.start(testUserA, "phone")
		close(pt.net.slowStartGate)

		resp := waitResp(t, slowResp)
		// The newer process, which the client is using, is untouched and completes.
		submit := pt.submitPhone(testUserA, newID)
		require.Equal(t, http.StatusOK, submit.Status, "the newer process must keep working: %s", submit.Body)
		pending := pt.pollAsync(testUserA, newID)
		waitSignal(t, newProc.waitEntered, "Wait")
		newProc.waitResults <- testLoginResult{step: completeStep(newID)}
		assert.Equal(t, http.StatusOK, waitResp(t, pending).Status)
		assert.EqualValues(t, 0, newProc.cancelled.Load())

		// The late process is refused and cancelled.
		assert.Equal(t, http.StatusGone, resp.Status, "%s", resp.Body)
		assert.Equal(t, "FI.MAU.BRIDGE.LOGIN_SUPERSEDED", resp.ErrCode)
		assert.Empty(t, resp.LoginID)
		waitCancelled(t, slowProc, "the late process")
	})

	t.Run("newer process already completed", func(t *testing.T) {
		pt := newProvTest(t, true)
		slowResp, slowProc := pt.startSlowAsync(testUserA)
		newID, newProc := pt.startAndSubmit(testUserA, "phone")
		pending := pt.pollAsync(testUserA, newID)
		waitSignal(t, newProc.waitEntered, "Wait")
		newProc.waitResults <- testLoginResult{step: completeStep(newID)}
		require.Equal(t, http.StatusOK, waitResp(t, pending).Status)
		close(pt.net.slowStartGate)

		resp := waitResp(t, slowResp)
		assert.Equal(t, http.StatusGone, resp.Status, "%s", resp.Body)
		assert.Equal(t, "FI.MAU.BRIDGE.LOGIN_SUPERSEDED", resp.ErrCode)
		waitCancelled(t, slowProc, "the late process")
		again := pt.poll(testUserA, newID)
		assert.Equal(t, http.StatusOK, again.Status, "the completed process keeps its result")
		assert.Equal(t, string(bridgev2.LoginStepTypeComplete), again.Type)
		assert.EqualValues(t, 0, newProc.cancelled.Load())
	})

	t.Run("newer process was cancelled", func(t *testing.T) {
		pt := newProvTest(t, true)
		slowResp, slowProc := pt.startSlowAsync(testUserA)
		newID, newProc := pt.start(testUserA, "phone")
		require.Equal(t, http.StatusOK, pt.cancel(testUserA, newID).Status)
		close(pt.net.slowStartGate)

		// Nothing newer is left, so the late process is a normal login.
		resp := waitResp(t, slowResp)
		require.Equal(t, http.StatusOK, resp.Status, "%s", resp.Body)
		require.NotEmpty(t, resp.LoginID)
		submit := pt.submitPhone(testUserA, resp.LoginID)
		assert.Equal(t, http.StatusOK, submit.Status, "%s", submit.Body)
		assert.EqualValues(t, 0, slowProc.cancelled.Load())
		assert.EqualValues(t, 1, newProc.cancelled.Load())
	})

	t.Run("slow newer process supersedes older one", func(t *testing.T) {
		pt := newProvTest(t, true)
		oldID, oldProc := pt.startAndSubmit(testUserA, "phone")
		pending := pt.pollAsync(testUserA, oldID)
		waitSignal(t, oldProc.waitEntered, "Wait")
		slowResp, slowProc := pt.startSlowAsync(testUserA)
		// The older process keeps working until the newer one is registered.
		assert.EqualValues(t, 0, oldProc.cancelled.Load())
		close(pt.net.slowStartGate)

		resp := waitResp(t, slowResp)
		require.Equal(t, http.StatusOK, resp.Status, "%s", resp.Body)
		old := waitResp(t, pending)
		assert.Equal(t, http.StatusGone, old.Status, "%s", old.Body)
		assert.Equal(t, "FI.MAU.BRIDGE.LOGIN_SUPERSEDED", old.ErrCode)
		waitCancelled(t, oldProc, "the older process")
		assert.EqualValues(t, 0, slowProc.cancelled.Load())
	})

	t.Run("newer process already cancelled by the connector", func(t *testing.T) {
		// e.g. two WhatsApp re-logins of the same number: both submit the phone number inside Start, and the
		// older request's code was issued last, so the connector kept it and cancelled the newer process.
		pt := newProvTest(t, true)
		slowResp, slowProc := pt.startSlowAsync(testUserA)
		newID, newProc := pt.start(testUserA, "phone")
		newProc.supersededByNetwork.Store(true)
		close(pt.net.slowStartGate)

		resp := waitResp(t, slowResp)
		require.Equal(t, http.StatusOK, resp.Status, "the process the connector kept must survive: %s", resp.Body)
		submit := pt.submitPhone(testUserA, resp.LoginID)
		assert.Equal(t, http.StatusOK, submit.Status, "%s", submit.Body)
		assert.EqualValues(t, 0, slowProc.cancelled.Load())
		// The newer process isn't touched by the provisioning API: the connector already cancelled it, and its
		// next step returns the connector's superseded error (TestProvisioning_SupersededByConnector).
		assert.EqualValues(t, 0, newProc.cancelled.Load())
		pt.prov.loginsLock.RLock()
		newState := pt.prov.logins[newID].state
		pt.prov.loginsLock.RUnlock()
		assert.Equal(t, provLoginActive, newState)
	})

	t.Run("other user's newer process", func(t *testing.T) {
		pt := newProvTest(t, true)
		slowResp, slowProc := pt.startSlowAsync(testUserA)
		_, otherProc := pt.start(testUserB, "phone")
		close(pt.net.slowStartGate)
		resp := waitResp(t, slowResp)
		assert.Equal(t, http.StatusOK, resp.Status, "%s", resp.Body)
		assert.EqualValues(t, 0, slowProc.cancelled.Load())
		assert.EqualValues(t, 0, otherProc.cancelled.Load())
	})

	t.Run("superseding disabled", func(t *testing.T) {
		pt := newProvTest(t, false)
		slowResp, slowProc := pt.startSlowAsync(testUserA)
		newID, newProc := pt.start(testUserA, "phone")
		close(pt.net.slowStartGate)
		resp := waitResp(t, slowResp)
		require.Equal(t, http.StatusOK, resp.Status, "%s", resp.Body)
		// Both processes work, like before superseding existed.
		assert.Equal(t, http.StatusOK, pt.submitPhone(testUserA, resp.LoginID).Status)
		assert.Equal(t, http.StatusOK, pt.submitPhone(testUserA, newID).Status)
		assert.EqualValues(t, 0, slowProc.cancelled.Load())
		assert.EqualValues(t, 0, newProc.cancelled.Load())
	})
}

// Many concurrent starts of one user with random Start durations: exactly one process survives, and it's
// the one whose start request arrived last. Every other process is cancelled exactly once.
func TestProvisioning_ConcurrentStartsNewestRequestWins(t *testing.T) {
	const starts = 24
	for round := 0; round < 5; round++ {
		pt := newProvTest(t, true)
		pt.prov.finishedLoginRetention = time.Minute
		var delayLock sync.Mutex
		delays := []time.Duration{0, 1, 3, 7, 15, 25}
		next := round
		pt.net.startDelay = func() time.Duration {
			delayLock.Lock()
			defer delayLock.Unlock()
			next = (next*7 + 3) % len(delays)
			return delays[next] * time.Millisecond
		}
		var wg sync.WaitGroup
		resps := make([]provResp, starts)
		for i := range starts {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resps[i] = pt.do(testUserA, "/v3/login/start/phone", nil)
			}()
		}
		wg.Wait()

		ok := 0
		for _, resp := range resps {
			switch resp.Status {
			case http.StatusOK:
				ok++
			case http.StatusGone:
				assert.Equal(t, "FI.MAU.BRIDGE.LOGIN_SUPERSEDED", resp.ErrCode)
			default:
				t.Fatalf("unexpected start response %d: %s", resp.Status, resp.Body)
			}
		}
		assert.GreaterOrEqual(t, ok, 1)
		require.EqualValues(t, starts, pt.prov.loginStartSeq.Load())

		pt.prov.loginsLock.RLock()
		var active []*ProvLogin
		for _, login := range pt.prov.logins {
			if login.state == provLoginActive {
				active = append(active, login)
			}
		}
		pt.prov.loginsLock.RUnlock()
		require.Len(t, active, 1, "round %d: exactly one process must stay active", round)
		winner := active[0]
		assert.EqualValues(t, starts, winner.startSeq, "round %d: the newest start request must win", round)

		for _, proc := range pt.net.all() {
			if bridgev2.LoginProcess(proc) == winner.Process {
				assert.EqualValues(t, 0, proc.cancelled.Load())
			} else {
				waitCancelled(t, proc, "a superseded process")
			}
		}
		submit := pt.submitPhone(testUserA, winner.ID)
		assert.Equal(t, http.StatusOK, submit.Status, "%s", submit.Body)
	}
}
