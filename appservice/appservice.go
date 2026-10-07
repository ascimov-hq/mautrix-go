// Copyright (c) 2025 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appservice

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"
	"golang.org/x/net/publicsuffix"
	"gopkg.in/yaml.v3"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

// EventChannelSize is the size for the Events channel in Appservice instances.
//
// intbox: 1024 instead of upstream's 64. With synchronous event handling (async_transactions: false) a transaction
// response waits until its last event is in this channel, so a full channel holds up the homeserver's appservice
// sender, which times out after about 35 s and resends the whole transaction. 64 slots filled up under 40 active
// devices' read receipts and during imports ("Event channel is full", load-test U6 and P2). An event is a few KB,
// so 1024 queued events are a few MB at most.
var EventChannelSize = 1024
var OTKChannelSize = 64

// Create creates a blank appservice instance.
func Create() *AppService {
	as := &AppService{
		Log:        zerolog.Nop(),
		clients:    make(map[id.UserID]*mautrix.Client),
		intents:    make(map[id.UserID]*IntentAPI),
		StateStore: mautrix.NewMemoryStateStore().(StateStore),
		Router:     http.NewServeMux(),
		UserAgent:  mautrix.DefaultUserAgent,
		txnIDC:     NewTransactionIDCache(128),
		Live:       true,
		Ready:      false,
		ProcessID:  getDefaultProcessID(),

		Events:         make(chan *event.Event, EventChannelSize),
		ToDeviceEvents: make(chan *event.Event, EventChannelSize),
		OTKCounts:      make(chan *mautrix.OTKCount, OTKChannelSize),
		DeviceLists:    make(chan *mautrix.DeviceLists, EventChannelSize),
		QueryHandler:   &QueryHandlerStub{},

		SpecVersions: &mautrix.RespVersions{},

		DefaultHTTPRetries: 4,

		HomeserverMaxConnsPerPool: mautrix.DefaultIntboxMaxConnsPerPool,
	}
	as.HTTPClient = as.newHomeserverHTTPClient(nil)

	as.Router.HandleFunc("PUT /_matrix/app/v1/transactions/{txnID}", as.PutTransaction)
	as.Router.HandleFunc("GET /_matrix/app/v1/rooms/{roomAlias}", as.GetRoom)
	as.Router.HandleFunc("GET /_matrix/app/v1/users/{userID}", as.GetUser)
	as.Router.HandleFunc("POST /_matrix/app/v1/ping", as.PostPing)
	as.Router.HandleFunc("GET /_matrix/mau/live", as.GetLive)
	as.Router.HandleFunc("GET /_matrix/mau/ready", as.GetReady)

	return as
}

// CreateOpts contains the options for initializing a new [AppService] instance.
type CreateOpts struct {
	// Required, the registration file data for this appservice.
	Registration *Registration
	// Required, the homeserver's server_name.
	HomeserverDomain string
	// Required, the homeserver URL to connect to. Should be either https://address or unix:path
	HomeserverURL string
	// Required if you want to use the standard HTTP server, optional for websockets (non-standard)
	HostConfig HostConfig
	// Optional, defaults to a memory state store
	StateStore StateStore
}

// CreateFull creates a fully configured appservice instance that can be [Start]ed and used directly.
func CreateFull(opts CreateOpts) (*AppService, error) {
	if opts.HomeserverDomain == "" {
		return nil, fmt.Errorf("missing homeserver domain")
	} else if opts.HomeserverURL == "" {
		return nil, fmt.Errorf("missing homeserver URL")
	} else if opts.Registration == nil {
		return nil, fmt.Errorf("missing registration")
	}
	as := Create()
	as.HomeserverDomain = opts.HomeserverDomain
	as.Host = opts.HostConfig
	as.Registration = opts.Registration
	err := as.SetHomeserverURL(opts.HomeserverURL)
	if err != nil {
		return nil, err
	}
	if opts.StateStore != nil {
		as.StateStore = opts.StateStore
	} else {
		as.StateStore = mautrix.NewMemoryStateStore().(StateStore)
	}
	return as, nil
}

var _ StateStore = (*mautrix.MemoryStateStore)(nil)

// QueryHandler handles room alias and user ID queries from the homeserver.
type QueryHandler interface {
	QueryAlias(alias id.RoomAlias) bool
	QueryUser(userID id.UserID) bool
}

type QueryHandlerStub struct{}

func (qh *QueryHandlerStub) QueryAlias(alias id.RoomAlias) bool {
	return false
}

func (qh *QueryHandlerStub) QueryUser(userID id.UserID) bool {
	return false
}

type WebsocketHandler func(WebsocketCommand) (ok bool, data any)

type StateStore interface {
	mautrix.StateStore

	IsRegistered(ctx context.Context, userID id.UserID) (bool, error)
	MarkRegistered(ctx context.Context, userID id.UserID) error

	GetPowerLevel(ctx context.Context, roomID id.RoomID, userID id.UserID) (int, error)
	GetPowerLevelRequirement(ctx context.Context, roomID id.RoomID, eventType event.Type) (int, error)
	HasPowerLevel(ctx context.Context, roomID id.RoomID, userID id.UserID, eventType event.Type) (bool, error)
}

// AppService is the main config for all appservices.
// It also serves as the appservice instance struct.
type AppService struct {
	HomeserverDomain string
	hsURLForClient   *url.URL
	Host             HostConfig

	Registration *Registration
	Log          zerolog.Logger

	txnIDC *TransactionIDCache

	Events         chan *event.Event
	ToDeviceEvents chan *event.Event
	DeviceLists    chan *mautrix.DeviceLists
	OTKCounts      chan *mautrix.OTKCount
	QueryHandler   QueryHandler
	StateStore     StateStore

	Router       *http.ServeMux
	UserAgent    string
	server       *http.Server
	HTTPClient   *http.Client
	botClient    *mautrix.Client
	botIntent    *IntentAPI
	SpecVersions *mautrix.RespVersions

	DefaultHTTPRetries int
	// HomeserverMaxConnsPerPool caps the concurrent connections to each homeserver host, per connection pool of
	// [mautrix.IntboxLaneTransport] (live, bulk writes, backfill writes, background reads and registrations; the
	// pool follows the request's lane, see [mautrix.IntboxLane]). Requests that the reverse proxy delays in the bulk
	// or backfill lane never hold up live or encryption requests, and requests over the cap wait in the bridge
	// instead of opening more sockets.
	// 0 disables this: [AppService.HTTPClient] then uses Go's default transport (no cap, one shared pool).
	// It is applied when the HTTP client is created, so set it before calling [AppService.SetHomeserverURL].
	// Create sets it to [mautrix.DefaultIntboxMaxConnsPerPool].
	HomeserverMaxConnsPerPool int

	// externalClient is the HTTP client for other homeservers (see externalHTTPClient), created on first use.
	externalClient         *http.Client
	externalHTTPClientLock sync.Mutex

	// ShouldQueueEvent, if set, is called for every timeline and ephemeral event of a transaction after the state
	// store has been updated and before the event is put into [AppService.Events]. Returning false drops the event.
	// Bridges use it to drop the echoes of their own events (sent by the bot or a ghost), which every handler
	// ignores anyway, so they no longer take slots in the channel ahead of real users' events.
	// To-device events are never passed to it.
	ShouldQueueEvent func(evt *event.Event) bool

	Live  bool
	Ready bool

	clients     map[id.UserID]*mautrix.Client
	clientsLock sync.RWMutex
	intents     map[id.UserID]*IntentAPI
	intentsLock sync.RWMutex

	ws                    *websocket.Conn
	StopWebsocket         func(error)
	websocketHandlers     map[string]WebsocketHandler
	websocketHandlersLock sync.RWMutex
	websocketRequests     map[int]chan<- *WebsocketCommand
	websocketRequestsLock sync.RWMutex
	websocketRequestID    int32
	// ProcessID is an identifier sent to the websocket proxy for debugging connections
	ProcessID string

	WebsocketTransactionHandler WebsocketTransactionHandler

	DoublePuppetValue string
	GetProfile        func(userID id.UserID, roomID id.RoomID) *event.MemberEventContent
}

const DoublePuppetKey = "fi.mau.double_puppet_source"
const DoublePuppetTSKey = "fi.mau.double_puppet_ts"

func getDefaultProcessID() string {
	pid := syscall.Getpid()
	uid := syscall.Getuid()
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%d", hostname, uid, pid)
}

func (as *AppService) PrepareWebsocket() {
	as.websocketHandlersLock.Lock()
	defer as.websocketHandlersLock.Unlock()
	if as.WebsocketTransactionHandler == nil {
		as.WebsocketTransactionHandler = as.defaultHandleWebsocketTransaction
	}
	if as.websocketHandlers == nil {
		as.websocketHandlers = make(map[string]WebsocketHandler, 32)
		as.websocketHandlers[WebsocketCommandHTTPProxy] = as.WebsocketHTTPProxy
		as.websocketRequests = make(map[int]chan<- *WebsocketCommand)
	}
}

// HostConfig contains info about how to host the appservice.
type HostConfig struct {
	// Hostname can be an IP address or an absolute path for a unix socket.
	Hostname string `yaml:"hostname"`
	// Port is required when Hostname is an IP address, optional for unix sockets
	Port uint16 `yaml:"port"`
}

// Address gets the whole address of the Appservice.
func (hc *HostConfig) Address() string {
	return fmt.Sprintf("%s:%d", hc.Hostname, hc.Port)
}

func (hc *HostConfig) IsUnixSocket() bool {
	return strings.HasPrefix(hc.Hostname, "/")
}

func (hc *HostConfig) IsConfigured() bool {
	return hc.IsUnixSocket() || hc.Port != 0
}

// Save saves this config into a file at the given path.
func (as *AppService) Save(path string) error {
	data, err := yaml.Marshal(as)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// YAML returns the config in YAML format.
func (as *AppService) YAML() (string, error) {
	data, err := yaml.Marshal(as)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// BotMXID returns the user ID corresponding to the appservice's sender_localpart
func (as *AppService) BotMXID() id.UserID {
	if as.botClient != nil {
		return as.botClient.UserID
	}
	return id.NewUserID(as.Registration.SenderLocalpart, as.HomeserverDomain)
}

func (as *AppService) makeIntent(userID id.UserID) *IntentAPI {
	as.intentsLock.Lock()
	defer as.intentsLock.Unlock()

	intent, ok := as.intents[userID]
	if ok {
		return intent
	}

	localpart, homeserver, err := userID.Parse()
	if err != nil || len(localpart) == 0 || homeserver != as.HomeserverDomain {
		if err != nil {
			as.Log.Error().Err(err).
				Str("user_id", userID.String()).
				Msg("Failed to parse user ID")
		} else if len(localpart) == 0 {
			as.Log.Error().Err(err).
				Str("user_id", userID.String()).
				Msg("Failed to make intent: localpart is empty")
		} else if homeserver != as.HomeserverDomain {
			as.Log.Error().Err(err).
				Str("user_id", userID.String()).
				Str("expected_homeserver", as.HomeserverDomain).
				Msg("Failed to make intent: homeserver doesn't match")
		}
		return nil
	}
	intent = as.NewIntentAPI(localpart)
	as.intents[userID] = intent
	return intent
}

// Intent returns an [IntentAPI] object for the given user ID.
//
// This will return nil if the given user ID has an empty localpart,
// or if the server name is not the same as the appservice's HomeserverDomain.
// It does not currently validate that the given user ID is actually in the
// appservice's namespace. Validation may be added later.
func (as *AppService) Intent(userID id.UserID) *IntentAPI {
	if userID == as.BotMXID() {
		return as.BotIntent()
	}
	as.intentsLock.RLock()
	intent, ok := as.intents[userID]
	as.intentsLock.RUnlock()
	if !ok {
		return as.makeIntent(userID)
	}
	return intent
}

// ForgetIntent removes the cached [IntentAPI] and [mautrix.Client] for the given user ID, so that the memory
// can be freed. The next call to [AppService.Intent] or [AppService.Client] will create new instances.
// The bot user is never forgotten.
func (as *AppService) ForgetIntent(userID id.UserID) {
	if userID == as.BotMXID() {
		return
	}
	as.intentsLock.Lock()
	delete(as.intents, userID)
	as.intentsLock.Unlock()
	as.clientsLock.Lock()
	delete(as.clients, userID)
	as.clientsLock.Unlock()
}

// BotIntent returns an [IntentAPI] object for the appservice's sender_localpart user.
func (as *AppService) BotIntent() *IntentAPI {
	if as.botIntent == nil {
		as.botIntent = as.makeIntent(as.BotMXID())
	}
	return as.botIntent
}

// SetHomeserverURL updates the appservice's homeserver URL.
//
// Note that this does not affect already-created [IntentAPI] or [mautrix.Client] objects,
// so it should not be called after Intent or Client are called.
func (as *AppService) SetHomeserverURL(homeserverURL string) error {
	parsedURL, err := url.Parse(homeserverURL)
	if err != nil {
		return err
	}
	copied := *parsedURL
	as.hsURLForClient = &copied
	if as.hsURLForClient.Scheme == "unix" {
		as.hsURLForClient.Scheme = "http"
		as.hsURLForClient.Host = "unix"
		as.hsURLForClient.Path = ""
	} else if as.hsURLForClient.Scheme == "" {
		as.hsURLForClient.Scheme = "https"
	}
	as.hsURLForClient.RawPath = as.hsURLForClient.EscapedPath()

	var base *http.Transport
	if parsedURL.Scheme == "unix" {
		base = &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", parsedURL.Path)
			},
		}
	}
	as.HTTPClient = as.newHomeserverHTTPClient(base)
	return nil
}

// newHomeserverHTTPClient creates the HTTP client used for homeserver requests. base is the transport to build on
// (nil means Go's default transport). With HomeserverMaxConnsPerPool > 0, every lane gets a capped clone of it.
func (as *AppService) newHomeserverHTTPClient(base *http.Transport) *http.Client {
	jar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	client := &http.Client{Timeout: 180 * time.Second, Jar: jar}
	if as.HomeserverMaxConnsPerPool > 0 {
		client.Transport = mautrix.NewIntboxLaneTransport(base, as.HomeserverMaxConnsPerPool)
	} else if base != nil {
		client.Transport = base
	}
	return client
}

// NewMautrixClient creates a new [mautrix.Client] instance for the given user ID.
//
// This does not do any validation, and it does not cache the client.
// Usually you should prefer [AppService.Client] or [AppService.Intent] over this method.
func (as *AppService) NewMautrixClient(userID id.UserID) *mautrix.Client {
	return &mautrix.Client{
		HomeserverURL:       as.hsURLForClient,
		UserID:              userID,
		SetAppServiceUserID: true,
		AccessToken:         as.Registration.AppToken,
		UserAgent:           as.UserAgent,
		StateStore:          as.StateStore,
		Log:                 as.Log.With().Stringer("as_user_id", userID).Logger(),
		Client:              as.HTTPClient,
		DefaultHTTPRetries:  as.DefaultHTTPRetries,
		SpecVersions:        as.SpecVersions,
	}
}

// NewExternalMautrixClient creates a new [mautrix.Client] instance for an external user,
// with a token and homeserver URL separate from the main appservice.
//
// This is primarily meant to facilitate double puppeting in bridges, and is used by [bridge.doublePuppetUtil].
// Non-bridge appservices will likely not need this.
//
// A homeserverURL with the same scheme and host as the appservice's own homeserver (for example when the bridge's
// own server is listed in double_puppet.servers) keeps the appservice's HTTP client, so double puppets share its
// capped connection pools. Other homeservers share one separate client (see externalHTTPClient).
func (as *AppService) NewExternalMautrixClient(userID id.UserID, token string, homeserverURL string) (*mautrix.Client, error) {
	client := as.NewMautrixClient(userID)
	client.AccessToken = token
	client.SetAppServiceUserID = false
	if homeserverURL != "" {
		parsedURL, err := mautrix.ParseAndNormalizeBaseURL(homeserverURL)
		if err != nil {
			return nil, err
		}
		client.HomeserverURL = parsedURL
		if !sameHomeserverHost(parsedURL, as.hsURLForClient) {
			client.Client = as.externalHTTPClient()
		}
	}
	return client, nil
}

// externalHTTPClient returns the HTTP client shared by all clients for other homeservers (double puppets on other
// servers): the same 180 s timeout as before, and with HomeserverMaxConnsPerPool > 0 its own
// [mautrix.IntboxLaneTransport], so the per-host caps apply there too. It used to be a new client with Go's default
// transport (no cap) per double puppet.
func (as *AppService) externalHTTPClient() *http.Client {
	as.externalHTTPClientLock.Lock()
	defer as.externalHTTPClientLock.Unlock()
	if as.externalClient == nil {
		as.externalClient = &http.Client{Timeout: 180 * time.Second}
		if as.HomeserverMaxConnsPerPool > 0 {
			as.externalClient.Transport = mautrix.NewIntboxLaneTransport(nil, as.HomeserverMaxConnsPerPool)
		}
	}
	return as.externalClient
}

// sameHomeserverHost reports whether two homeserver base URLs reach the same server: same scheme, host and port
// (the default port when none is given), compared case-insensitively. Paths don't matter: the HTTP client's
// connections are per host.
func sameHomeserverHost(a, b *url.URL) bool {
	if a == nil || b == nil || !strings.EqualFold(a.Scheme, b.Scheme) {
		return false
	}
	hostPort := func(u *url.URL) string {
		port := u.Port()
		if port == "" {
			switch strings.ToLower(u.Scheme) {
			case "http":
				port = "80"
			case "https":
				port = "443"
			}
		}
		return net.JoinHostPort(strings.ToLower(u.Hostname()), port)
	}
	return hostPort(a) == hostPort(b)
}

func (as *AppService) makeClient(userID id.UserID) *mautrix.Client {
	as.clientsLock.Lock()
	defer as.clientsLock.Unlock()

	client, ok := as.clients[userID]
	if !ok {
		client = as.NewMautrixClient(userID)
		as.clients[userID] = client
	}
	return client
}

// Client returns the [mautrix.Client] instance for the given user ID.
//
// Unlike [AppService.Intent], this does not do any validation, and will always return a value.
// Usually you should prefer creating intents and using intent methods over direct client methods.
// You can always access the client inside an intent with [IntentAPI.Client].
func (as *AppService) Client(userID id.UserID) *mautrix.Client {
	if userID == as.BotMXID() {
		return as.BotClient()
	}
	as.clientsLock.RLock()
	client, ok := as.clients[userID]
	as.clientsLock.RUnlock()
	if !ok {
		return as.makeClient(userID)
	}
	return client
}

// BotClient returns the [mautrix.Client] instance for the appservice's sender_localpart user.
//
// Like with the generic Client method, [AppService.BotIntent] should be preferred over this.
func (as *AppService) BotClient() *mautrix.Client {
	if as.botClient == nil {
		as.botClient = as.makeClient(as.BotMXID())
	}
	return as.botClient
}
