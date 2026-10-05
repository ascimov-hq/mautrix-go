// Copyright (c) 2023 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appservice

import (
	"context"
	"encoding/json"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type ExecMode uint8

const (
	AsyncHandlers ExecMode = iota
	AsyncLoop
	Sync
	// SyncPerRoom (intbox) handles the events of one room one at a time, in the order they arrived, like Sync, but
	// different rooms in parallel. With Sync, one slow event (a portal busy with an import, a 3 s wait for a megolm
	// key, a slow database) held up every room's events behind it: in load test L5 the app -> WhatsApp messages of
	// users who weren't importing anything waited about 200 s behind other users' imports. AsyncHandlers has no such
	// stall but gives up the order within a room (3 % of messages, more than half of the chats, in the same test).
	// At most RoomQueueMaxPending events are queued or being handled across all rooms; beyond that, reading from
	// the Events channel waits, so the homeserver is slowed down as with Sync.
	SyncPerRoom
)

// DefaultRoomQueueMaxPending is the default for EventProcessor.RoomQueueMaxPending.
var DefaultRoomQueueMaxPending = 4096

type roomEventQueue struct {
	events []*event.Event
}

type EventHandler = func(ctx context.Context, evt *event.Event)
type OTKHandler = func(ctx context.Context, otk *mautrix.OTKCount)
type DeviceListHandler = func(ctx context.Context, lists *mautrix.DeviceLists, since string)

type EventProcessor struct {
	ExecMode ExecMode

	ExecSyncWarnTime time.Duration
	ExecSyncTimeout  time.Duration

	// RoomQueueMaxPending is how many events SyncPerRoom holds at most, queued or being handled, across all rooms
	// (0 means DefaultRoomQueueMaxPending). It's read once, in Start.
	RoomQueueMaxPending int

	roomLock   sync.Mutex
	roomQueues map[id.RoomID]*roomEventQueue
	roomSlots  chan struct{}

	// inHand (intbox) counts events taken from the Events channel by the event loop that are neither in a room
	// queue nor handled yet: the event Sync/AsyncLoop is dispatching, or the one SyncPerRoom waits a slot for.
	inHand atomic.Int64

	as       *AppService
	stop     chan struct{}
	handlers map[event.Type][]EventHandler

	otkHandlers        []OTKHandler
	deviceListHandlers []DeviceListHandler
}

func NewEventProcessor(as *AppService) *EventProcessor {
	return &EventProcessor{
		ExecMode: AsyncHandlers,
		as:       as,
		stop:     make(chan struct{}, 1),
		handlers: make(map[event.Type][]EventHandler),

		ExecSyncWarnTime: 30 * time.Second,
		ExecSyncTimeout:  15 * time.Minute,

		otkHandlers:        make([]OTKHandler, 0),
		deviceListHandlers: make([]DeviceListHandler, 0),
	}
}

func (ep *EventProcessor) On(evtType event.Type, handler EventHandler) {
	handlers, ok := ep.handlers[evtType]
	if !ok {
		handlers = []EventHandler{handler}
	} else {
		handlers = append(handlers, handler)
	}
	ep.handlers[evtType] = handlers
}

func (ep *EventProcessor) PrependHandler(evtType event.Type, handler EventHandler) {
	handlers, ok := ep.handlers[evtType]
	if !ok {
		handlers = []EventHandler{handler}
	} else {
		handlers = append([]EventHandler{handler}, handlers...)
	}
	ep.handlers[evtType] = handlers
}

func (ep *EventProcessor) OnOTK(handler OTKHandler) {
	ep.otkHandlers = append(ep.otkHandlers, handler)
}

func (ep *EventProcessor) OnDeviceList(handler DeviceListHandler) {
	ep.deviceListHandlers = append(ep.deviceListHandlers, handler)
}

func (ep *EventProcessor) recoverFunc(data interface{}) {
	if err := recover(); err != nil {
		d, _ := json.Marshal(data)
		ep.as.Log.Error().
			Str(zerolog.ErrorStackFieldName, string(debug.Stack())).
			Interface(zerolog.ErrorFieldName, err).
			Str("event_content", string(d)).
			Msg("Panic in Matrix event handler")
	}
}

func (ep *EventProcessor) callHandler(ctx context.Context, handler EventHandler, evt *event.Event) {
	defer ep.recoverFunc(evt)
	handler(ctx, evt)
}

func (ep *EventProcessor) callOTKHandler(ctx context.Context, handler OTKHandler, otk *mautrix.OTKCount) {
	defer ep.recoverFunc(otk)
	handler(ctx, otk)
}

func (ep *EventProcessor) callDeviceListHandler(ctx context.Context, handler DeviceListHandler, dl *mautrix.DeviceLists) {
	defer ep.recoverFunc(dl)
	handler(ctx, dl, "")
}

func (ep *EventProcessor) DispatchOTK(ctx context.Context, otk *mautrix.OTKCount) {
	for _, handler := range ep.otkHandlers {
		go ep.callOTKHandler(ctx, handler, otk)
	}
}

func (ep *EventProcessor) DispatchDeviceList(ctx context.Context, dl *mautrix.DeviceLists) {
	for _, handler := range ep.deviceListHandlers {
		go ep.callDeviceListHandler(ctx, handler, dl)
	}
}

func (ep *EventProcessor) Dispatch(ctx context.Context, evt *event.Event) {
	handlers, ok := ep.handlers[evt.Type]
	if !ok {
		return
	}
	switch ep.ExecMode {
	case AsyncHandlers:
		for _, handler := range handlers {
			go ep.callHandler(ctx, handler, evt)
		}
	case AsyncLoop:
		go func() {
			for _, handler := range handlers {
				ep.callHandler(ctx, handler, evt)
			}
		}()
	case Sync, SyncPerRoom:
		if ep.ExecSyncWarnTime == 0 && ep.ExecSyncTimeout == 0 {
			for _, handler := range handlers {
				ep.callHandler(ctx, handler, evt)
			}
			return
		}
		doneChan := make(chan struct{})
		go func() {
			for _, handler := range handlers {
				ep.callHandler(ctx, handler, evt)
			}
			close(doneChan)
		}()
		select {
		case <-doneChan:
			return
		case <-time.After(ep.ExecSyncWarnTime):
			log := ep.as.Log.With().
				Str("event_id", evt.ID.String()).
				Str("event_type", evt.Type.String()).
				Logger()
			log.Warn().Msg("Handling event in appservice transaction channel is taking long")
			select {
			case <-doneChan:
				return
			case <-time.After(ep.ExecSyncTimeout):
				log.Error().Msg("Giving up waiting for event handler")
			}
		}
	}
}
func (ep *EventProcessor) startEvents(ctx context.Context) {
	if ep.ExecMode == SyncPerRoom {
		ep.startEventsPerRoom(ctx)
		return
	}
	for {
		select {
		case evt := <-ep.as.Events:
			ep.inHand.Add(1)
			ep.Dispatch(ctx, evt)
			ep.inHand.Add(-1)
		case <-ep.stop:
			return
		}
	}
}

func (ep *EventProcessor) startEncryption(ctx context.Context) {
	for {
		select {
		case evt := <-ep.as.ToDeviceEvents:
			ep.Dispatch(ctx, evt)
		case otk := <-ep.as.OTKCounts:
			ep.DispatchOTK(ctx, otk)
		case dl := <-ep.as.DeviceLists:
			ep.DispatchDeviceList(ctx, dl)
		case <-ep.stop:
			return
		}
	}
}

// startEventsPerRoom is the SyncPerRoom loop: it moves each event from the Events channel to its room's queue as
// soon as a slot is free, and every room with queued events has one goroutine that handles them in order.
func (ep *EventProcessor) startEventsPerRoom(ctx context.Context) {
	var lastFullWarning time.Time
	for {
		select {
		case evt := <-ep.as.Events:
			ep.inHand.Add(1)
			select {
			case ep.roomSlots <- struct{}{}:
			default:
				if time.Since(lastFullWarning) >= 10*time.Second {
					lastFullWarning = time.Now()
					ep.as.Log.Warn().
						Int("max_pending", cap(ep.roomSlots)).
						Str("event_id", evt.ID.String()).
						Str("room_id", evt.RoomID.String()).
						Msg("Per-room event queues are full, waiting for a free slot")
				}
				select {
				case ep.roomSlots <- struct{}{}:
				case <-ep.stop:
					ep.inHand.Add(-1)
					return
				}
			}
			ep.queueRoomEvent(ctx, evt)
			ep.inHand.Add(-1)
		case <-ep.stop:
			return
		}
	}
}

func (ep *EventProcessor) queueRoomEvent(ctx context.Context, evt *event.Event) {
	ep.roomLock.Lock()
	queue, running := ep.roomQueues[evt.RoomID]
	if !running {
		queue = &roomEventQueue{}
		ep.roomQueues[evt.RoomID] = queue
	}
	queue.events = append(queue.events, evt)
	ep.roomLock.Unlock()
	if !running {
		go ep.runRoomQueue(ctx, evt.RoomID, queue)
	}
}

// runRoomQueue handles the room's events one at a time and exits when the queue is empty. queueRoomEvent starts a
// new one for the next event, so there is at most one per room at any time.
func (ep *EventProcessor) runRoomQueue(ctx context.Context, roomID id.RoomID, queue *roomEventQueue) {
	for {
		ep.roomLock.Lock()
		if len(queue.events) == 0 {
			delete(ep.roomQueues, roomID)
			ep.roomLock.Unlock()
			return
		}
		evt := queue.events[0]
		queue.events[0] = nil
		queue.events = queue.events[1:]
		ep.roomLock.Unlock()
		ep.Dispatch(ctx, evt)
		<-ep.roomSlots
	}
}

// PendingRoomEvents returns how many events SyncPerRoom holds (queued or being handled) and in how many rooms.
// It is (0, 0) in the other modes.
func (ep *EventProcessor) PendingRoomEvents() (events, rooms int) {
	ep.roomLock.Lock()
	defer ep.roomLock.Unlock()
	return len(ep.roomSlots), len(ep.roomQueues)
}

// PendingEvents (intbox) returns how many Matrix events the appservice has accepted (the homeserver got its
// transaction response) that haven't been handled yet: waiting in the Events channel, taken by the event loop, or
// queued or being handled by SyncPerRoom. A bridge stop waits for it to reach 0 before disconnecting, so that these
// events aren't dropped (load-test L6-2). Handlers that AsyncHandlers/AsyncLoop started in the background and
// to-device events aren't counted.
func (ep *EventProcessor) PendingEvents() int {
	pending := len(ep.as.Events) + int(ep.inHand.Load())
	ep.roomLock.Lock()
	if ep.roomSlots != nil {
		pending += len(ep.roomSlots)
	}
	ep.roomLock.Unlock()
	return pending
}

func (ep *EventProcessor) Start(ctx context.Context) {
	if ep.ExecMode == SyncPerRoom {
		maxPending := ep.RoomQueueMaxPending
		if maxPending <= 0 {
			maxPending = DefaultRoomQueueMaxPending
		}
		ep.roomLock.Lock()
		ep.roomQueues = make(map[id.RoomID]*roomEventQueue)
		ep.roomSlots = make(chan struct{}, maxPending)
		ep.roomLock.Unlock()
	}
	go ep.startEvents(ctx)
	go ep.startEncryption(ctx)
}

func (ep *EventProcessor) Stop() {
	close(ep.stop)
}
