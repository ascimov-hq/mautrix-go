// Copyright (c) 2024 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package bridgev2

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/ptr"
	"go.mau.fi/util/variationselector"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func (portal *Portal) doForwardBackfill(ctx context.Context, source *UserLogin, lastMessage *database.Message, bundledData any) {
	ctx = WithIntboxLane(ctx, mautrix.IntboxLaneBackfill, source)
	log := zerolog.Ctx(ctx).With().Str("action", "forward backfill").Logger()
	if !portal.forwardBackfillLock.TryLock() {
		log.Warn().Msg("Another forward backfill is already running")
		return
	}
	defer portal.forwardBackfillLock.Unlock()
	ctx = log.WithContext(ctx)
	api, ok := source.Client.(BackfillingNetworkAPI)
	if !ok {
		log.Debug().Msg("Network API does not support backfilling")
		return
	}
	var limit int
	var latestMessageID string
	if lastMessage != nil {
		latestMessageID = string(lastMessage.ID)
		limit = portal.Bridge.Config.Backfill.MaxCatchupMessages
	} else {
		limit = portal.Bridge.Config.Backfill.MaxInitialMessages
	}
	if limit <= 0 {
		return
	}
	log.Info().Str("latest_message_id", latestMessageID).Msg("Fetching messages for forward backfill")
	resp, err := api.FetchMessages(ctx, FetchMessagesParams{
		Portal:        portal,
		ThreadRoot:    "",
		Forward:       true,
		AnchorMessage: lastMessage,
		Count:         limit,
		BundledData:   bundledData,
	})
	if err != nil {
		log.Err(err).Msg("Failed to fetch messages for forward backfill")
		return
	} else if resp == nil {
		log.Debug().Msg("Didn't get backfill response")
		return
	} else if len(resp.Messages) == 0 {
		log.Debug().Msg("No messages to backfill")
		if resp.CompleteCallback != nil {
			resp.CompleteCallback()
		}
		return
	}
	log.Debug().
		Int("message_count", len(resp.Messages)).
		Bool("mark_read", resp.MarkRead).
		Bool("aggressive_deduplication", resp.AggressiveDeduplication).
		Str("queried_latest_message_id", latestMessageID).
		Msg("Fetched messages for forward backfill, deduplicating before sending")
	// TODO mark backfill queue task as done if last message is nil (-> room was empty) and HasMore is false?
	resp.Messages = portal.cutoffMessages(ctx, resp.Messages, resp.AggressiveDeduplication, true, lastMessage)
	if len(resp.Messages) == 0 {
		log.Warn().Msg("No messages left to backfill after cutting off old messages")
		if resp.CompleteCallback != nil {
			resp.CompleteCallback()
		}
		return
	}
	err = portal.sendBackfill(ctx, source, resp.Messages, true, resp.MarkRead, false, resp.CompleteCallback)
	if err != nil {
		log.Err(err).Msg("Failed to send forward backfill")
	}
}

var errNoMessagesLeftAfterCutoff = errors.New("no messages left to backfill after cutting off too new messages")

func (portal *Portal) doBackwardsBackfill(ctx context.Context, source *UserLogin, task *database.BackfillTask, resp *FetchMessagesResponse) (bool, error) {
	ctx = WithIntboxLane(ctx, mautrix.IntboxLaneBackfill, source)
	log := zerolog.Ctx(ctx)
	api, ok := source.Client.(BackfillingNetworkAPI)
	if !ok {
		return false, fmt.Errorf("network API does not support backfilling")
	}
	firstMessage, err := portal.Bridge.DB.Message.GetFirstPortalMessage(ctx, portal.PortalKey)
	if err != nil {
		return false, fmt.Errorf("failed to get first portal message: %w", err)
	}
	logEvt := log.Info().
		Str("cursor", string(task.Cursor)).
		Str("task_oldest_message_id", string(task.OldestMessageID)).
		Int("current_batch_count", task.BatchCount)
	if firstMessage != nil {
		logEvt = logEvt.Str("db_oldest_message_id", string(firstMessage.ID))
	} else {
		logEvt = logEvt.Str("db_oldest_message_id", "")
	}
	if resp == nil {
		logEvt.Msg("Fetching messages for backward backfill")
		resp, err = api.FetchMessages(ctx, FetchMessagesParams{
			Portal:         portal,
			ThreadRoot:     "",
			Forward:        false,
			Cursor:         task.Cursor,
			AnchorMessage:  firstMessage,
			Count:          portal.Bridge.Config.Backfill.Queue.BatchSize,
			Task:           task,
			AllowSlowFetch: !task.FromQueue || !portal.Bridge.Config.Backfill.Queue.Manual,
		})
		if err != nil {
			return false, fmt.Errorf("failed to fetch messages for backward backfill: %w", err)
		} else if resp == nil {
			log.Debug().Msg("Didn't get backfill response, marking task as done")
			task.IsDone = true
			task.QueueDone = true
			return false, nil
		} else if resp.Pending {
			log.Debug().Msg("Backfill response is pending")
			// TODO make configurable by admin and/or network connector?
			task.NextDispatchMinTS = time.Now().Add(24 * time.Hour)
			return true, nil
		} else if resp.MoreRequiresSlowFetch {
			if !task.FromQueue {
				log.Warn().Msg("Backfill response indicates more messages require slow fetching even though task is not from queue")
			} else {
				log.Debug().Msg("Backfill response indicates more messages require slow fetching, setting queue done flag")
			}
			task.QueueDone = true
			return true, nil
		}
	} else {
		log.Debug().Msg("Using data from event for backwards backfill")
	}
	log.Debug().
		Str("new_cursor", string(resp.Cursor)).
		Bool("has_more", resp.HasMore).
		Int("message_count", len(resp.Messages)).
		Msg("Fetched messages for backward backfill")
	task.Cursor = resp.Cursor
	if !resp.HasMore {
		task.IsDone = true
		task.QueueDone = true
	}
	if len(resp.Messages) == 0 {
		if !resp.HasMore {
			log.Debug().Msg("No messages to backfill, marking backfill task as done")
		} else {
			log.Warn().Msg("No messages to backfill, but HasMore is true")
		}
		if resp.CompleteCallback != nil {
			resp.CompleteCallback()
		}
		return false, nil
	}
	resp.Messages = portal.cutoffMessages(ctx, resp.Messages, resp.AggressiveDeduplication, false, firstMessage)
	if len(resp.Messages) == 0 {
		if resp.CompleteCallback != nil {
			resp.CompleteCallback()
		}
		// Hack to handle some migrated portals where message timestamps are unknown.
		// They can't be backfilled further, so just mark them as done.
		if firstMessage != nil && firstMessage.Timestamp.Unix() == 0 {
			task.IsDone = true
			task.QueueDone = true
			return false, nil
		}
		return false, errNoMessagesLeftAfterCutoff
	}
	err = portal.sendBackfill(ctx, source, resp.Messages, false, resp.MarkRead, false, resp.CompleteCallback)
	if err != nil {
		return false, fmt.Errorf("failed to send backward backfill: %w", err)
	}
	if len(resp.Messages) > 0 {
		task.OldestMessageID = resp.Messages[0].ID
	}
	return false, nil
}

func (portal *Portal) fetchThreadBackfill(ctx context.Context, source *UserLogin, anchor *database.Message) *FetchMessagesResponse {
	log := zerolog.Ctx(ctx)
	resp, err := source.Client.(BackfillingNetworkAPI).FetchMessages(ctx, FetchMessagesParams{
		Portal:        portal,
		ThreadRoot:    anchor.ID,
		Forward:       true,
		AnchorMessage: anchor,
		Count:         portal.Bridge.Config.Backfill.Threads.MaxInitialMessages,
	})
	if err != nil {
		log.Err(err).Msg("Failed to fetch messages for thread backfill")
		return nil
	} else if resp == nil {
		log.Debug().Msg("Didn't get backfill response")
		return nil
	} else if len(resp.Messages) == 0 {
		log.Debug().Msg("No messages to backfill")
		return nil
	}
	resp.Messages = portal.cutoffMessages(ctx, resp.Messages, resp.AggressiveDeduplication, true, anchor)
	if len(resp.Messages) == 0 {
		log.Warn().Msg("No messages left to backfill after cutting off old messages")
		if resp.CompleteCallback != nil {
			resp.CompleteCallback()
		}
		return nil
	}
	return resp
}

func (portal *Portal) doThreadBackfill(ctx context.Context, source *UserLogin, threadID networkid.MessageID) error {
	ctx = WithIntboxLane(ctx, mautrix.IntboxLaneBackfill, source)
	log := zerolog.Ctx(ctx).With().
		Str("subaction", "thread backfill").
		Str("thread_id", string(threadID)).
		Logger()
	ctx = log.WithContext(ctx)
	log.Info().Msg("Backfilling thread inside other backfill")
	anchorMessage, err := portal.Bridge.DB.Message.GetLastThreadMessage(ctx, portal.PortalKey, threadID)
	if err != nil {
		return fmt.Errorf("failed to get last thread message: %w", err)
	} else if anchorMessage == nil {
		log.Warn().Msg("No messages found in thread?")
		return nil
	}
	resp := portal.fetchThreadBackfill(ctx, source, anchorMessage)
	if resp != nil {
		return portal.sendBackfill(ctx, source, resp.Messages, true, resp.MarkRead, true, resp.CompleteCallback)
	}
	return nil
}

func (portal *Portal) cutoffMessages(ctx context.Context, messages []*BackfillMessage, aggressiveDedup, forward bool, lastMessage *database.Message) []*BackfillMessage {
	if lastMessage == nil {
		return messages
	}
	if forward {
		cutoff := -1
		for i, msg := range messages {
			if msg.ID == lastMessage.ID || msg.Timestamp.Before(lastMessage.Timestamp) {
				cutoff = i
			} else {
				break
			}
		}
		if cutoff != -1 {
			zerolog.Ctx(ctx).Debug().
				Int("cutoff_count", cutoff+1).
				Int("total_count", len(messages)).
				Time("last_bridged_ts", lastMessage.Timestamp).
				Msg("Cutting off forward backfill messages older than latest bridged message")
			messages = messages[cutoff+1:]
		}
	} else {
		cutoff := -1
		for i := len(messages) - 1; i >= 0; i-- {
			if messages[i].ID == lastMessage.ID || messages[i].Timestamp.After(lastMessage.Timestamp) {
				cutoff = i
			} else {
				break
			}
		}
		if cutoff != -1 {
			zerolog.Ctx(ctx).Debug().
				Int("cutoff_count", len(messages)-cutoff).
				Int("total_count", len(messages)).
				Time("oldest_bridged_ts", lastMessage.Timestamp).
				Msg("Cutting off backward backfill messages newer than oldest bridged message")
			messages = messages[:cutoff]
		}
	}
	if aggressiveDedup {
		filteredMessages := messages[:0]
		for _, msg := range messages {
			existingMsg, err := portal.Bridge.DB.Message.GetFirstPartByID(ctx, portal.Receiver, msg.ID)
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Str("message_id", string(msg.ID)).Msg("Failed to check for existing message")
			} else if existingMsg != nil {
				zerolog.Ctx(ctx).Err(err).
					Str("message_id", string(msg.ID)).
					Time("message_ts", msg.Timestamp).
					Str("message_sender", string(msg.Sender.Sender)).
					Msg("Ignoring duplicate message in backfill")
				continue
			}
			if forward && msg.TxnID != "" {
				wasPending, _ := portal.checkPendingMessage(ctx, msg)
				if wasPending {
					zerolog.Ctx(ctx).Err(err).
						Str("transaction_id", string(msg.TxnID)).
						Str("message_id", string(msg.ID)).
						Time("message_ts", msg.Timestamp).
						Msg("Found pending message in backfill")
					continue
				}
			}
			filteredMessages = append(filteredMessages, msg)
		}
		messages = filteredMessages
	}
	return messages
}

func (portal *Portal) sendBackfill(
	ctx context.Context,
	source *UserLogin,
	messages []*BackfillMessage,
	forceForward,
	markRead,
	inThread bool,
	done func(),
) error {
	canBatchSend := portal.Bridge.Matrix.GetCapabilities().BatchSending
	unreadThreshold := time.Duration(portal.Bridge.Config.Backfill.UnreadHoursThreshold) * time.Hour
	forceMarkRead := unreadThreshold > 0 && time.Since(messages[len(messages)-1].Timestamp) > unreadThreshold
	zerolog.Ctx(ctx).Info().
		Int("message_count", len(messages)).
		Bool("batch_send", canBatchSend).
		Bool("mark_read", markRead).
		Bool("mark_read_past_threshold", forceMarkRead).
		Msg("Sending backfill messages")
	var err error
	if canBatchSend {
		err = portal.sendBatch(ctx, source, messages, forceForward, markRead || forceMarkRead, inThread)
	} else {
		err = portal.sendLegacyBackfill(ctx, source, messages, markRead || forceMarkRead)
	}
	if err != nil {
		return err
	}
	if done != nil {
		done()
	}
	zerolog.Ctx(ctx).Debug().Msg("Backfill finished")
	// Threads are backfilled after the main batch in both modes: batch send segments can't contain
	// events that relate to each other, as the event IDs are only known after sending.
	if !inThread && portal.Bridge.Config.Backfill.Threads.MaxInitialMessages > 0 {
		for _, msg := range messages {
			if msg.ShouldBackfillThread {
				err = portal.doThreadBackfill(ctx, source, msg.ID)
				if err != nil {
					zerolog.Ctx(ctx).Debug().
						Str("thread_id", string(msg.ID)).
						Msg("Failed to backfill thread")
				}
			}
		}
	}
	return nil
}

// maxBatchSendEvents is the maximum number of events sent in a single batch send request. The homeserver
// holds the room lock while it handles the whole batch, so large batches would block live sends in the room.
const maxBatchSendEvents = 200

type compileBatchOutput struct {
	PrevThreadEvents map[networkid.MessageID]id.EventID

	Events []*event.Event
	Extras []*MatrixSendExtra

	DBMessages  []*database.Message
	DBReactions []*database.Reaction
	Disappear   []*database.DisappearingMessage

	// Reactions to the messages in this segment. They're compiled into the next segment,
	// after the homeserver has returned the event IDs of the messages they target.
	PendingReactions []*pendingBatchReactions

	// Messages and threads in this segment, used to detect relations that point to events
	// whose event IDs aren't known yet because the segment hasn't been sent.
	unsentMessages map[networkid.MessageID]struct{}
	unsentThreads  map[networkid.MessageID]struct{}
	// Senders whose membership has already been ensured during this batch (shared by all segments).
	joinedSenders map[id.UserID]struct{}
}

type pendingBatchReactions struct {
	Message *BackfillMessage
	PartIDs []networkid.PartID
	PartMap map[networkid.PartID]*database.Message
}

func newCompileBatchOutput(capacity int, joinedSenders map[id.UserID]struct{}) *compileBatchOutput {
	return &compileBatchOutput{
		PrevThreadEvents: make(map[networkid.MessageID]id.EventID),
		Events:           make([]*event.Event, 0, capacity),
		Extras:           make([]*MatrixSendExtra, 0, capacity),
		DBMessages:       make([]*database.Message, 0, capacity),
		DBReactions:      make([]*database.Reaction, 0),
		Disappear:        make([]*database.DisappearingMessage, 0),

		unsentMessages: make(map[networkid.MessageID]struct{}),
		unsentThreads:  make(map[networkid.MessageID]struct{}),
		joinedSenders:  joinedSenders,
	}
}

// dependsOnUnsentEvent returns true if the given message replies to or continues a thread with
// a message in the current segment. Such messages must go into the next segment, because the
// event IDs are assigned by the homeserver and are only known after the segment has been sent.
func (out *compileBatchOutput) dependsOnUnsentEvent(msg *BackfillMessage) bool {
	if len(out.Events) == 0 {
		return false
	}
	if msg.ReplyTo != nil {
		if _, ok := out.unsentMessages[msg.ReplyTo.MessageID]; ok {
			return true
		}
	}
	if msg.ThreadRoot != nil {
		_, rootUnsent := out.unsentMessages[*msg.ThreadRoot]
		_, threadUnsent := out.unsentThreads[*msg.ThreadRoot]
		return rootUnsent || threadUnsent
	}
	return false
}

// applyEventIDs replaces the pre-generated event IDs of the segment with the event IDs returned by
// the homeserver, which are what the events actually got in the room. The database rows share the
// pointers in Extras, so they're updated too.
func (out *compileBatchOutput) applyEventIDs(resp *mautrix.RespBeeperBatchSend) error {
	if resp == nil || len(resp.EventIDs) != len(out.Events) || len(out.Extras) != len(out.Events) {
		var gotIDs int
		if resp != nil {
			gotIDs = len(resp.EventIDs)
		}
		return fmt.Errorf("homeserver returned %d event IDs for %d events", gotIDs, len(out.Events))
	}
	replaced := make(map[id.EventID]id.EventID)
	for i, extra := range out.Extras {
		newID := resp.EventIDs[i]
		oldID := out.Events[i].ID
		if newID == "" {
			return fmt.Errorf("homeserver returned empty event ID at index %d", i)
		} else if newID == oldID {
			continue
		}
		replaced[oldID] = newID
		out.Events[i].ID = newID
		if extra.MessageMeta != nil {
			extra.MessageMeta.MXID = newID
		}
		if extra.ReactionMeta != nil {
			extra.ReactionMeta.MXID = newID
		}
	}
	for _, disappear := range out.Disappear {
		if newID, ok := replaced[disappear.EventID]; ok {
			disappear.EventID = newID
		}
	}
	return nil
}

// ensureBatchSenderJoined makes sure that the sender of a batch send event is in the room before
// the batch is sent, as the homeserver rejects events from users who aren't joined.
func (portal *Portal) ensureBatchSenderJoined(ctx context.Context, intent MatrixAPI, out *compileBatchOutput) {
	userID := intent.GetMXID()
	if _, alreadyChecked := out.joinedSenders[userID]; alreadyChecked {
		return
	}
	out.joinedSenders[userID] = struct{}{}
	if userID == portal.Bridge.Bot.GetMXID() {
		return
	}
	err := intent.EnsureJoined(ctx, portal.MXID)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).
			Stringer("user_id", userID).
			Msg("Failed to ensure batch send event sender is joined to the room")
	}
}

func (portal *Portal) compileBatchMessage(ctx context.Context, source *UserLogin, msg *BackfillMessage, out *compileBatchOutput, inThread bool) {
	if len(msg.Parts) == 0 {
		return
	}
	intent, ok := portal.GetIntentFor(ctx, msg.Sender, source, RemoteEventMessage)
	if !ok {
		return
	}
	portal.ensureBatchSenderJoined(ctx, intent, out)
	// Relation targets must be real events: messages that are being sent in the same batch are always
	// in an earlier segment (see dependsOnUnsentEvent), so they're already in the database.
	replyTo, threadRoot, prevThreadEvent := portal.getRelationMeta(
		ctx, msg.ID, msg.ConvertedMessage, false,
	)
	if threadRoot != nil && out.PrevThreadEvents[*msg.ThreadRoot] != "" {
		prevThreadEvent.MXID = out.PrevThreadEvents[*msg.ThreadRoot]
	}
	out.unsentMessages[msg.ID] = struct{}{}
	if msg.ThreadRoot != nil {
		out.unsentThreads[*msg.ThreadRoot] = struct{}{}
	}
	var partIDs []networkid.PartID
	partMap := make(map[networkid.PartID]*database.Message, len(msg.Parts))
	for i, part := range msg.Parts {
		partIDs = append(partIDs, part.ID)
		portal.applyRelationMeta(ctx, part.Content, replyTo, threadRoot, prevThreadEvent)
		part.Content.BeeperDisappearingTimer = msg.Disappear.ToEventContent()
		// This is only a placeholder, it's replaced with the event ID returned by the homeserver after sending.
		evtID := portal.Bridge.Matrix.GenerateDeterministicEventID(portal.MXID, portal.PortalKey, msg.ID, part.ID)
		dbMessage := &database.Message{
			ID:               msg.ID,
			PartID:           part.ID,
			MXID:             evtID,
			Room:             portal.PortalKey,
			SenderID:         msg.Sender.Sender,
			SenderMXID:       intent.GetMXID(),
			Timestamp:        msg.Timestamp,
			ThreadRoot:       ptr.Val(msg.ThreadRoot),
			ReplyTo:          ptr.Val(msg.ReplyTo),
			Metadata:         part.DBMetadata,
			IsDoublePuppeted: intent.IsDoublePuppet(),
		}
		if part.DontBridge {
			dbMessage.SetFakeMXID()
			out.DBMessages = append(out.DBMessages, dbMessage)
			continue
		}
		out.Events = append(out.Events, &event.Event{
			Sender:    intent.GetMXID(),
			Type:      part.Type,
			Timestamp: msg.Timestamp.UnixMilli(),
			ID:        evtID,
			RoomID:    portal.MXID,
			Content: event.Content{
				Parsed: part.Content,
				Raw:    part.Extra,
			},
		})
		partMap[part.ID] = dbMessage
		out.Extras = append(out.Extras, &MatrixSendExtra{MessageMeta: dbMessage, StreamOrder: msg.StreamOrder, PartIndex: i})
		out.DBMessages = append(out.DBMessages, dbMessage)
		if prevThreadEvent != nil {
			prevThreadEvent.MXID = evtID
			out.PrevThreadEvents[*msg.ThreadRoot] = evtID
		}
		if msg.Disappear.Type != event.DisappearingTypeNone {
			if msg.Disappear.Type == event.DisappearingTypeAfterSend && msg.Disappear.DisappearAt.IsZero() {
				msg.Disappear.DisappearAt = msg.Timestamp.Add(msg.Disappear.Timer)
			}
			out.Disappear = append(out.Disappear, &database.DisappearingMessage{
				RoomID:              portal.MXID,
				EventID:             evtID,
				Timestamp:           msg.Timestamp,
				DisappearingSetting: msg.Disappear,
			})
		}
	}
	slices.Sort(partIDs)
	if len(msg.Reactions) > 0 && len(partIDs) > 0 {
		out.PendingReactions = append(out.PendingReactions, &pendingBatchReactions{
			Message: msg,
			PartIDs: partIDs,
			PartMap: partMap,
		})
	}
}

// compileBatchReactions compiles the reactions of a message that was sent in a previous segment.
// The message parts in PartMap already have the event IDs returned by the homeserver.
func (portal *Portal) compileBatchReactions(ctx context.Context, source *UserLogin, pending *pendingBatchReactions, out *compileBatchOutput) {
	msg := pending.Message
	for _, reaction := range msg.Reactions {
		if reaction == nil {
			continue
		}
		reactionIntent, ok := portal.GetIntentFor(ctx, reaction.Sender, source, RemoteEventReactionRemove)
		if !ok {
			continue
		}
		if reaction.TargetPart == nil {
			reaction.TargetPart = &pending.PartIDs[0]
		}
		if reaction.Timestamp.IsZero() {
			reaction.Timestamp = msg.Timestamp.Add(10 * time.Millisecond)
		}
		targetPart, ok := pending.PartMap[*reaction.TargetPart]
		if !ok || targetPart.HasFakeMXID() {
			zerolog.Ctx(ctx).Debug().
				Str("message_id", string(msg.ID)).
				Str("target_part_id", string(*reaction.TargetPart)).
				Msg("Skipping backfill reaction to message part that wasn't bridged")
			continue
		}
		portal.ensureBatchSenderJoined(ctx, reactionIntent, out)
		reactionMXID := portal.Bridge.Matrix.GenerateReactionEventID(portal.MXID, targetPart, reaction.Sender.Sender, reaction.EmojiID)
		dbReaction := &database.Reaction{
			Room:          portal.PortalKey,
			MessageID:     msg.ID,
			MessagePartID: *reaction.TargetPart,
			SenderID:      reaction.Sender.Sender,
			EmojiID:       reaction.EmojiID,
			MXID:          reactionMXID,
			Timestamp:     reaction.Timestamp,
			Emoji:         reaction.Emoji,
			Metadata:      reaction.DBMetadata,
		}
		out.Events = append(out.Events, &event.Event{
			Sender:    reactionIntent.GetMXID(),
			Type:      event.EventReaction,
			Timestamp: reaction.Timestamp.UnixMilli(),
			ID:        reactionMXID,
			RoomID:    portal.MXID,
			Content: event.Content{
				Parsed: &event.ReactionEventContent{
					RelatesTo: event.RelatesTo{
						Type:    event.RelAnnotation,
						EventID: targetPart.MXID,
						Key:     variationselector.Add(reaction.Emoji),
					},
				},
				Raw: reaction.ExtraContent,
			},
		})
		out.DBReactions = append(out.DBReactions, dbReaction)
		out.Extras = append(out.Extras, &MatrixSendExtra{ReactionMeta: dbReaction})
	}
}

// batchSendTxnID returns a deterministic transaction ID for a batch send segment, so that a request that is
// retried (e.g. after a timeout) is deduplicated by the homeserver instead of creating the events twice.
func (portal *Portal) batchSendTxnID(out *compileBatchOutput) string {
	hash := sha256.New()
	hash.Write([]byte(portal.MXID))
	for _, extra := range out.Extras {
		if extra.MessageMeta != nil {
			_, _ = fmt.Fprintf(hash, "\x00m\x00%s\x00%s", extra.MessageMeta.ID, extra.MessageMeta.PartID)
		} else if extra.ReactionMeta != nil {
			_, _ = fmt.Fprintf(
				hash, "\x00r\x00%s\x00%s\x00%s\x00%s",
				extra.ReactionMeta.MessageID, extra.ReactionMeta.MessagePartID, extra.ReactionMeta.SenderID, extra.ReactionMeta.EmojiID,
			)
		}
	}
	return "mautrix-batch-" + base64.RawURLEncoding.EncodeToString(hash.Sum(nil)[:18])
}

// sendBatch sends backfill messages using the batch send API.
//
// The homeserver assigns the event IDs of batch sent events itself and returns them in the response,
// so the messages are split into segments: a message that replies to or continues a thread with
// a message in the current segment starts a new segment, and reactions are sent in the segment after
// the messages they target. Segments are also capped at maxBatchSendEvents events.
func (portal *Portal) sendBatch(ctx context.Context, source *UserLogin, messages []*BackfillMessage, forceForward, markRead, inThread bool) error {
	// Every segment is part of the same backfill, so they all get the same notification flag
	// (like the single batch did before segmenting). Only the last segment marks the room as read.
	sendNotification := !markRead && forceForward && !inThread
	joinedSenders := make(map[id.UserID]struct{})
	out := newCompileBatchOutput(min(len(messages), maxBatchSendEvents), joinedSenders)
	nextSegment := func() error {
		err := portal.sendBatchSegment(ctx, source, out, forceForward, false, sendNotification)
		if err != nil {
			return err
		}
		pendingReactions := out.PendingReactions
		out = newCompileBatchOutput(min(len(messages), maxBatchSendEvents), joinedSenders)
		for _, pending := range pendingReactions {
			portal.compileBatchReactions(ctx, source, pending, out)
		}
		// The new segment only contains reactions at this point (one event, extra and database row each),
		// and a segment's worth of messages can have more reactions than fit in a single request.
		for len(out.Events) > maxBatchSendEvents {
			reactionsOnly := newCompileBatchOutput(0, joinedSenders)
			reactionsOnly.Events, out.Events = out.Events[:maxBatchSendEvents], out.Events[maxBatchSendEvents:]
			reactionsOnly.Extras, out.Extras = out.Extras[:maxBatchSendEvents], out.Extras[maxBatchSendEvents:]
			reactionsOnly.DBReactions, out.DBReactions = out.DBReactions[:maxBatchSendEvents], out.DBReactions[maxBatchSendEvents:]
			err = portal.sendBatchSegment(ctx, source, reactionsOnly, forceForward, false, sendNotification)
			if err != nil {
				return err
			}
		}
		return nil
	}
	for _, msg := range messages {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if len(out.Events) > 0 && (len(out.Events)+len(msg.Parts) > maxBatchSendEvents || out.dependsOnUnsentEvent(msg)) {
			if err := nextSegment(); err != nil {
				return err
			}
		}
		portal.compileBatchMessage(ctx, source, msg, out, inThread)
	}
	if len(out.PendingReactions) > 0 {
		if err := nextSegment(); err != nil {
			return err
		}
	}
	return portal.sendBatchSegment(ctx, source, out, forceForward, markRead, sendNotification)
}

func (portal *Portal) sendBatchSegment(ctx context.Context, source *UserLogin, out *compileBatchOutput, forceForward, markRead, sendNotification bool) error {
	if len(out.Events) > 0 {
		req := &mautrix.ReqBeeperBatchSend{
			ForwardIfNoMessages: !forceForward,
			Forward:             forceForward,
			SendNotification:    sendNotification,
			Events:              out.Events,
			TxnID:               portal.batchSendTxnID(out),
		}
		if markRead {
			req.MarkReadBy = source.UserMXID
		}
		zerolog.Ctx(ctx).Debug().
			Int("event_count", len(out.Events)).
			Str("txn_id", req.TxnID).
			Msg("Sending batch of backfill events")
		resp, err := portal.Bridge.Matrix.BatchSend(ctx, portal.MXID, req, out.Extras)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("Failed to send backfill messages")
			return err
		}
		err = out.applyEventIDs(resp)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("Failed to apply event IDs of batch sent backfill messages")
			return err
		}
	}
	if len(out.Disappear) > 0 {
		disappear := out.Disappear
		// TODO mass insert disappearing messages
		go func() {
			for _, msg := range disappear {
				portal.Bridge.DisappearLoop.Add(ctx, msg)
			}
		}()
	}
	// TODO mass insert db messages
	for _, msg := range out.DBMessages {
		err := portal.Bridge.DB.Message.Insert(ctx, msg)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).
				Str("message_id", string(msg.ID)).
				Str("part_id", string(msg.PartID)).
				Str("sender_id", string(msg.SenderID)).
				Str("portal_id", string(msg.Room.ID)).
				Str("portal_receiver", string(msg.Room.Receiver)).
				Msg("Failed to insert backfilled message to database")
		}
	}
	// TODO mass insert db reactions
	for _, react := range out.DBReactions {
		err := portal.Bridge.DB.Reaction.Upsert(ctx, react)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).
				Str("message_id", string(react.MessageID)).
				Str("part_id", string(react.MessagePartID)).
				Str("sender_id", string(react.SenderID)).
				Str("portal_id", string(react.Room.ID)).
				Str("portal_receiver", string(react.Room.Receiver)).
				Msg("Failed to insert backfilled reaction to database")
		}
	}
	return nil
}

// isSkippableBackfillError returns true if a backfill message failed in a way that won't be fixed by retrying,
// like the homeserver rejecting the event (e.g. 403 for power levels or 413 for oversized events) or the message
// row failing to insert. Such messages are skipped so that one bad message doesn't stop the rest of the chat's
// history from being backfilled. Other errors (network errors, server errors, rate limits) still abort the batch,
// so that the backfill isn't marked as complete while the server is unavailable. 401 (bad appservice token)
// and 408 (request timeout) are 4xx errors that affect every message, so they also abort the batch.
func isSkippableBackfillError(err error) bool {
	switch typedErr := err.(type) {
	case nil:
		return false
	case mautrix.HTTPError:
		if typedErr.Response == nil {
			return false
		}
		switch statusCode := typedErr.Response.StatusCode; statusCode {
		case http.StatusUnauthorized, http.StatusRequestTimeout, http.StatusTooManyRequests:
			return false
		default:
			return statusCode >= 400 && statusCode < 500
		}
	case interface{ Unwrap() []error }:
		errs := typedErr.Unwrap()
		if len(errs) == 0 {
			return false
		}
		if _, isStatus := errs[0].(MessageStatus); isStatus && errors.Is(errs[0], ErrDatabaseError) {
			// fmt.Errorf("%w: ...: %w", ErrDatabaseError, err): failing to save a message part that was already sent to Matrix.
			// This only matches the database error wrapper itself, not an errors.Join that also contains other errors.
			return true
		}
		// Multiple failed parts (errors.Join): only skip if all of them are skippable
		for _, wrappedErr := range errs {
			if !isSkippableBackfillError(wrappedErr) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return isSkippableBackfillError(typedErr.Unwrap())
	default:
		return false
	}
}

func (portal *Portal) sendLegacyBackfill(ctx context.Context, source *UserLogin, messages []*BackfillMessage, markRead bool) error {
	var lastPart id.EventID
	var skippedMessages int
	var lastSkipErr error
	for _, msg := range messages {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		intent, ok := portal.GetIntentFor(ctx, msg.Sender, source, RemoteEventMessage)
		if !ok {
			continue
		}
		dbMessages, res := portal.sendConvertedMessage(ctx, msg.ID, intent, msg.Sender.Sender, msg.ConvertedMessage, msg.Timestamp, msg.StreamOrder, func(z *zerolog.Event) *zerolog.Event {
			return z.
				Str("message_id", string(msg.ID)).
				Any("sender_id", msg.Sender).
				Time("message_ts", msg.Timestamp)
		})
		if !res.Success {
			if ctx.Err() != nil {
				return ctx.Err()
			} else if !isSkippableBackfillError(res.Error) {
				return res.Error
			}
			skippedMessages++
			lastSkipErr = res.Error
			zerolog.Ctx(ctx).Warn().Err(res.Error).
				Str("message_id", string(msg.ID)).
				Any("sender_id", msg.Sender).
				Time("message_ts", msg.Timestamp).
				Int("sent_parts", len(dbMessages)).
				Msg("Failed to send backfill message, skipping it and continuing with the rest of the batch")
		}
		if len(dbMessages) > 0 {
			lastPart = dbMessages[len(dbMessages)-1].MXID
			for _, reaction := range msg.Reactions {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				reactionIntent, ok := portal.GetIntentFor(ctx, reaction.Sender, source, RemoteEventReaction)
				if !ok {
					continue
				}
				targetPart := dbMessages[0]
				if reaction.TargetPart != nil {
					targetPartIdx := slices.IndexFunc(dbMessages, func(dbMsg *database.Message) bool {
						return dbMsg.PartID == *reaction.TargetPart
					})
					if targetPartIdx != -1 {
						targetPart = dbMessages[targetPartIdx]
					} else {
						// TODO warning log and/or skip reaction?
					}
				}
				portal.sendConvertedReaction(
					ctx, reaction.Sender.Sender, reactionIntent, targetPart, reaction.EmojiID, reaction.Emoji,
					reaction.Timestamp, reaction.DBMetadata, reaction.ExtraContent,
					func(z *zerolog.Event) *zerolog.Event {
						return z.
							Str("target_message_id", string(msg.ID)).
							Str("target_part_id", string(targetPart.PartID)).
							Any("reaction_sender_id", reaction.Sender).
							Time("reaction_ts", reaction.Timestamp)
					},
				)
			}
		}
	}
	if skippedMessages > 0 && lastPart == "" {
		// Nothing reached Matrix, so the errors are most likely not specific to the skipped messages
		// (e.g. the bridge lost permissions in the room). Fail the batch like before instead of completing it,
		// so that the network connector doesn't throw away the history.
		return fmt.Errorf("all %d backfill messages that were tried failed to send: %w", skippedMessages, lastSkipErr)
	} else if skippedMessages > 0 {
		zerolog.Ctx(ctx).Warn().
			Int("skipped_count", skippedMessages).
			Int("total_count", len(messages)).
			Msg("Some backfill messages were skipped because they failed to send")
	}
	if markRead && lastPart != "" {
		dp := source.User.DoublePuppet(ctx)
		if dp != nil {
			err := dp.MarkRead(ctx, portal.MXID, lastPart, time.Now())
			if err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("Failed to mark room as read after backfill")
			}
		}
	}
	return nil
}
