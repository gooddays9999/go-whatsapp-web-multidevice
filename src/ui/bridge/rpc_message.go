package bridge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	domainMessage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/message"
	domainSend "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/send"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/utils"
	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	"github.com/disintegration/imaging"
	"github.com/sirupsen/logrus"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

const (
	statusMediaMaxBytes        = 50 * 1024 * 1024
	statusThumbnailMaxEdge     = 320
	statusThumbnailJPEGQuality = 75
)

func (s *Service) accountContext(ctx context.Context, accountID string) (context.Context, error) {
	if accountID == "" {
		return nil, fmt.Errorf("account_id is required")
	}
	env, _, err := s.environmentForAccount(ctx, accountID, "", nil, false)
	if err != nil {
		return nil, err
	}
	if env == nil {
		return nil, fmt.Errorf("account not connected")
	}
	proxyURL, err := env.ProxyURL()
	if err != nil {
		return nil, err
	}
	inst, err := s.deps.DeviceManager.EnsureClientWithEnvironment(ctx, accountID, whatsapp.ClientEnvironment{
		ProxyAddress:    proxyURL,
		ProxyConfigured: true,
		UserAgent:       env.UserAgent,
		BrowserFamily:   env.BrowserFamily,
		OSName:          env.OSName,
	})
	if err != nil {
		return nil, err
	}
	client := inst.GetClient()
	if client == nil || client.Store == nil || client.Store.ID == nil {
		return nil, fmt.Errorf("account not connected")
	}
	state := inst.UpdateStateFromClient()
	if !cachedLoggedIn(state) && cachedConnected(state) {
		client.Disconnect()
		state = inst.MarkDisconnected()
	}
	if !cachedConnected(state) {
		if err := s.connectWithSlot(ctx, inst, accountID, "bridge account context connect", s.connectTimeout()); err != nil {
			return nil, err
		}
		state = inst.UpdateStateFromClient()
	}
	if !cachedLoggedIn(state) {
		return nil, fmt.Errorf("account not logged in")
	}
	return whatsapp.ContextWithDevice(ctx, inst), nil
}

func (s *Service) SendMessage(ctx context.Context, req *bridgepb.SendMessageRequest) (*bridgepb.SendMessageResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		s.publish("message.failed", req.GetAccountId(), map[string]any{"to": req.GetTo(), "error": err.Error()})
		return nil, grpcError(err)
	}
	inst, _ := whatsapp.DeviceFromContext(scoped)
	timeout := statusTimeout(s.cfg.MessageSendTimeout, 25*time.Second)
	start := logAccountOperationStart(req.GetAccountId(), "SendMessage", timeout, logrus.Fields{"to": req.GetTo()})
	sendCtx, cancel := statusDeviceContext(ctx, inst, timeout)
	resp, err := s.deps.SendUsecase.SendText(sendCtx, domainSend.MessageRequest{
		BaseRequest:    domainSend.BaseRequest{Phone: req.GetTo()},
		Message:        req.GetContent().GetText(),
		ReplyMessageID: optionalString(req.GetQuotedMsgId()),
	})
	cancel()
	if err != nil {
		stageErr := accountOperationError("SendMessage", timeout, err)
		logAccountOperationFailure(req.GetAccountId(), "SendMessage", start, stageErr, logrus.Fields{"to": req.GetTo()})
		s.handleAccountOperationFailure(req.GetAccountId(), inst, "SendMessage", err)
		s.publish("message.failed", req.GetAccountId(), map[string]any{"to": req.GetTo(), "error": stageErr.Error()})
		return &bridgepb.SendMessageResponse{Success: false, Status: "failed", Error: stageErr.Error()}, nil
	}
	s.markRecentIncomingAsRead(scoped, req.GetTo())
	s.publish("message.sent", req.GetAccountId(), map[string]any{"messageId": resp.MessageID, "to": req.GetTo()})
	logAccountOperationSuccess(req.GetAccountId(), "SendMessage", start, logrus.Fields{"to": req.GetTo(), "message_id": resp.MessageID})
	return &bridgepb.SendMessageResponse{Success: true, MessageId: resp.MessageID, Status: "sent"}, nil
}

func (s *Service) SendMedia(ctx context.Context, req *bridgepb.SendMediaRequest) (*bridgepb.SendMediaResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		s.publish("message.failed", req.GetAccountId(), map[string]any{"to": req.GetTo(), "error": err.Error()})
		return nil, grpcError(err)
	}
	mediaURL := req.GetMediaUrl()
	var msgID string
	switch strings.ToLower(req.GetType()) {
	case "image":
		resp, err := s.deps.SendUsecase.SendImage(scoped, domainSend.ImageRequest{BaseRequest: domainSend.BaseRequest{Phone: req.GetTo()}, ImageURL: &mediaURL, Caption: req.GetCaption()})
		if err != nil {
			return mediaFailed(s, req, err), nil
		}
		msgID = resp.MessageID
	case "video":
		resp, err := s.deps.SendUsecase.SendVideo(scoped, domainSend.VideoRequest{BaseRequest: domainSend.BaseRequest{Phone: req.GetTo()}, VideoURL: &mediaURL, Caption: req.GetCaption()})
		if err != nil {
			return mediaFailed(s, req, err), nil
		}
		msgID = resp.MessageID
	case "audio":
		resp, err := s.deps.SendUsecase.SendAudio(scoped, domainSend.AudioRequest{BaseRequest: domainSend.BaseRequest{Phone: req.GetTo()}, AudioURL: &mediaURL, PTT: req.GetSendAudioAsVoice()})
		if err != nil {
			return mediaFailed(s, req, err), nil
		}
		msgID = resp.MessageID
	default:
		resp, err := s.deps.SendUsecase.SendFile(scoped, domainSend.FileRequest{BaseRequest: domainSend.BaseRequest{Phone: req.GetTo()}, FileURL: &mediaURL, Caption: req.GetCaption()})
		if err != nil {
			return mediaFailed(s, req, err), nil
		}
		msgID = resp.MessageID
	}
	s.markRecentIncomingAsRead(scoped, req.GetTo())
	s.publish("message.sent", req.GetAccountId(), map[string]any{"messageId": msgID, "to": req.GetTo()})
	return &bridgepb.SendMediaResponse{Success: true, MessageId: msgID, Status: "sent"}, nil
}

func mediaFailed(s *Service, req *bridgepb.SendMediaRequest, err error) *bridgepb.SendMediaResponse {
	s.publish("message.failed", req.GetAccountId(), map[string]any{"to": req.GetTo(), "error": err.Error()})
	return &bridgepb.SendMediaResponse{Success: false, Status: "failed", Error: err.Error()}
}

func (s *Service) SendContact(ctx context.Context, req *bridgepb.SendContactRequest) (*bridgepb.SendContactResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	name, phone := parseContactData(req.GetContactData())
	resp, err := s.deps.SendUsecase.SendContact(scoped, domainSend.ContactRequest{
		BaseRequest:  domainSend.BaseRequest{Phone: req.GetTo()},
		ContactName:  name,
		ContactPhone: phone,
	})
	if err != nil {
		return &bridgepb.SendContactResponse{Success: false, Status: "failed", Error: err.Error()}, nil
	}
	s.markRecentIncomingAsRead(scoped, req.GetTo())
	return &bridgepb.SendContactResponse{Success: true, MessageId: resp.MessageID, Status: "sent"}, nil
}

func (s *Service) markRecentIncomingAsRead(ctx context.Context, to string) {
	inst, ok := whatsapp.DeviceFromContext(ctx)
	if !ok || inst == nil {
		return
	}
	client := inst.GetClient()
	if client == nil || client.Store == nil || client.Store.ID == nil {
		return
	}
	chat, err := utils.ParseJID(to)
	if err != nil {
		return
	}
	chat = whatsapp.NormalizeJIDFromLID(ctx, chat, client).ToNonAD()
	if chat.IsEmpty() || chat.Server == types.GroupServer {
		return
	}

	repo := inst.GetChatStorage()
	if repo == nil {
		repo = s.deps.ChatStorageRepo
	}
	if repo == nil {
		return
	}
	deviceID := inst.JID()
	if deviceID == "" {
		deviceID = inst.ID()
	}
	isFromMe := false
	messages, err := repo.GetMessages(&domainChatStorage.MessageFilter{
		DeviceID: deviceID,
		ChatJID:  chat.String(),
		Limit:    20,
		IsFromMe: &isFromMe,
	})
	if err != nil || len(messages) == 0 {
		return
	}

	ids := make([]types.MessageID, 0, len(messages))
	for _, msg := range messages {
		if msg != nil && msg.ID != "" {
			ids = append(ids, msg.ID)
		}
	}
	if len(ids) == 0 {
		return
	}
	_ = client.MarkRead(ctx, ids, time.Now(), chat, chat)
}

func (s *Service) GetMessageStatus(ctx context.Context, req *bridgepb.MessageStatusRequest) (*bridgepb.MessageStatusResponse, error) {
	if req.GetAccountId() == "" || req.GetMessageId() == "" {
		return nil, grpcError(fmt.Errorf("account_id and message_id are required"))
	}
	if s.deps.ChatStorageRepo != nil {
		if msg, _ := s.deps.ChatStorageRepo.GetMessageByID(req.GetMessageId()); msg != nil {
			return &bridgepb.MessageStatusResponse{MessageId: req.GetMessageId(), Status: "sent", Timestamp: msg.Timestamp.UnixMilli()}, nil
		}
	}
	return &bridgepb.MessageStatusResponse{MessageId: req.GetMessageId(), Status: "unknown", Timestamp: 0}, nil
}

func (s *Service) ReactToMessage(ctx context.Context, req *bridgepb.ReactToMessageRequest) (*bridgepb.ReactToMessageResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	inst, _ := whatsapp.DeviceFromContext(scoped)
	client := inst.GetClient()
	msg, err := s.resolveReactionTargetMessage(scoped, client, req.GetMessageId())
	if err != nil {
		return nil, grpcError(err)
	}
	timeout := statusTimeout(s.cfg.MessageReactionTimeout, 15*time.Second)
	start := logAccountOperationStart(req.GetAccountId(), "ReactToMessage", timeout, logrus.Fields{"message_id": msg.ID})
	reactCtx, cancel := statusDeviceContext(ctx, inst, timeout)
	resp, err := s.deps.MessageUsecase.ReactMessage(reactCtx, domainMessage.ReactionRequest{
		MessageID: msg.ID,
		Phone:     msg.ChatJID,
		Emoji:     req.GetEmoji(),
	})
	cancel()
	if err != nil {
		stageErr := accountOperationError("ReactToMessage", timeout, err)
		logAccountOperationFailure(req.GetAccountId(), "ReactToMessage", start, stageErr, logrus.Fields{"message_id": msg.ID})
		s.handleAccountOperationFailure(req.GetAccountId(), inst, "ReactToMessage", err)
		return &bridgepb.ReactToMessageResponse{Success: false, MessageId: msg.ID, Emoji: req.GetEmoji(), Error: stageErr.Error()}, nil
	}
	action := "add"
	if req.GetEmoji() == "" {
		action = "remove"
	}
	logAccountOperationSuccess(req.GetAccountId(), "ReactToMessage", start, logrus.Fields{"message_id": msg.ID, "action": action})
	return &bridgepb.ReactToMessageResponse{Success: true, MessageId: resp.MessageID, Emoji: req.GetEmoji(), Action: action}, nil
}

func (s *Service) resolveReactionTargetMessage(ctx context.Context, client *whatsmeow.Client, rawMessageID string) (*domainChatStorage.Message, error) {
	if s.deps.ChatStorageRepo == nil {
		return nil, fmt.Errorf("chat storage is not available")
	}
	rawMessageID = strings.TrimSpace(rawMessageID)
	if rawMessageID == "" {
		return nil, fmt.Errorf("message_id is required")
	}

	msg, err := s.deps.ChatStorageRepo.GetMessageByID(rawMessageID)
	if err != nil {
		return nil, fmt.Errorf("failed to find message: %w", err)
	}
	if msg != nil {
		return msg, nil
	}
	if !looksLikeLegacyReactionRecipient(rawMessageID) {
		return nil, fmt.Errorf("message not found: %s", rawMessageID)
	}

	chatJID, err := parseLegacyReactionRecipientJID(rawMessageID)
	if err != nil {
		return nil, fmt.Errorf("message not found: %s", rawMessageID)
	}
	if client != nil {
		chatJID = whatsapp.NormalizeJIDFromLID(ctx, chatJID, client)
	}
	chatJID = chatJID.ToNonAD()

	deviceID := currentDeviceStorageID(ctx, client)
	if deviceID == "" {
		return nil, fmt.Errorf("unable to resolve current account device")
	}
	msg, err = findLatestIncomingReactionTarget(s.deps.ChatStorageRepo, deviceID, chatJID.String(), rawMessageID)
	if err != nil {
		return nil, err
	}
	logrus.WithFields(logrus.Fields{
		"legacy_message_id": rawMessageID,
		"resolved_message":  msg.ID,
		"chat_jid":          msg.ChatJID,
	}).Info("resolved legacy reaction recipient to latest incoming message")
	return msg, nil
}

func looksLikeLegacyReactionRecipient(raw string) bool {
	value := strings.TrimSpace(raw)
	if value == "" || strings.Contains(value, "_") {
		return false
	}
	lower := strings.ToLower(value)
	for _, suffix := range []string{"@c.us", "@s.whatsapp.net", "@lid", "@g.us"} {
		if strings.Contains(lower, suffix) {
			return true
		}
	}

	digits := 0
	for i, r := range value {
		switch {
		case r >= '0' && r <= '9':
			digits++
		case r == '+' && i == 0:
		case r == ' ' || r == '-' || r == '(' || r == ')' || r == '.':
		default:
			return false
		}
	}
	return digits >= 5
}

func parseLegacyReactionRecipientJID(raw string) (types.JID, error) {
	value := strings.TrimSpace(raw)
	if !strings.Contains(value, "@") {
		value = legacyContactNumber(value)
	}
	return parseStatusUserJID(value)
}

type reactionTargetMessageStore interface {
	GetMessageByID(id string) (*domainChatStorage.Message, error)
	GetMessages(filter *domainChatStorage.MessageFilter) ([]*domainChatStorage.Message, error)
}

func findLatestIncomingReactionTarget(repo reactionTargetMessageStore, deviceID, chatJID, legacyInput string) (*domainChatStorage.Message, error) {
	isFromMe := false
	messages, err := repo.GetMessages(&domainChatStorage.MessageFilter{
		DeviceID: deviceID,
		ChatJID:  chatJID,
		Limit:    50,
		IsFromMe: &isFromMe,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find latest incoming message for %s: %w", legacyInput, err)
	}
	for _, msg := range messages {
		if msg != nil && msg.ID != "" && !msg.IsFromMe {
			return msg, nil
		}
	}
	return nil, fmt.Errorf("message not found: %s", legacyInput)
}

func (s *Service) GetMessageReactions(ctx context.Context, req *bridgepb.GetMessageReactionsRequest) (*bridgepb.GetMessageReactionsResponse, error) {
	if req.GetAccountId() == "" || req.GetMessageId() == "" {
		return nil, grpcError(fmt.Errorf("account_id and message_id are required"))
	}
	return &bridgepb.GetMessageReactionsResponse{Success: true, MessageId: req.GetMessageId(), HasReaction: false, Reactions: []*bridgepb.ReactionGroup{}}, nil
}

func (s *Service) SendStatus(ctx context.Context, req *bridgepb.SendStatusRequest) (*bridgepb.SendStatusResponse, error) {
	accountID := req.GetAccountId()
	accountTimeout := statusTimeout(s.cfg.StatusAccountContextTimeout, 12*time.Second)
	accountStart := logStatusStageStart(accountID, "accountContext", accountTimeout)
	accountCtx, accountCancel := context.WithTimeout(ctx, accountTimeout)
	scoped, err := s.accountContext(accountCtx, accountID)
	var inst *whatsapp.DeviceInstance
	if err == nil {
		inst, _ = whatsapp.DeviceFromContext(scoped)
	}
	accountCancel()
	if err != nil {
		stageErr := statusStageError("accountContext", accountTimeout, err)
		logStatusStageFailure(accountID, "accountContext", accountStart, stageErr, nil)
		s.handleStatusStageFailure(accountID, nil, "accountContext", err)
		return &bridgepb.SendStatusResponse{Success: false, Error: stageErr.Error()}, nil
	}
	logStatusStageSuccess(accountID, "accountContext", accountStart, nil)

	if inst == nil {
		err := fmt.Errorf("status accountContext failed: account device instance missing")
		logStatusStageFailure(accountID, "accountContext", accountStart, err, nil)
		return &bridgepb.SendStatusResponse{Success: false, Error: err.Error()}, nil
	}
	client := inst.GetClient()

	queueTimeout := statusTimeout(s.cfg.StatusSendQueueTimeout, 5*time.Second)
	queueStart := logStatusStageStart(accountID, "queue", queueTimeout)
	queueCtx, queueCancel := statusDeviceContext(ctx, inst, queueTimeout)
	release, err := s.acquireStatusSendSlot(queueCtx)
	queueCancel()
	if err != nil {
		stageErr := statusStageError("queue", queueTimeout, err)
		logStatusStageFailure(accountID, "queue", queueStart, stageErr, nil)
		s.handleStatusStageFailure(accountID, inst, "queue", err)
		return &bridgepb.SendStatusResponse{Success: false, Error: stageErr.Error()}, nil
	}
	defer release()
	logStatusStageSuccess(accountID, "queue", queueStart, nil)

	recipientTimeout := statusTimeout(s.cfg.StatusRecipientTimeout, 8*time.Second)
	recipientStart := logStatusStageStart(accountID, "ensureStatusRecipients", recipientTimeout)
	recipientCtx, recipientCancel := statusDeviceContext(ctx, inst, recipientTimeout)
	recipientCount, err := ensureStatusRecipients(recipientCtx, client)
	recipientCancel()
	if err != nil {
		stageErr := statusStageError("ensureStatusRecipients", recipientTimeout, err)
		logStatusStageFailure(accountID, "ensureStatusRecipients", recipientStart, stageErr, nil)
		s.handleStatusStageFailure(accountID, inst, "ensureStatusRecipients", err)
		return &bridgepb.SendStatusResponse{Success: false, Error: stageErr.Error()}, nil
	}
	logStatusStageSuccess(accountID, "ensureStatusRecipients", recipientStart, logrus.Fields{"recipients": recipientCount})

	buildTimeout := statusTimeout(s.cfg.StatusBuildTimeout, 30*time.Second)
	buildStart := logStatusStageStart(accountID, "buildMessage", buildTimeout)
	buildCtx, buildCancel := statusDeviceContext(ctx, inst, buildTimeout)
	msg, hasMedia, err := buildStatusMessage(buildCtx, client, req)
	buildCancel()
	if err != nil {
		stageErr := statusStageError("buildMessage", buildTimeout, err)
		logStatusStageFailure(accountID, "buildMessage", buildStart, stageErr, nil)
		s.handleStatusStageFailure(accountID, inst, "buildMessage", err)
		return &bridgepb.SendStatusResponse{Success: false, Error: stageErr.Error()}, nil
	}
	logStatusStageSuccess(accountID, "buildMessage", buildStart, logrus.Fields{
		"kind":      statusMessageKind(msg),
		"has_media": hasMedia,
	})

	logrus.WithFields(logrus.Fields{
		"account_id": accountID,
		"kind":       statusMessageKind(msg),
		"has_media":  hasMedia,
		"recipients": recipientCount,
	}).Info("sending WhatsApp status")
	sendTimeout := statusTimeout(s.cfg.StatusMessageTimeout, 25*time.Second)
	sendStart := logStatusStageStart(accountID, "SendMessage", sendTimeout)
	sendCtx, sendCancel := statusDeviceContext(ctx, inst, sendTimeout)
	ts, err := client.SendMessage(sendCtx, types.StatusBroadcastJID, msg)
	sendCancel()
	if err != nil {
		stageErr := statusStageError("SendMessage", sendTimeout, err)
		logStatusStageFailure(accountID, "SendMessage", sendStart, stageErr, logrus.Fields{
			"kind":       statusMessageKind(msg),
			"recipients": recipientCount,
		})
		s.handleStatusStageFailure(accountID, inst, "SendMessage", err)
		return &bridgepb.SendStatusResponse{Success: false, Error: stageErr.Error()}, nil
	}
	logStatusStageSuccess(accountID, "SendMessage", sendStart, logrus.Fields{
		"kind":       statusMessageKind(msg),
		"message_id": string(ts.ID),
		"recipients": recipientCount,
	})
	logrus.WithFields(logrus.Fields{
		"account_id": accountID,
		"kind":       statusMessageKind(msg),
		"message_id": string(ts.ID),
		"server_ts":  ts.Timestamp,
		"recipients": recipientCount,
	}).Info("WhatsApp status send acknowledged")
	return &bridgepb.SendStatusResponse{Success: true, MessageId: ts.ID, HasMedia: hasMedia}, nil
}

func statusTimeout(value, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return fallback
}

func statusDeviceContext(parent context.Context, inst *whatsapp.DeviceInstance, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	if inst != nil {
		ctx = whatsapp.ContextWithDevice(ctx, inst)
	}
	return ctx, cancel
}

func statusStageError(stage string, timeout time.Duration, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("status %s timed out after %s: %w", stage, timeout, err)
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("status %s cancelled after %s: %w", stage, timeout, err)
	default:
		return fmt.Errorf("status %s failed: %w", stage, err)
	}
}

func accountOperationError(operation string, timeout time.Duration, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%s timed out after %s: %w", operation, timeout, err)
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("%s cancelled after %s: %w", operation, timeout, err)
	default:
		return fmt.Errorf("%s failed: %w", operation, err)
	}
}

func logAccountOperationStart(accountID, operation string, timeout time.Duration, fields logrus.Fields) time.Time {
	start := time.Now()
	logFields := logrus.Fields{
		"account_id": accountID,
		"operation":  operation,
		"timeout_ms": timeout.Milliseconds(),
	}
	for key, value := range fields {
		logFields[key] = value
	}
	logrus.WithFields(logFields).Info("WhatsApp account operation started")
	return start
}

func logAccountOperationSuccess(accountID, operation string, start time.Time, fields logrus.Fields) {
	logFields := logrus.Fields{
		"account_id":  accountID,
		"operation":   operation,
		"duration_ms": time.Since(start).Milliseconds(),
	}
	for key, value := range fields {
		logFields[key] = value
	}
	logrus.WithFields(logFields).Info("WhatsApp account operation completed")
}

func logAccountOperationFailure(accountID, operation string, start time.Time, err error, fields logrus.Fields) {
	logFields := logrus.Fields{
		"account_id":  accountID,
		"operation":   operation,
		"duration_ms": time.Since(start).Milliseconds(),
	}
	for key, value := range fields {
		logFields[key] = value
	}
	logrus.WithError(err).WithFields(logFields).Warn("WhatsApp account operation failed")
}

func logStatusStageStart(accountID, stage string, timeout time.Duration) time.Time {
	start := time.Now()
	logrus.WithFields(logrus.Fields{
		"account_id": accountID,
		"stage":      stage,
		"timeout_ms": timeout.Milliseconds(),
	}).Info("WhatsApp status stage started")
	return start
}

func logStatusStageSuccess(accountID, stage string, start time.Time, fields logrus.Fields) {
	logFields := logrus.Fields{
		"account_id":  accountID,
		"stage":       stage,
		"duration_ms": time.Since(start).Milliseconds(),
	}
	for key, value := range fields {
		logFields[key] = value
	}
	logrus.WithFields(logFields).Info("WhatsApp status stage completed")
}

func logStatusStageFailure(accountID, stage string, start time.Time, err error, fields logrus.Fields) {
	logFields := logrus.Fields{
		"account_id":  accountID,
		"stage":       stage,
		"duration_ms": time.Since(start).Milliseconds(),
	}
	for key, value := range fields {
		logFields[key] = value
	}
	logrus.WithError(err).WithFields(logFields).Warn("WhatsApp status stage failed")
}

func (s *Service) acquireStatusSendSlot(ctx context.Context) (func(), error) {
	if s == nil || s.statusSendSlots == nil {
		return func() {}, nil
	}
	select {
	case s.statusSendSlots <- struct{}{}:
	case <-ctx.Done():
		return nil, fmt.Errorf("status send queued cancelled: %w", ctx.Err())
	}

	released := false
	release := func() {
		if released {
			return
		}
		released = true
		<-s.statusSendSlots
	}

	if s.cfg.StatusSendMinInterval > 0 {
		s.statusSendMu.Lock()
		wait := s.lastStatusSend.Add(s.cfg.StatusSendMinInterval).Sub(time.Now())
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				s.statusSendMu.Unlock()
				release()
				return nil, fmt.Errorf("status send delayed cancelled: %w", ctx.Err())
			}
		}
		s.lastStatusSend = time.Now()
		s.statusSendMu.Unlock()
	}

	return release, nil
}

func (s *Service) handleStatusStageFailure(accountID string, inst *whatsapp.DeviceInstance, stage string, err error) {
	switch {
	case shouldRecycleStatusClient(stage, err):
		if inst != nil {
			state := inst.MarkDisconnected()
			logrus.WithError(err).WithFields(logrus.Fields{
				"account_id": accountID,
				"stage":      stage,
				"state":      state,
			}).Warn("WhatsApp status stage timed out, recycling account client")
		} else {
			logrus.WithError(err).WithFields(logrus.Fields{
				"account_id": accountID,
				"stage":      stage,
			}).Warn("WhatsApp status stage timed out, recycling account client")
		}
		s.markDisconnected(accountID)
		s.scheduleClientRecycle(accountID, "status "+stage+" timeout")
	case isDisconnectedSendError(err):
		s.handleStatusSendFailure(accountID, inst, err)
	}
}

func (s *Service) handleAccountOperationFailure(accountID string, inst *whatsapp.DeviceInstance, operation string, err error) {
	switch {
	case shouldRecycleAccountClient(err):
		if inst != nil {
			state := inst.MarkDisconnected()
			logrus.WithError(err).WithFields(logrus.Fields{
				"account_id": accountID,
				"operation":  operation,
				"state":      state,
			}).Warn("WhatsApp account operation timed out, recycling account client")
		} else {
			logrus.WithError(err).WithFields(logrus.Fields{
				"account_id": accountID,
				"operation":  operation,
			}).Warn("WhatsApp account operation timed out, recycling account client")
		}
		s.markDisconnected(accountID)
		s.scheduleClientRecycle(accountID, operation+" timeout")
	case isDisconnectedSendError(err):
		s.handleStatusSendFailure(accountID, inst, err)
	}
}

func shouldRecycleStatusClient(stage string, err error) bool {
	if err == nil {
		return false
	}
	switch stage {
	case "accountContext", "ensureStatusRecipients", "SendMessage":
	default:
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func shouldRecycleAccountClient(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func (s *Service) handleStatusSendFailure(accountID string, inst *whatsapp.DeviceInstance, err error) {
	if !isDisconnectedSendError(err) {
		return
	}
	state := ""
	if inst != nil {
		state = string(inst.UpdateStateFromClient())
	}
	logrus.WithError(err).WithFields(logrus.Fields{
		"account_id": accountID,
		"state":      state,
	}).Warn("WhatsApp websocket disconnected during status send, scheduling reconnect")
	if inst == nil || !cachedConnected(inst.State()) {
		s.markDisconnected(accountID)
	}
	s.scheduleReconnect(accountID, "status send websocket disconnected")
}

func isDisconnectedSendError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, pattern := range []string{
		"websocket disconnected",
		"not connected",
		"connection closed",
		"use of closed network connection",
		"broken pipe",
	} {
		if strings.Contains(msg, pattern) {
			return true
		}
	}
	return false
}

func (s *Service) scheduleReconnect(accountID, reason string) {
	s.scheduleReconnectWithMode(accountID, reason, false)
}

func (s *Service) scheduleClientRecycle(accountID, reason string) {
	s.scheduleReconnectWithMode(accountID, reason, true)
}

func (s *Service) scheduleReconnectWithMode(accountID, reason string, recycle bool) {
	if s == nil || accountID == "" {
		return
	}
	s.mu.Lock()
	if _, ok := s.reconnecting[accountID]; ok {
		s.mu.Unlock()
		return
	}
	s.reconnecting[accountID] = time.Now()
	s.mu.Unlock()

	go func() {
		defer func() {
			s.mu.Lock()
			delete(s.reconnecting, accountID)
			s.mu.Unlock()
		}()

		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		<-timer.C

		timeout := s.connectTimeout()
		ctx, cancel := context.WithTimeout(context.Background(), timeout+5*time.Second)
		defer cancel()
		var err error
		if recycle {
			err = s.recycleAccountClient(ctx, accountID)
		} else {
			_, err = s.accountContext(ctx, accountID)
		}
		if err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"account_id": accountID,
				"reason":     reason,
				"recycle":    recycle,
			}).Warn("scheduled WhatsApp reconnect failed")
			return
		}
		logrus.WithFields(logrus.Fields{
			"account_id": accountID,
			"reason":     reason,
			"recycle":    recycle,
		}).Info("scheduled WhatsApp reconnect completed")
	}()
}

func (s *Service) recycleAccountClient(ctx context.Context, accountID string) error {
	if s == nil || s.envStore == nil || s.deps.DeviceManager == nil {
		return fmt.Errorf("bridge service is not ready")
	}
	env, _, err := s.environmentForAccount(ctx, accountID, "", nil, false)
	if err != nil {
		return err
	}
	if env == nil {
		return fmt.Errorf("account not connected")
	}
	proxyURL, err := env.ProxyURL()
	if err != nil {
		return err
	}
	inst, err := s.deps.DeviceManager.RecreateClientWithEnvironment(ctx, accountID, whatsapp.ClientEnvironment{
		ProxyAddress:    proxyURL,
		ProxyConfigured: true,
		UserAgent:       env.UserAgent,
		BrowserFamily:   env.BrowserFamily,
		OSName:          env.OSName,
	})
	if err != nil {
		return err
	}
	if inst == nil || inst.GetClient() == nil {
		return fmt.Errorf("account client is nil after recycle")
	}
	if err := s.connectWithSlot(ctx, inst, accountID, "bridge account client recycle", s.connectTimeout()); err != nil {
		return err
	}
	if !cachedLoggedIn(inst.UpdateStateFromClient()) {
		return fmt.Errorf("account not logged in after client recycle")
	}
	s.markConnected(accountID)
	s.publish("account.connected", accountID, map[string]any{
		"phoneNumber": inst.PhoneNumber(),
		"workerId":    s.workerID,
		"connectedAt": time.Now().UnixMilli(),
		"verified":    true,
	})
	return nil
}

func ensureStatusRecipients(ctx context.Context, client *whatsmeow.Client) (int, error) {
	recipients, err := client.DangerousInternals().GetStatusBroadcastRecipients(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to resolve status recipients: %w", err)
	}
	if len(recipients) > 0 {
		return len(recipients), nil
	}

	if err := client.FetchAppState(ctx, appstate.WAPatchCriticalUnblockLow, false, true); err != nil {
		return 0, fmt.Errorf("failed to sync WhatsApp contacts for status recipients: %w", err)
	}
	recipients, err = client.DangerousInternals().GetStatusBroadcastRecipients(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to resolve status recipients after contact sync: %w", err)
	}
	if len(recipients) > 0 {
		return len(recipients), nil
	}

	if err := client.FetchAppState(ctx, appstate.WAPatchCriticalUnblockLow, true, false); err != nil {
		return 0, fmt.Errorf("failed to full-sync WhatsApp contacts for status recipients: %w", err)
	}
	recipients, err = client.DangerousInternals().GetStatusBroadcastRecipients(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to resolve status recipients after full contact sync: %w", err)
	}
	if len(recipients) == 0 {
		return 0, fmt.Errorf("no WhatsApp status recipients found; sync contacts or adjust WhatsApp status privacy before publishing")
	}
	return len(recipients), nil
}

func buildStatusMessage(ctx context.Context, client *whatsmeow.Client, req *bridgepb.SendStatusRequest) (*waE2E.Message, bool, error) {
	mediaURL := strings.TrimSpace(req.GetMediaUrl())
	if mediaURL != "" {
		msg, err := buildMediaStatusMessage(ctx, client, mediaURL, firstNonEmpty(req.GetCaption(), req.GetContent()), req.GetSendVideoAsGif())
		return msg, true, err
	}

	text := strings.TrimSpace(firstNonEmpty(req.GetContent(), req.GetCaption()))
	if text == "" {
		return nil, false, fmt.Errorf("status content or media_url is required")
	}
	ext := &waE2E.ExtendedTextMessage{
		Text:        proto.String(text),
		ContextInfo: statusContextInfo(waE2E.ContextInfo_TEXT),
	}
	if bg, ok := parseStatusARGB(req.GetColor()); ok {
		ext.BackgroundArgb = proto.Uint32(bg)
		ext.TextArgb = proto.Uint32(0xFFFFFFFF)
	}
	return &waE2E.Message{ExtendedTextMessage: ext}, false, nil
}

func buildMediaStatusMessage(ctx context.Context, client *whatsmeow.Client, mediaURL, caption string, videoAsGif bool) (*waE2E.Message, error) {
	data, mimeType, err := downloadStatusMedia(mediaURL)
	if err != nil {
		return nil, err
	}
	mediaKeyTimestamp := proto.Int64(time.Now().Unix())

	switch {
	case strings.HasPrefix(mimeType, "image/"):
		imageData, imageMime, thumb, width, height, err := prepareStatusImage(data, mimeType)
		if err != nil {
			return nil, err
		}
		uploaded, err := client.Upload(ctx, imageData, whatsmeow.MediaImage)
		if err != nil {
			return nil, fmt.Errorf("failed to upload status image: %w", err)
		}
		return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
			URL:               proto.String(uploaded.URL),
			DirectPath:        proto.String(uploaded.DirectPath),
			MediaKey:          uploaded.MediaKey,
			Mimetype:          proto.String(imageMime),
			FileEncSHA256:     uploaded.FileEncSHA256,
			FileSHA256:        uploaded.FileSHA256,
			FileLength:        proto.Uint64(uploaded.FileLength),
			MediaKeyTimestamp: mediaKeyTimestamp,
			Caption:           proto.String(caption),
			JPEGThumbnail:     thumb,
			Width:             proto.Uint32(width),
			Height:            proto.Uint32(height),
			ContextInfo:       statusContextInfo(waE2E.ContextInfo_IMAGE),
		}}, nil
	case strings.HasPrefix(mimeType, "video/"):
		uploaded, err := client.Upload(ctx, data, whatsmeow.MediaVideo)
		if err != nil {
			return nil, fmt.Errorf("failed to upload status video: %w", err)
		}
		sourceType := waE2E.ContextInfo_VIDEO
		if videoAsGif {
			sourceType = waE2E.ContextInfo_GIF
		}
		return &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
			URL:               proto.String(uploaded.URL),
			DirectPath:        proto.String(uploaded.DirectPath),
			MediaKey:          uploaded.MediaKey,
			Mimetype:          proto.String(mimeType),
			FileEncSHA256:     uploaded.FileEncSHA256,
			FileSHA256:        uploaded.FileSHA256,
			FileLength:        proto.Uint64(uploaded.FileLength),
			MediaKeyTimestamp: mediaKeyTimestamp,
			Caption:           proto.String(caption),
			GifPlayback:       proto.Bool(videoAsGif),
			ContextInfo:       statusContextInfo(sourceType),
		}}, nil
	default:
		return nil, fmt.Errorf("unsupported status media type: %s", mimeType)
	}
}

func statusContextInfo(source waE2E.ContextInfo_StatusSourceType) *waE2E.ContextInfo {
	return &waE2E.ContextInfo{StatusSourceType: source.Enum()}
}

func statusMessageKind(msg *waE2E.Message) string {
	switch {
	case msg == nil:
		return "unknown"
	case msg.GetImageMessage() != nil:
		return "image"
	case msg.GetVideoMessage() != nil:
		if msg.GetVideoMessage().GetGifPlayback() {
			return "gif"
		}
		return "video"
	case msg.GetExtendedTextMessage() != nil:
		return "text"
	default:
		return "unknown"
	}
}

func downloadStatusMedia(rawURL string) ([]byte, string, error) {
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(rawURL)
	if err != nil {
		return nil, "", fmt.Errorf("download status media: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("download status media failed: HTTP %d", resp.StatusCode)
	}
	if resp.ContentLength > statusMediaMaxBytes {
		return nil, "", fmt.Errorf("status media size %d exceeds maximum %d", resp.ContentLength, statusMediaMaxBytes)
	}
	limited := &io.LimitedReader{R: resp.Body, N: statusMediaMaxBytes + 1}
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, "", fmt.Errorf("read status media: %w", err)
	}
	if len(data) == 0 {
		return nil, "", fmt.Errorf("status media is empty")
	}
	if len(data) > statusMediaMaxBytes {
		return nil, "", fmt.Errorf("status media size %d exceeds maximum %d", len(data), statusMediaMaxBytes)
	}

	mimeType := normalizeStatusMediaMIME(resp.Header.Get("Content-Type"), rawURL, data)
	if !strings.HasPrefix(mimeType, "image/") && !strings.HasPrefix(mimeType, "video/") {
		return nil, "", fmt.Errorf("unsupported status media type: %s", mimeType)
	}
	return data, mimeType, nil
}

func normalizeStatusMediaMIME(contentType, rawURL string, data []byte) string {
	if idx := strings.Index(contentType, ";"); idx >= 0 {
		contentType = contentType[:idx]
	}
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	if strings.HasPrefix(contentType, "image/") || strings.HasPrefix(contentType, "video/") {
		return contentType
	}
	if detected := strings.ToLower(http.DetectContentType(data)); strings.HasPrefix(detected, "image/") || strings.HasPrefix(detected, "video/") {
		return detected
	}
	if parsed, err := url.Parse(rawURL); err == nil {
		if byExt := mime.TypeByExtension(strings.ToLower(filepath.Ext(parsed.Path))); byExt != "" {
			if idx := strings.Index(byExt, ";"); idx >= 0 {
				byExt = byExt[:idx]
			}
			byExt = strings.ToLower(strings.TrimSpace(byExt))
			if strings.HasPrefix(byExt, "image/") || strings.HasPrefix(byExt, "video/") {
				return byExt
			}
		}
	}
	lowerURL := strings.ToLower(rawURL)
	switch {
	case strings.Contains(lowerURL, "jpeg"), strings.Contains(lowerURL, "jpg"):
		return "image/jpeg"
	case strings.Contains(lowerURL, "png"):
		return "image/png"
	case strings.Contains(lowerURL, "webp"):
		return "image/webp"
	case strings.Contains(lowerURL, "mp4"):
		return "video/mp4"
	case strings.Contains(lowerURL, "mov"):
		return "video/quicktime"
	case strings.Contains(lowerURL, "webm"):
		return "video/webm"
	}
	return contentType
}

func prepareStatusImage(data []byte, mimeType string) ([]byte, string, []byte, uint32, uint32, error) {
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, "", nil, 0, 0, fmt.Errorf("decode status image: %w", err)
	}
	bounds := img.Bounds()
	width := uint32(bounds.Dx())
	height := uint32(bounds.Dy())
	thumb := makeStatusImageThumbnail(img)
	if strings.EqualFold(mimeType, "image/webp") {
		var buf bytes.Buffer
		if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: 90}); err != nil {
			return nil, "", nil, 0, 0, fmt.Errorf("convert webp status image: %w", err)
		}
		return buf.Bytes(), "image/jpeg", thumb, width, height, nil
	}
	return data, mimeType, thumb, width, height, nil
}

func makeStatusImageThumbnail(img image.Image) []byte {
	bounds := img.Bounds()
	if bounds.Dx() > statusThumbnailMaxEdge || bounds.Dy() > statusThumbnailMaxEdge {
		img = imaging.Fit(img, statusThumbnailMaxEdge, statusThumbnailMaxEdge, imaging.Lanczos)
	}
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: statusThumbnailJPEGQuality}); err != nil {
		return nil
	}
	return buf.Bytes()
}

func parseStatusARGB(raw string) (uint32, bool) {
	value := strings.TrimSpace(strings.TrimPrefix(raw, "#"))
	if len(value) != 6 && len(value) != 8 {
		return 0, false
	}
	var parsed uint64
	if _, err := fmt.Sscanf(value, "%x", &parsed); err != nil {
		return 0, false
	}
	if len(value) == 6 {
		parsed |= 0xFF000000
	}
	return uint32(parsed), true
}

func (s *Service) CommentStatus(ctx context.Context, req *bridgepb.CommentStatusRequest) (*bridgepb.CommentStatusResponse, error) {
	if req.GetAccountId() == "" || (req.GetMessageId() == "" && req.GetUserId() == "") {
		return nil, grpcError(fmt.Errorf("account_id and message_id or user_id are required"))
	}
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	inst, _ := whatsapp.DeviceFromContext(scoped)
	client := inst.GetClient()

	target, err := s.resolveStatusReplyTarget(scoped, client, req.GetMessageId(), req.GetUserId())
	if err != nil {
		return &bridgepb.CommentStatusResponse{Success: false, Error: err.Error()}, nil
	}
	comment := strings.TrimSpace(req.GetComment())
	if comment == "" {
		comment = "👍\u200B"
	}
	sentID, err := s.sendStatusReply(scoped, client, target, comment)
	if err != nil {
		return &bridgepb.CommentStatusResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.CommentStatusResponse{
		Success:         true,
		MessageId:       string(sentID),
		TargetUserId:    imsTargetUserID(target.TargetJID),
		Comment:         comment,
		Source:          target.Source,
		StatusMessageId: target.StatusMessageID,
	}, nil
}

func (s *Service) LikeStatus(ctx context.Context, req *bridgepb.LikeStatusRequest) (*bridgepb.LikeStatusResponse, error) {
	if req.GetAccountId() == "" || (req.GetMessageId() == "" && req.GetUserId() == "") {
		return nil, grpcError(fmt.Errorf("account_id and message_id or user_id are required"))
	}
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	inst, _ := whatsapp.DeviceFromContext(scoped)
	client := inst.GetClient()

	target, err := s.resolveStatusReplyTarget(scoped, client, req.GetMessageId(), req.GetUserId())
	if err != nil {
		return &bridgepb.LikeStatusResponse{Success: false, Error: err.Error()}, nil
	}
	emoji := strings.TrimSpace(req.GetEmoji())
	if emoji == "" {
		emoji = "👍"
	}
	if _, err := s.sendStatusReply(scoped, client, target, emoji); err != nil {
		return &bridgepb.LikeStatusResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.LikeStatusResponse{
		Success:         true,
		StatusMessageId: target.StatusMessageID,
		TargetUserId:    imsTargetUserID(target.TargetJID),
		Emoji:           emoji,
		Action:          "add",
		Source:          target.Source,
	}, nil
}

type statusReplyTarget struct {
	StatusMessageID string
	TargetJID       types.JID
	Source          string
	QuotedMessage   *waE2E.Message
}

func (s *Service) resolveStatusReplyTarget(ctx context.Context, client *whatsmeow.Client, messageID, userID string) (*statusReplyTarget, error) {
	if s.deps.ChatStorageRepo == nil {
		return nil, fmt.Errorf("chat storage is not available")
	}
	deviceID := currentDeviceStorageID(ctx, client)
	if deviceID == "" {
		return nil, fmt.Errorf("unable to resolve current account device")
	}

	if strings.TrimSpace(messageID) != "" {
		msg, err := findStatusMessageByID(s.deps.ChatStorageRepo, deviceID, messageID)
		if err != nil {
			return nil, err
		}
		return statusReplyTargetFromMessage(msg, "messageId")
	}

	targetJID, err := parseStatusUserJID(userID)
	if err != nil {
		return nil, err
	}
	msg, err := findLatestStatusMessageByUser(s.deps.ChatStorageRepo, deviceID, targetJID)
	if err != nil {
		return nil, err
	}
	return statusReplyTargetFromMessage(msg, "userId")
}

func (s *Service) sendStatusReply(ctx context.Context, client *whatsmeow.Client, target *statusReplyTarget, content string) (types.MessageID, error) {
	msg := buildStatusReplyMessage(target, content)
	resp, err := client.SendMessage(ctx, target.TargetJID, msg)
	if err != nil {
		return "", fmt.Errorf("failed to send status reply: %w", err)
	}
	if s.deps.ChatStorageRepo != nil {
		sender := ""
		if client.Store != nil && client.Store.ID != nil {
			sender = client.Store.ID.String()
		}
		if err := s.deps.ChatStorageRepo.StoreSentMessageWithContext(ctx, string(resp.ID), sender, target.TargetJID.String(), content, resp.Timestamp, msg); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"message_id":        string(resp.ID),
				"status_message_id": target.StatusMessageID,
				"target":            target.TargetJID.String(),
			}).Warn("failed to store status reply message")
		}
	}
	logrus.WithFields(logrus.Fields{
		"message_id":        string(resp.ID),
		"status_message_id": target.StatusMessageID,
		"target":            target.TargetJID.String(),
	}).Info("WhatsApp status reply acknowledged")
	return resp.ID, nil
}

func buildStatusReplyMessage(target *statusReplyTarget, content string) *waE2E.Message {
	return &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text: proto.String(content),
		ContextInfo: &waE2E.ContextInfo{
			StanzaID:      proto.String(target.StatusMessageID),
			Participant:   proto.String(target.TargetJID.ToNonAD().String()),
			RemoteJID:     proto.String(types.StatusBroadcastJID.String()),
			QuotedMessage: target.QuotedMessage,
		},
	}}
}

func statusReplyTargetFromMessage(msg *domainChatStorage.Message, source string) (*statusReplyTarget, error) {
	if msg == nil {
		return nil, fmt.Errorf("status message not found")
	}
	if msg.ChatJID != types.StatusBroadcastJID.String() {
		return nil, fmt.Errorf("message %s is not a status message", msg.ID)
	}
	targetJID, err := parseStatusUserJID(msg.Sender)
	if err != nil {
		return nil, fmt.Errorf("invalid status sender %s: %w", msg.Sender, err)
	}
	return &statusReplyTarget{
		StatusMessageID: normalizeStatusMessageID(msg.ID),
		TargetJID:       targetJID,
		Source:          source,
		QuotedMessage:   quotedStatusMessage(msg),
	}, nil
}

func findStatusMessageByID(repo domainChatStorage.IChatStorageRepository, deviceID, rawID string) (*domainChatStorage.Message, error) {
	targetID := normalizeStatusMessageID(rawID)
	messages, err := repo.GetMessages(&domainChatStorage.MessageFilter{
		DeviceID: deviceID,
		ChatJID:  types.StatusBroadcastJID.String(),
		Limit:    1000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find status message: %w", err)
	}
	for _, msg := range messages {
		if normalizeStatusMessageID(msg.ID) == targetID {
			return msg, nil
		}
	}

	for _, candidate := range []string{targetID, strings.TrimSpace(rawID)} {
		if candidate == "" {
			continue
		}
		msg, err := repo.GetMessageByID(candidate)
		if err != nil {
			return nil, fmt.Errorf("failed to find status message: %w", err)
		}
		if isStatusMessageForDevice(msg, deviceID) {
			return msg, nil
		}
	}
	return nil, fmt.Errorf("status message not found: %s", rawID)
}

func findLatestStatusMessageByUser(repo domainChatStorage.IChatStorageRepository, deviceID string, userJID types.JID) (*domainChatStorage.Message, error) {
	messages, err := repo.GetMessages(&domainChatStorage.MessageFilter{
		DeviceID: deviceID,
		ChatJID:  types.StatusBroadcastJID.String(),
		Limit:    1000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find latest status message: %w", err)
	}
	for _, msg := range messages {
		if statusSenderMatches(msg.Sender, userJID) {
			return msg, nil
		}
	}
	return nil, fmt.Errorf("no status found for user: %s", imsTargetUserID(userJID))
}

func isStatusMessageForDevice(msg *domainChatStorage.Message, deviceID string) bool {
	if msg == nil || msg.ChatJID != types.StatusBroadcastJID.String() {
		return false
	}
	return msg.DeviceID == "" || msg.DeviceID == deviceID
}

func statusSenderMatches(sender string, target types.JID) bool {
	senderJID, err := parseStatusUserJID(sender)
	if err != nil {
		return false
	}
	return senderJID.User == target.User || senderJID.ToNonAD().String() == target.ToNonAD().String()
}

func quotedStatusMessage(msg *domainChatStorage.Message) *waE2E.Message {
	content := strings.TrimSpace(msg.Content)
	switch strings.ToLower(msg.MediaType) {
	case "image":
		return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{Caption: proto.String(content)}}
	case "video":
		return &waE2E.Message{VideoMessage: &waE2E.VideoMessage{Caption: proto.String(content)}}
	default:
		if content == "" {
			content = "Status"
		}
		return &waE2E.Message{Conversation: proto.String(content)}
	}
}

func parseStatusUserJID(raw string) (types.JID, error) {
	value := strings.TrimSpace(raw)
	value = strings.TrimPrefix(value, "+")
	value = strings.ReplaceAll(value, "@c.us", "@s.whatsapp.net")
	if idx := strings.LastIndex(value, ":"); idx >= 0 && strings.Contains(value, "@s.whatsapp.net") {
		value = value[:idx] + value[strings.Index(value, "@s.whatsapp.net"):]
	}
	if !strings.Contains(value, "@") {
		return types.NewJID(value, types.DefaultUserServer), nil
	}
	return types.ParseJID(value)
}

func imsTargetUserID(jid types.JID) string {
	if jid.Server == types.DefaultUserServer {
		return jid.User + "@c.us"
	}
	return jid.ToNonAD().String()
}

func normalizeStatusMessageID(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" || !strings.Contains(value, "_") {
		return value
	}
	parts := strings.Split(value, "_")
	for i := len(parts) - 1; i >= 0; i-- {
		part := strings.TrimSpace(parts[i])
		if len(part) >= 12 && !strings.Contains(part, "@") {
			return part
		}
	}
	return value
}

func currentDeviceStorageID(ctx context.Context, client *whatsmeow.Client) string {
	if inst, ok := whatsapp.DeviceFromContext(ctx); ok && inst != nil {
		if jid := inst.JID(); jid != "" {
			return jid
		}
	}
	if client != nil && client.Store != nil && client.Store.ID != nil {
		return client.Store.ID.ToNonAD().String()
	}
	return ""
}

func (s *Service) GetStatusViewers(ctx context.Context, req *bridgepb.GetStatusViewersRequest) (*bridgepb.GetStatusViewersResponse, error) {
	if req.GetAccountId() == "" || req.GetMessageId() == "" {
		return nil, grpcError(fmt.Errorf("account_id and message_id are required"))
	}
	return &bridgepb.GetStatusViewersResponse{Success: true, Viewers: []*bridgepb.StatusViewer{}, TotalCount: 0, RemainingCount: 0}, nil
}

func (s *Service) DeleteMessage(ctx context.Context, req *bridgepb.DeleteMessageRequest) (*bridgepb.DeleteMessageResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.deps.MessageUsecase.DeleteMessage(scoped, domainMessage.DeleteRequest{MessageID: req.GetMessageId(), Phone: req.GetChatId()}); err != nil {
		return &bridgepb.DeleteMessageResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.DeleteMessageResponse{Success: true}, nil
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

var phonePattern = regexp.MustCompile(`\+?\d{7,18}`)

func parseContactData(data string) (string, string) {
	phone := phonePattern.FindString(data)
	phone = strings.TrimPrefix(phone, "+")
	name := "Contact"
	for _, line := range strings.Split(data, "\n") {
		if strings.HasPrefix(strings.ToUpper(line), "FN:") && len(line) > 3 {
			name = strings.TrimSpace(line[3:])
			break
		}
	}
	return name, phone
}
