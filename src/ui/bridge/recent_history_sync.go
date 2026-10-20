package bridge

import (
	"context"
	"fmt"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/sirupsen/logrus"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

const (
	defaultHistorySyncMaxChats             = 20
	defaultHistorySyncMessageCount         = 50
	defaultHistorySyncExactOutgoingPerChat = 2
	defaultHistorySyncTimeout              = 30 * time.Second
	defaultHistorySyncMinInterval          = 5 * time.Minute
)

type recentHistorySyncStore interface {
	GetChats(filter *domainChatStorage.ChatFilter) ([]*domainChatStorage.Chat, error)
	GetMessages(filter *domainChatStorage.MessageFilter) ([]*domainChatStorage.Message, error)
}

type recentHistorySyncPlan struct {
	HistoryAnchors []*types.MessageInfo
	ExactMessages  []*types.MessageInfo
}

func (s *Service) scheduleRecentHistorySync(parent context.Context, accountID string, inst *whatsapp.DeviceInstance, reason string) {
	if s == nil || inst == nil || !s.cfg.HistorySyncOnConnect {
		return
	}
	if !s.claimRecentHistorySync(accountID) {
		return
	}
	go s.requestRecentHistorySync(parent, accountID, inst, reason)
}

func (s *Service) claimRecentHistorySync(accountID string) bool {
	if s == nil || accountID == "" {
		return false
	}
	interval := s.cfg.HistorySyncMinInterval
	if interval <= 0 {
		interval = defaultHistorySyncMinInterval
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.historySyncRequested == nil {
		s.historySyncRequested = make(map[string]time.Time)
	}
	if last := s.historySyncRequested[accountID]; !last.IsZero() && now.Sub(last) < interval {
		return false
	}
	s.historySyncRequested[accountID] = now
	return true
}

func (s *Service) requestRecentHistorySync(parent context.Context, accountID string, inst *whatsapp.DeviceInstance, reason string) {
	if s.historySyncSlots != nil {
		select {
		case s.historySyncSlots <- struct{}{}:
			defer func() { <-s.historySyncSlots }()
		case <-parent.Done():
			return
		}
	}
	client := inst.GetClient()
	if client == nil || !client.IsLoggedIn() {
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
	maxChats := s.cfg.HistorySyncMaxChats
	if maxChats <= 0 {
		maxChats = defaultHistorySyncMaxChats
	}
	exactPerChat := s.cfg.HistorySyncExactOutgoingPerChat
	if exactPerChat < 0 {
		exactPerChat = 0
	}
	count := s.cfg.HistorySyncMessageCount
	if count <= 0 {
		count = defaultHistorySyncMessageCount
	}
	timeout := s.cfg.HistorySyncTimeout
	if timeout <= 0 {
		timeout = defaultHistorySyncTimeout
	}
	planStart := time.Now()
	plan, err := buildRecentHistorySyncPlan(repo, deviceID, maxChats, exactPerChat)
	planDuration := time.Since(planStart)
	if err != nil {
		logrus.WithError(err).WithField("account_id", accountID).Warn("failed to build recent history sync plan")
		return
	}
	if len(plan.HistoryAnchors) == 0 && len(plan.ExactMessages) == 0 {
		return
	}

	sendStart := time.Now()
	result := sendRecentHistorySyncPlan(
		context.WithoutCancel(parent),
		logrus.WithFields(logrus.Fields{"account_id": accountID, "sync_reason": reason}),
		plan,
		timeout,
		func(ctx context.Context, anchor *types.MessageInfo) error {
			return requestHistoryAroundMessage(ctx, client, anchor, count)
		},
		func(ctx context.Context, msg *types.MessageInfo) error {
			return requestExactMessageResend(ctx, client, msg)
		},
	)
	fields := logrus.Fields{
		"account_id":      accountID,
		"sync_reason":     reason,
		"history_ok":      result.historyOK,
		"history_failed":  result.historyFailed,
		"exact_ok":        result.exactOK,
		"exact_failed":    result.exactFailed,
		"skipped":         result.skipped,
		"history_anchors": len(plan.HistoryAnchors),
		"exact_messages":  len(plan.ExactMessages),
		"plan_ms":         planDuration.Milliseconds(),
		"send_ms":         time.Since(sendStart).Milliseconds(),
	}
	if result.aborted {
		logrus.WithError(result.abortErr).WithFields(fields).Warn("recent WhatsApp history sync aborted after a request timed out")
		return
	}
	logrus.WithFields(fields).Info("requested recent WhatsApp history sync")
}

type historySyncRequestFunc func(ctx context.Context, msg *types.MessageInfo) error

type recentHistorySyncResult struct {
	historyOK, historyFailed int
	exactOK, exactFailed     int
	skipped                  int
	aborted                  bool
	abortErr                 error
}

// sendRecentHistorySyncPlan sends the plan's requests one by one, each with its
// own timeout so a slow request cannot eat the budget of the ones after it.
// A timed-out request means the client is stalled (whatsmeow's send lock is not
// context-aware, so a busy client makes every following request wait the full
// timeout too), so the rest of the batch is skipped instead of failing one by
// one. whatsmeow formats the cause with %v, so the timeout is detected from the
// request context rather than with errors.Is.
func sendRecentHistorySyncPlan(parent context.Context, logger *logrus.Entry, plan *recentHistorySyncPlan, timeout time.Duration, history, exact historySyncRequestFunc) recentHistorySyncResult {
	type request struct {
		msg     *types.MessageInfo
		send    historySyncRequestFunc
		isExact bool
	}
	requests := make([]request, 0, len(plan.HistoryAnchors)+len(plan.ExactMessages))
	for _, anchor := range plan.HistoryAnchors {
		requests = append(requests, request{msg: anchor, send: history})
	}
	for _, msg := range plan.ExactMessages {
		requests = append(requests, request{msg: msg, send: exact, isExact: true})
	}

	var result recentHistorySyncResult
	for i, req := range requests {
		if parent.Err() != nil {
			result.aborted = true
			result.skipped = len(requests) - i
			return result
		}
		timedOut, err := sendHistorySyncRequest(parent, timeout, req.send, req.msg)
		switch {
		case err == nil && req.isExact:
			result.exactOK++
			continue
		case err == nil:
			result.historyOK++
			continue
		case req.isExact:
			result.exactFailed++
		default:
			result.historyFailed++
		}
		if timedOut {
			result.aborted = true
			result.abortErr = err
			result.skipped = len(requests) - i - 1
			return result
		}
		logger.WithError(err).WithFields(logrus.Fields{
			"message_id": req.msg.ID,
			"chat":       req.msg.Chat.String(),
			"exact":      req.isExact,
		}).Warn("failed to request WhatsApp history sync")
	}
	return result
}

func sendHistorySyncRequest(parent context.Context, timeout time.Duration, send historySyncRequestFunc, msg *types.MessageInfo) (timedOut bool, err error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	err = send(ctx, msg)
	return err != nil && ctx.Err() != nil, err
}

func requestHistoryAroundMessage(ctx context.Context, client *whatsmeow.Client, anchor *types.MessageInfo, count int) error {
	if client == nil || anchor == nil || anchor.ID == "" || anchor.Chat.IsEmpty() {
		return fmt.Errorf("history anchor is invalid")
	}
	_, err := client.SendPeerMessage(ctx, client.BuildHistorySyncRequest(anchor, count))
	return err
}

func requestExactMessageResend(ctx context.Context, client *whatsmeow.Client, msg *types.MessageInfo) error {
	if client == nil || msg == nil || msg.ID == "" || msg.Chat.IsEmpty() {
		return fmt.Errorf("exact message anchor is invalid")
	}
	sender := msg.Sender
	if msg.IsFromMe {
		sender = types.EmptyJID
	}
	_, err := client.SendPeerMessage(ctx, client.BuildUnavailableMessageRequest(msg.Chat, sender, msg.ID))
	return err
}

func buildRecentHistorySyncPlan(repo recentHistorySyncStore, deviceID string, maxChats, exactOutgoingPerChat int) (*recentHistorySyncPlan, error) {
	if repo == nil || deviceID == "" || maxChats <= 0 {
		return &recentHistorySyncPlan{}, nil
	}
	chats, err := repo.GetChats(&domainChatStorage.ChatFilter{
		DeviceID: deviceID,
		Limit:    maxChats,
	})
	if err != nil {
		return nil, err
	}
	plan := &recentHistorySyncPlan{}
	exactSeen := make(map[string]struct{})
	for _, chat := range chats {
		if chat == nil || chat.JID == "" {
			continue
		}
		chatJID, err := types.ParseJID(chat.JID)
		if err != nil || chatJID.IsEmpty() {
			continue
		}
		messages, err := repo.GetMessages(&domainChatStorage.MessageFilter{
			DeviceID: deviceID,
			ChatJID:  chat.JID,
			Limit:    1,
		})
		if err != nil {
			return nil, err
		}
		if len(messages) > 0 {
			if info := messageInfoFromStoredMessage(chatJID, messages[0]); info != nil {
				plan.HistoryAnchors = append(plan.HistoryAnchors, info)
			}
		}
		if exactOutgoingPerChat <= 0 {
			continue
		}
		isFromMe := true
		outgoing, err := repo.GetMessages(&domainChatStorage.MessageFilter{
			DeviceID: deviceID,
			ChatJID:  chat.JID,
			Limit:    exactOutgoingPerChat,
			IsFromMe: &isFromMe,
		})
		if err != nil {
			return nil, err
		}
		for _, msg := range outgoing {
			info := messageInfoFromStoredMessage(chatJID, msg)
			if info == nil {
				continue
			}
			key := info.Chat.String() + "\x00" + string(info.ID)
			if _, ok := exactSeen[key]; ok {
				continue
			}
			exactSeen[key] = struct{}{}
			plan.ExactMessages = append(plan.ExactMessages, info)
		}
	}
	return plan, nil
}

func messageInfoFromStoredMessage(chatJID types.JID, msg *domainChatStorage.Message) *types.MessageInfo {
	if msg == nil || msg.ID == "" || chatJID.IsEmpty() {
		return nil
	}
	sender := types.EmptyJID
	if msg.Sender != "" {
		if parsed, err := types.ParseJID(msg.Sender); err == nil {
			sender = parsed
		}
	}
	return &types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     chatJID,
			Sender:   sender,
			IsFromMe: msg.IsFromMe,
			IsGroup:  chatJID.Server == types.GroupServer,
		},
		ID:        types.MessageID(msg.ID),
		Timestamp: msg.Timestamp,
	}
}
