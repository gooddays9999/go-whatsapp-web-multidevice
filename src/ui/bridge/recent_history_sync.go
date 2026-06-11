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
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()

	plan, err := buildRecentHistorySyncPlan(repo, deviceID, maxChats, exactPerChat)
	if err != nil {
		logrus.WithError(err).WithField("account_id", accountID).Warn("failed to build recent history sync plan")
		return
	}
	if len(plan.HistoryAnchors) == 0 && len(plan.ExactMessages) == 0 {
		return
	}

	var historyOK, historyFailed, exactOK, exactFailed int
	for _, anchor := range plan.HistoryAnchors {
		if err := requestHistoryAroundMessage(ctx, client, anchor, count); err != nil {
			historyFailed++
			logrus.WithError(err).WithFields(logrus.Fields{
				"account_id":  accountID,
				"message_id":  anchor.ID,
				"chat":        anchor.Chat.String(),
				"sync_reason": reason,
			}).Warn("failed to request on-demand history sync")
			continue
		}
		historyOK++
	}
	for _, msg := range plan.ExactMessages {
		if err := requestExactMessageResend(ctx, client, msg); err != nil {
			exactFailed++
			logrus.WithError(err).WithFields(logrus.Fields{
				"account_id":  accountID,
				"message_id":  msg.ID,
				"chat":        msg.Chat.String(),
				"sync_reason": reason,
			}).Warn("failed to request exact message history sync")
			continue
		}
		exactOK++
	}
	logrus.WithFields(logrus.Fields{
		"account_id":      accountID,
		"sync_reason":     reason,
		"history_ok":      historyOK,
		"history_failed":  historyFailed,
		"exact_ok":        exactOK,
		"exact_failed":    exactFailed,
		"history_anchors": len(plan.HistoryAnchors),
		"exact_messages":  len(plan.ExactMessages),
	}).Info("requested recent WhatsApp history sync")
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
