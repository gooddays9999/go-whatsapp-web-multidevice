package bridge

import (
	"testing"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
)

func TestBuildRecentHistorySyncPlanUsesRecentChatsAndOutgoingMessages(t *testing.T) {
	now := time.Date(2026, time.June, 11, 4, 0, 0, 0, time.UTC)
	repo := &recentHistorySyncRepoStub{
		chats: []*domainChatStorage.Chat{
			{DeviceID: "sender@s.whatsapp.net", JID: "receiver-a@s.whatsapp.net", LastMessageTime: now},
			{DeviceID: "sender@s.whatsapp.net", JID: "receiver-b@s.whatsapp.net", LastMessageTime: now.Add(-time.Minute)},
			{DeviceID: "sender@s.whatsapp.net", JID: "receiver-c@s.whatsapp.net", LastMessageTime: now.Add(-2 * time.Minute)},
		},
		messages: map[string][]*domainChatStorage.Message{
			"receiver-a@s.whatsapp.net": {
				{ID: "a-latest", DeviceID: "sender@s.whatsapp.net", ChatJID: "receiver-a@s.whatsapp.net", Sender: "receiver-a@s.whatsapp.net", Timestamp: now, IsFromMe: false},
				{ID: "a-outgoing", DeviceID: "sender@s.whatsapp.net", ChatJID: "receiver-a@s.whatsapp.net", Sender: "sender@s.whatsapp.net", Timestamp: now.Add(-time.Second), IsFromMe: true},
			},
			"receiver-b@s.whatsapp.net": {
				{ID: "b-latest-outgoing", DeviceID: "sender@s.whatsapp.net", ChatJID: "receiver-b@s.whatsapp.net", Sender: "sender@s.whatsapp.net", Timestamp: now.Add(-time.Minute), IsFromMe: true},
			},
			"receiver-c@s.whatsapp.net": {
				{ID: "c-latest", DeviceID: "sender@s.whatsapp.net", ChatJID: "receiver-c@s.whatsapp.net", Sender: "receiver-c@s.whatsapp.net", Timestamp: now.Add(-2 * time.Minute), IsFromMe: false},
			},
		},
	}

	plan, err := buildRecentHistorySyncPlan(repo, "sender@s.whatsapp.net", 2, 2)

	if err != nil {
		t.Fatalf("buildRecentHistorySyncPlan() error = %v", err)
	}
	if len(plan.HistoryAnchors) != 2 {
		t.Fatalf("history anchors len = %d, want 2", len(plan.HistoryAnchors))
	}
	if plan.HistoryAnchors[0].ID != "a-latest" || plan.HistoryAnchors[1].ID != "b-latest-outgoing" {
		t.Fatalf("history anchor ids = %s,%s; want a-latest,b-latest-outgoing", plan.HistoryAnchors[0].ID, plan.HistoryAnchors[1].ID)
	}
	if len(plan.ExactMessages) != 2 {
		t.Fatalf("exact messages len = %d, want 2", len(plan.ExactMessages))
	}
	if plan.ExactMessages[0].ID != "a-outgoing" || plan.ExactMessages[1].ID != "b-latest-outgoing" {
		t.Fatalf("exact ids = %s,%s; want a-outgoing,b-latest-outgoing", plan.ExactMessages[0].ID, plan.ExactMessages[1].ID)
	}
}

type recentHistorySyncRepoStub struct {
	chats    []*domainChatStorage.Chat
	messages map[string][]*domainChatStorage.Message
}

func (r *recentHistorySyncRepoStub) GetChats(filter *domainChatStorage.ChatFilter) ([]*domainChatStorage.Chat, error) {
	limit := filter.Limit
	if limit <= 0 || limit > len(r.chats) {
		limit = len(r.chats)
	}
	return r.chats[:limit], nil
}

func (r *recentHistorySyncRepoStub) GetMessages(filter *domainChatStorage.MessageFilter) ([]*domainChatStorage.Message, error) {
	var result []*domainChatStorage.Message
	for _, msg := range r.messages[filter.ChatJID] {
		if filter.IsFromMe != nil && msg.IsFromMe != *filter.IsFromMe {
			continue
		}
		result = append(result, msg)
		if filter.Limit > 0 && len(result) >= filter.Limit {
			break
		}
	}
	return result, nil
}
