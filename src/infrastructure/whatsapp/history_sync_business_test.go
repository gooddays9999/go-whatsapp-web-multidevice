package whatsapp

import (
	"context"
	"testing"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// History sync delivers messages still wrapped (e.g. in disappearing-message
// chats). A wrapped captioned image must keep its media columns, or re-syncing
// a row the live path stored would wipe them, and a wrapped business template
// must not be skipped. Ported from upstream 35d2e46 (#857).
func TestProcessConversationMessagesUnwrapsHistoryMessages(t *testing.T) {
	originalLog := log
	log = waLog.Noop
	defer func() { log = originalLog }()

	deviceID := "device-a@s.whatsapp.net"
	chatJID := "628123456789@s.whatsapp.net"
	repo := &historyMessageBatchRepoSpy{}
	ctx := ContextWithDevice(context.Background(), NewDeviceInstance(deviceID, nil, nil))
	syncType := waHistorySync.HistorySync_RECENT
	timestamp := uint64(time.Date(2026, time.September, 1, 8, 0, 0, 0, time.UTC).Unix())
	ephemeral := func(id string, inner *waE2E.Message) *waHistorySync.HistorySyncMsg {
		return &waHistorySync.HistorySyncMsg{Message: &waWeb.WebMessageInfo{
			Key:              &waCommon.MessageKey{RemoteJID: proto.String(chatJID), FromMe: proto.Bool(false), ID: proto.String(id)},
			Message:          &waE2E.Message{EphemeralMessage: &waE2E.FutureProofMessage{Message: inner}},
			MessageTimestamp: &timestamp,
		}}
	}
	data := &waHistorySync.HistorySync{
		SyncType: &syncType,
		Conversations: []*waHistorySync.Conversation{{
			ID: proto.String(chatJID),
			Messages: []*waHistorySync.HistorySyncMsg{
				ephemeral("img-1", &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
					Caption:    proto.String("look at this"),
					DirectPath: proto.String("/v/t62/abc"),
					MediaKey:   []byte{1, 2, 3},
				}}),
				ephemeral("tpl-1", &waE2E.Message{TemplateMessage: &waE2E.TemplateMessage{
					HydratedTemplate: &waE2E.TemplateMessage_HydratedFourRowTemplate{
						Title:               &waE2E.TemplateMessage_HydratedFourRowTemplate_HydratedTitleText{HydratedTitleText: "Order confirmed"},
						HydratedContentText: proto.String("Your order #42 has shipped"),
					},
				}}),
			},
		}},
	}

	if err := processConversationMessages(ctx, data, repo, nil); err != nil {
		t.Fatalf("processConversationMessages: %v", err)
	}

	if len(repo.lastBatch) != 2 {
		t.Fatalf("expected both wrapped messages to be stored, got %d", len(repo.lastBatch))
	}
	image, template := repo.lastBatch[0], repo.lastBatch[1]
	if image.MediaType != "image" || image.DirectPath != "/v/t62/abc" || len(image.MediaKey) == 0 || image.Content != "look at this" {
		t.Fatalf("wrapped image lost its media: %+v", image)
	}
	if template.Content != "Order confirmed\nYour order #42 has shipped" {
		t.Fatalf("unexpected template content %q", template.Content)
	}
	if repo.lastStoredChat == nil || repo.lastStoredChat.JID != chatJID {
		t.Fatalf("expected the chat to be stored, got %+v", repo.lastStoredChat)
	}
}

type historyMessageBatchRepoSpy struct {
	domainChatStorage.IChatStorageRepository
	lastBatch      []*domainChatStorage.Message
	existingChat   *domainChatStorage.Chat
	lastStoredChat *domainChatStorage.Chat
}

func (r *historyMessageBatchRepoSpy) StoreChat(chat *domainChatStorage.Chat) error {
	r.lastStoredChat = chat
	return nil
}

func (r *historyMessageBatchRepoSpy) GetChatByDevice(_, _ string) (*domainChatStorage.Chat, error) {
	return r.existingChat, nil
}

func (r *historyMessageBatchRepoSpy) StoreMessagesBatch(messages []*domainChatStorage.Message) error {
	r.lastBatch = messages
	return nil
}

func (r *historyMessageBatchRepoSpy) GetChatNameWithPushName(jid types.JID, _ string, _ string, pushName string) string {
	if pushName != "" {
		return pushName
	}
	return jid.String()
}
