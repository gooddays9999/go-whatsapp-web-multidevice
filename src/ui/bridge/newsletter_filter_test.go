package bridge

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestNewsletterFilterMatchesOnlyChannelChat(t *testing.T) {
	for _, tc := range []struct {
		chat string
		want bool
	}{
		{"123@newsletter", true}, {" 123@newsletter ", true},
		{"123@s.whatsapp.net", false}, {"123@c.us", false}, {"123@g.us", false},
		{"123@lid", false}, {"status@broadcast", false}, {"", false},
		{"123@newsletter.example", false},
	} {
		if got := isNewsletterChat(tc.chat); got != tc.want {
			t.Fatalf("%q: got %t", tc.chat, got)
		}
	}
}

func TestNewsletterEventsReturnBeforeMessageOrMediaProcessing(t *testing.T) {
	s := &Service{}
	evt := &events.Message{}
	evt.Info.Chat = types.NewJID("123", types.NewsletterServer)
	s.handleMessageEvent(context.Background(), "1", nil, evt)
	s.downloadAndPublishMedia(context.Background(), "1", nil, evt, nil, nil)
	receipt := &events.Receipt{Type: types.ReceiptTypeRead}
	receipt.Chat = evt.Info.Chat
	s.handleReceiptEvent(context.Background(), "1", nil, receipt)
	if err := s.publishRecoveredReply(context.Background(), nil, map[string]any{"chatId": "123@newsletter"}); err != nil {
		t.Fatal(err)
	}
	if s.newsletterSkipped.Load() != 4 {
		t.Fatalf("skipped=%d", s.newsletterSkipped.Load())
	}
}

func TestNewsletterPublishGuardPreservesOtherTrafficAndOldQueue(t *testing.T) {
	o, err := openEventOutbox(filepath.Join(t.TempDir(), "filter.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.db.Close()
	if err := o.enqueue(context.Background(), "existing-channel", "message.received", []byte(`{"message":{"chatId":"123@newsletter"}}`)); err != nil {
		t.Fatal(err)
	}
	s := &Service{publisher: &NATSPublisher{url: "test", outbox: o, wake: make(chan struct{}, 1)}}
	s.publish("message.received", "", map[string]any{"message": map[string]any{"chatId": "123@newsletter"}})
	s.publish("message.status", "", map[string]any{"chatId": "123@newsletter", "status": "read"})
	s.publish("message.media_ready", "", map[string]any{"chatId": "123@newsletter"})
	for _, chat := range []string{"123@s.whatsapp.net", "123@g.us", "status@broadcast", "123@lid"} {
		s.publish("message.received", "", map[string]any{"message": map[string]any{
			"chatId": chat, "content": map[string]any{"text": "https://whatsapp.com/channel/example"},
			"quotedMessage": map[string]any{"chatId": "123@newsletter"},
		}})
	}
	s.publish("message.status", "", map[string]any{"chatId": "123@s.whatsapp.net", "status": "delivered"})
	s.publish("account.connected", "", nil)
	stats, err := o.stats(context.Background())
	if err != nil || stats.Pending != 7 || s.newsletterSkipped.Load() != 3 {
		t.Fatalf("stats=%+v skipped=%d error=%v", stats, s.newsletterSkipped.Load(), err)
	}
	batch, err := o.batch(context.Background())
	if err != nil || batch[0].eventID != "existing-channel" {
		t.Fatalf("existing queue changed: %v", err)
	}
}

func TestNewsletterHistoryReceiptsAreSkippedButPrivateAndGroupRemain(t *testing.T) {
	data := &waHistorySync.HistorySync{}
	for _, chat := range []string{"123@newsletter", "123@s.whatsapp.net", "123@g.us"} {
		data.Conversations = append(data.Conversations, &waHistorySync.Conversation{
			ID: proto.String(chat), Messages: []*waHistorySync.HistorySyncMsg{
				historyStatusMessage(chat, chat, true, waWeb.WebMessageInfo_READ, uint64(time.Now().Unix())),
			},
		})
	}
	payloads := historySyncStatusEvents(context.Background(), nil, data)
	if len(payloads) != 2 {
		t.Fatalf("expected private and group receipts, got %+v", payloads)
	}
	for _, payload := range payloads {
		if isNewsletterChat(payload["chatId"].(string)) {
			t.Fatal("channel receipt escaped filter")
		}
	}
}
