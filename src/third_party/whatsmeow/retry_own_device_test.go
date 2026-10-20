package whatsmeow

import (
	"context"
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestOwnDeviceRetryFindsMessageCachedUnderRecipient(t *testing.T) {
	cli := &Client{
		Store:             store.NoopDevice,
		recentMessagesMap: make(map[recentMessageKey]RecentMessage),
		GetMessageForRetry: func(types.JID, types.JID, types.MessageID) *waE2E.Message {
			return nil
		},
	}
	recipient := types.NewJID("17606277755", types.DefaultUserServer)
	ownLID := types.NewJID("195962882515124", types.HiddenUserServer)
	message := &waE2E.Message{Conversation: proto.String("sent to recipient")}
	if err := cli.addRecentMessage(context.Background(), recipient, "3EB0WEBMAC", message, nil); err != nil {
		t.Fatal(err)
	}
	receipt := &events.Receipt{MessageSource: types.MessageSource{
		Chat: ownLID, Sender: ownLID, IsFromMe: true,
	}}

	got, err := cli.getMessageForRetry(context.Background(), receipt, "3EB0WEBMAC")
	if err != nil || got == nil || got.wa != message {
		t.Fatalf("own-device retry found %#v, err=%v; want original message", got, err)
	}
	wrapped := wrapOwnDeviceRetryMessage(receipt, got)
	if destination := wrapped.GetDeviceSentMessage().GetDestinationJID(); destination != recipient.String() {
		t.Fatalf("retry destination = %q, want recipient %q", destination, recipient)
	}
}

func TestOwnDeviceRetryDoesNotGuessBetweenRecipients(t *testing.T) {
	cli := &Client{recentMessagesMap: make(map[recentMessageKey]RecentMessage)}
	first := types.NewJID("17606277755", types.DefaultUserServer)
	second := types.NewJID("17606277756", types.DefaultUserServer)
	cli.recentMessagesMap[recentMessageKey{first, "same-id"}] = RecentMessage{wa: &waE2E.Message{}, to: first}
	cli.recentMessagesMap[recentMessageKey{second, "same-id"}] = RecentMessage{wa: &waE2E.Message{}, to: second}
	if got := cli.getRecentMessageForOwnDevice("same-id"); !got.IsEmpty() {
		t.Fatalf("ambiguous retry resolved to %s", got.to)
	}
}
