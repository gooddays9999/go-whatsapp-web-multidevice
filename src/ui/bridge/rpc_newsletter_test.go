package bridge

import (
	"context"
	"strings"
	"testing"
	"time"

	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	"go.mau.fi/whatsmeow"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func TestNewsletterMetadataToProtoIncludesStableFields(t *testing.T) {
	meta := &types.NewsletterMetadata{
		ID: types.NewJID("120363123456789", types.NewsletterServer),
		ThreadMeta: types.NewsletterThreadMetadata{
			InviteCode:      "abc123",
			SubscriberCount: 42,
			Name:            types.NewsletterText{Text: "MINISO Updates"},
			Description:     types.NewsletterText{Text: "Daily store updates"},
		},
		ViewerMeta: &types.NewsletterViewerMetadata{Role: types.NewsletterRoleOwner},
	}

	got := newsletterMetadataToProto(meta)

	if got.GetId() != "120363123456789@newsletter" {
		t.Fatalf("id = %q", got.GetId())
	}
	if got.GetName() != "MINISO Updates" || got.GetDescription() != "Daily store updates" {
		t.Fatalf("text fields = %q/%q", got.GetName(), got.GetDescription())
	}
	if got.GetInviteLink() != "https://whatsapp.com/channel/abc123" {
		t.Fatalf("invite link = %q", got.GetInviteLink())
	}
	if got.GetSubscriberCount() != 42 {
		t.Fatalf("subscriber count = %d", got.GetSubscriberCount())
	}
	if got.GetRole() != "owner" {
		t.Fatalf("role = %q", got.GetRole())
	}
}

func TestNewsletterMetadataToProtoHandlesNil(t *testing.T) {
	if got := newsletterMetadataToProto(nil); got != nil {
		t.Fatalf("nil metadata converted to %#v", got)
	}
}

func TestNewsletterInviteCodeAcceptsLinksAndRawInvite(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "whatsapp link", raw: "https://whatsapp.com/channel/abc123", want: "abc123"},
		{name: "www whatsapp link", raw: "https://www.whatsapp.com/channel/xyz789", want: "xyz789"},
		{name: "raw invite", raw: "inviteOnly", want: "inviteOnly"},
		{name: "newsletter jid is not invite", raw: "120363123456789@newsletter", want: ""},
		{name: "other url ignored", raw: "https://example.com/channel/abc123", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newsletterInviteCode(tt.raw); got != tt.want {
				t.Fatalf("newsletterInviteCode(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

func TestNewsletterBridgeProtoHasVerificationRequests(t *testing.T) {
	_ = &bridgepb.CreateNewsletterRequest{AccountId: "357", Name: "Test", Description: "Desc"}
	_ = &bridgepb.FollowNewsletterRequest{AccountId: "434", NewsletterId: "120363123456789@newsletter"}
	_ = &bridgepb.GetNewslettersRequest{AccountId: "357"}
	_ = &bridgepb.GetNewsletterMessagesRequest{AccountId: "357", NewsletterId: "120363123456789@newsletter", Count: 10}
	_ = &bridgepb.SendNewsletterPollRequest{
		AccountId:    "357",
		NewsletterId: "120363123456789@newsletter",
		Question:     "Pick one",
		Options:      []string{"A", "B"},
		MaxAnswer:    1,
	}
}

func TestSendNewsletterPollMessageUsesWhatsmeowNewsletterSendPath(t *testing.T) {
	jid := types.NewJID("120363123456789", types.NewsletterServer)
	message := &waE2E.Message{
		PollCreationMessage: &waE2E.PollCreationMessage{
			Name: proto.String("Pick one"),
			Options: []*waE2E.PollCreationMessage_Option{
				{OptionName: proto.String("A")},
				{OptionName: proto.String("B")},
			},
			SelectableOptionsCount: proto.Uint32(1),
		},
		MessageContextInfo: &waE2E.MessageContextInfo{MessageSecret: []byte("secret")},
	}
	sender := &fakeNewsletterPollSender{
		response: whatsmeow.SendResponse{
			ID:       types.MessageID("3EB01234567890"),
			ServerID: 101,
		},
	}

	messageID, serverID, err := sendNewsletterPollMessage(t.Context(), sender, jid, message, 15*time.Second)
	if err != nil {
		t.Fatalf("sendNewsletterPollMessage returned error: %v", err)
	}

	if messageID != "3EB01234567890" {
		t.Fatalf("message id = %q", messageID)
	}
	if serverID != 101 {
		t.Fatalf("server id = %d", serverID)
	}
	if sender.to != jid {
		t.Fatalf("to = %#v, want %#v", sender.to, jid)
	}
	if sender.message != message {
		t.Fatalf("message was not sent through whatsmeow SendMessage")
	}
	if len(sender.extra) != 1 || sender.extra[0].Timeout != 15*time.Second {
		t.Fatalf("send extra = %#v", sender.extra)
	}
}

func TestSendNewsletterPollMessageRejectsAckWithoutServerID(t *testing.T) {
	jid := types.NewJID("120363123456789", types.NewsletterServer)
	message := &waE2E.Message{
		PollCreationMessage: &waE2E.PollCreationMessage{
			Name: proto.String("Pick one"),
			Options: []*waE2E.PollCreationMessage_Option{
				{OptionName: proto.String("A")},
				{OptionName: proto.String("B")},
			},
			SelectableOptionsCount: proto.Uint32(1),
		},
	}
	sender := &fakeNewsletterPollSender{
		response: whatsmeow.SendResponse{ID: types.MessageID("3EB0NO_SERVER_ID")},
	}

	_, _, err := sendNewsletterPollMessage(t.Context(), sender, jid, message, 15*time.Second)
	if err == nil {
		t.Fatalf("expected missing server_id to fail")
	}
	if !strings.Contains(err.Error(), "without server_id") {
		t.Fatalf("error = %v", err)
	}
}

type fakeNewsletterPollSender struct {
	to       types.JID
	message  *waE2E.Message
	extra    []whatsmeow.SendRequestExtra
	response whatsmeow.SendResponse
	err      error
}

func (f *fakeNewsletterPollSender) SendMessage(ctx context.Context, to types.JID, message *waE2E.Message, extra ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
	f.to = to
	f.message = message
	f.extra = extra
	return f.response, f.err
}

func TestNewsletterMessageToProtoTextMessage(t *testing.T) {
	msg := &types.NewsletterMessage{
		MessageServerID: 101,
		MessageID:       "3EB0TEXT",
		Type:            "text",
		Timestamp:       time.Unix(1719200000, 0),
		ViewsCount:      7,
		ReactionCounts:  map[string]int{"👍": 2},
		Message:         &waE2E.Message{Conversation: proto.String("IMS channel text test")},
	}

	got := newsletterMessageToProto(msg)

	if got.GetServerId() != "101" {
		t.Fatalf("server_id = %q", got.GetServerId())
	}
	if got.GetMessageId() != "3EB0TEXT" {
		t.Fatalf("message_id = %q", got.GetMessageId())
	}
	if got.GetType() != "text" {
		t.Fatalf("type = %q", got.GetType())
	}
	if got.GetTimestamp() != 1719200000 {
		t.Fatalf("timestamp = %d", got.GetTimestamp())
	}
	if got.GetText() != "IMS channel text test" {
		t.Fatalf("text = %q", got.GetText())
	}
	if got.GetHasPoll() {
		t.Fatalf("has_poll = true")
	}
	if got.GetViewsCount() != 7 {
		t.Fatalf("views_count = %d", got.GetViewsCount())
	}
	if got.GetReactionCounts()["👍"] != 2 {
		t.Fatalf("reaction count = %d", got.GetReactionCounts()["👍"])
	}
}

func TestNewsletterMessageToProtoPollV3Message(t *testing.T) {
	msg := &types.NewsletterMessage{
		MessageServerID: 102,
		MessageID:       "3EB0POLL",
		Type:            "pollCreation",
		Timestamp:       time.Unix(1719200100, 0),
		Message: &waE2E.Message{PollCreationMessageV3: &waE2E.PollCreationMessage{
			Name: proto.String("Pick one"),
			Options: []*waE2E.PollCreationMessage_Option{
				{OptionName: proto.String("A")},
				{OptionName: proto.String("B")},
			},
			SelectableOptionsCount: proto.Uint32(1),
		}},
	}

	got := newsletterMessageToProto(msg)

	if !got.GetHasPoll() {
		t.Fatalf("has_poll = false")
	}
	if got.GetPollField() != "pollCreationMessageV3" {
		t.Fatalf("poll_field = %q", got.GetPollField())
	}
	if got.GetPollName() != "Pick one" {
		t.Fatalf("poll_name = %q", got.GetPollName())
	}
	if got.GetOptionCount() != 2 {
		t.Fatalf("option_count = %d", got.GetOptionCount())
	}
	if got.GetSelectableOptionsCount() != 1 {
		t.Fatalf("selectable_options_count = %d", got.GetSelectableOptionsCount())
	}
}
