package bridge

import (
	"testing"
	"time"

	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
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
		{name: "whatsapp message link", raw: "https://whatsapp.com/channel/abc123/104", want: "abc123"},
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
	_ = &bridgepb.VoteNewsletterPollRequest{
		AccountId:    "434",
		NewsletterId: "120363123456789@newsletter",
		MessageId:    "3EB0POLL",
		ServerId:     102,
		Options:      []string{"A"},
		Count:        50,
	}
	_ = &bridgepb.ReactNewsletterMessageRequest{
		AccountId:    "434",
		NewsletterId: "120363123456789@newsletter",
		ServerId:     104,
		Emoji:        "👍",
	}
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
	if len(got.GetPollOptions()) != 2 || got.GetPollOptions()[0] != "A" || got.GetPollOptions()[1] != "B" {
		t.Fatalf("poll_options = %#v", got.GetPollOptions())
	}
}

func TestFindNewsletterPollMessageFindsByMessageID(t *testing.T) {
	items := []*types.NewsletterMessage{
		{MessageServerID: 101, MessageID: "3EB0TEXT", Message: &waE2E.Message{Conversation: proto.String("text")}},
		{MessageServerID: 102, MessageID: "3EB0POLL", Message: testNewsletterPollMessage("Pick one")},
	}

	got, err := findNewsletterPollMessage(items, "3EB0POLL", 0)
	if err != nil {
		t.Fatalf("findNewsletterPollMessage returned error: %v", err)
	}
	if got.MessageServerID != 102 {
		t.Fatalf("server id = %d", got.MessageServerID)
	}
}

func TestFindNewsletterPollMessageFindsByServerID(t *testing.T) {
	items := []*types.NewsletterMessage{
		{MessageServerID: 101, MessageID: "3EB0TEXT", Message: &waE2E.Message{Conversation: proto.String("text")}},
		{MessageServerID: 102, MessageID: "3EB0POLL", Message: testNewsletterPollMessage("Pick one")},
	}

	got, err := findNewsletterPollMessage(items, "", 102)
	if err != nil {
		t.Fatalf("findNewsletterPollMessage returned error: %v", err)
	}
	if got.MessageID != "3EB0POLL" {
		t.Fatalf("message id = %s", got.MessageID)
	}
}

func TestFindNewsletterPollMessageRejectsNonPoll(t *testing.T) {
	items := []*types.NewsletterMessage{
		{MessageServerID: 101, MessageID: "3EB0TEXT", Message: &waE2E.Message{Conversation: proto.String("text")}},
	}

	if _, err := findNewsletterPollMessage(items, "3EB0TEXT", 0); err == nil {
		t.Fatalf("expected non-poll message to fail")
	}
}

// A requested server_id that is not present must never resolve to a different
// poll. This is the "vote landed on the wrong poll" incident: the caller asks to
// vote on server_id 31958, that message is not in what came back, and the only
// poll around is a different one (31970). Returning it would silently vote on the
// wrong poll and report success. The lookup must fail instead — a visible failure
// is recoverable, a wrong vote billed as success is not.
func TestFindNewsletterPollMessageServerIDMismatchNeverReturnsAnotherPoll(t *testing.T) {
	items := []*types.NewsletterMessage{
		{MessageServerID: 31970, MessageID: "3EB0POLLA", Message: testNewsletterPollMessage("Pick one")},
		{MessageServerID: 31973, MessageID: "3EB0POLLB", Message: testNewsletterPollMessage("Pick two")},
	}

	got, err := findNewsletterPollMessage(items, "", 31958)
	if err == nil {
		t.Fatalf("expected error for absent server_id 31958, got poll at server_id %d", got.MessageServerID)
	}
}

// The exact 31958 case: the requested server_id IS present but is a plain text
// post, while a real poll sits a few messages later. The lookup must reject the
// text post — it must not skip past it to the nearby poll.
func TestFindNewsletterPollMessageRequestedServerIDIsTextWithNearbyPoll(t *testing.T) {
	items := []*types.NewsletterMessage{
		{MessageServerID: 31958, MessageID: "3EB0TEXT", Message: &waE2E.Message{Conversation: proto.String("plain post")}},
		{MessageServerID: 31970, MessageID: "3EB0POLL", Message: testNewsletterPollMessage("Pick one")},
	}

	got, err := findNewsletterPollMessage(items, "", 31958)
	if err == nil {
		t.Fatalf("expected error: server_id 31958 is text, but got poll at server_id %d", got.MessageServerID)
	}
}

// Poll update and empty (option-less) poll messages are not votable poll
// creations and must be refused rather than treated as a poll.
func TestFindNewsletterPollMessageRejectsPollUpdateAndEmptyV4(t *testing.T) {
	items := []*types.NewsletterMessage{
		{MessageServerID: 200, MessageID: "3EB0UPD", Message: &waE2E.Message{PollUpdateMessage: &waE2E.PollUpdateMessage{}}},
	}
	if _, err := findNewsletterPollMessage(items, "", 200); err == nil {
		t.Fatalf("expected poll update message to be refused")
	}

	empty := []*types.NewsletterMessage{
		{MessageServerID: 201, MessageID: "3EB0V4", Message: &waE2E.Message{PollCreationMessageV4: &waE2E.FutureProofMessage{}}},
	}
	if _, err := findNewsletterPollMessage(empty, "", 201); err == nil {
		t.Fatalf("expected option-less V4 poll to be refused")
	}
}

// A message whose content WhatsApp stripped in deep history (Message==nil, or an
// empty body) must be flagged metadata_incomplete, while a real text/poll/media
// post must not be — otherwise a stripped poll is indistinguishable from a
// genuine text post.
func TestNewsletterMessageToProtoFlagsStrippedContent(t *testing.T) {
	stripped := newsletterMessageToProto(&types.NewsletterMessage{
		MessageServerID: 31958, MessageID: "3EB0GONE", Type: "text", ViewsCount: 9, Message: nil,
	})
	if !stripped.GetMetadataIncomplete() {
		t.Fatalf("stripped message (Message==nil) not flagged metadata_incomplete")
	}
	if stripped.GetHasPoll() || stripped.GetText() != "" {
		t.Fatalf("stripped message should carry no poll/text: %#v", stripped)
	}

	empty := newsletterMessageToProto(&types.NewsletterMessage{
		MessageServerID: 31959, MessageID: "3EB0EMPTY", Type: "text", Message: &waE2E.Message{},
	})
	if !empty.GetMetadataIncomplete() {
		t.Fatalf("empty-body message not flagged metadata_incomplete")
	}

	text := newsletterMessageToProto(&types.NewsletterMessage{
		MessageServerID: 101, MessageID: "3EB0TEXT", Type: "text",
		Message: &waE2E.Message{Conversation: proto.String("real post")},
	})
	if text.GetMetadataIncomplete() {
		t.Fatalf("genuine text post wrongly flagged metadata_incomplete")
	}

	poll := newsletterMessageToProto(&types.NewsletterMessage{
		MessageServerID: 102, MessageID: "3EB0POLL", Type: "pollCreation",
		Message: testNewsletterPollMessage("Pick one"),
	})
	if poll.GetMetadataIncomplete() {
		t.Fatalf("poll with content wrongly flagged metadata_incomplete")
	}
}

func TestNewsletterMessageContentAvailable(t *testing.T) {
	if newsletterMessageContentAvailable(nil) {
		t.Fatalf("nil message reported as content available")
	}
	if newsletterMessageContentAvailable(&waE2E.Message{}) {
		t.Fatalf("empty message reported as content available")
	}
	if !newsletterMessageContentAvailable(&waE2E.Message{Conversation: proto.String("hi")}) {
		t.Fatalf("text message reported as content unavailable")
	}
	if !newsletterMessageContentAvailable(testNewsletterPollMessage("q")) {
		t.Fatalf("poll message reported as content unavailable")
	}
	if !newsletterMessageContentAvailable(&waE2E.Message{ImageMessage: &waE2E.ImageMessage{}}) {
		t.Fatalf("image message reported as content unavailable")
	}
}

func TestNewsletterVoteLookupCount(t *testing.T) {
	if got := newsletterVoteLookupCount(0); got != defaultNewsletterVoteScan {
		t.Fatalf("default count = %d", got)
	}
	if got := newsletterVoteLookupCount(200); got != maxNewsletterMessageCount {
		t.Fatalf("max count = %d", got)
	}
	if got := newsletterVoteLookupCount(12); got != 12 {
		t.Fatalf("explicit count = %d", got)
	}
}

func TestNewsletterPollMessageInfoUsesNewsletterAsChatAndSender(t *testing.T) {
	jid := types.NewJID("120363123456789", types.NewsletterServer)
	msg := &types.NewsletterMessage{
		MessageServerID: 102,
		MessageID:       "3EB0POLL",
		Type:            "poll",
		Timestamp:       time.Unix(1719200100, 0),
	}

	got := newsletterPollMessageInfo(jid, msg)

	if got.Chat != jid {
		t.Fatalf("chat = %s", got.Chat)
	}
	if got.Sender != jid {
		t.Fatalf("sender = %s", got.Sender)
	}
	if got.ID != "3EB0POLL" || got.ServerID != 102 || got.Type != "poll" {
		t.Fatalf("info = %#v", got)
	}
}

func TestValidateNewsletterPollOptions(t *testing.T) {
	msg := &types.NewsletterMessage{Message: testNewsletterPollMessage("pick one")}

	if err := validateNewsletterPollOptions(msg, []string{"A"}); err != nil {
		t.Fatalf("validateNewsletterPollOptions returned error: %v", err)
	}
	if err := validateNewsletterPollOptions(msg, []string{"C"}); err == nil {
		t.Fatalf("expected unknown option to fail")
	}
}

func testNewsletterPollMessage(name string) *waE2E.Message {
	return &waE2E.Message{PollCreationMessage: &waE2E.PollCreationMessage{
		Name: proto.String(name),
		Options: []*waE2E.PollCreationMessage_Option{
			{OptionName: proto.String("A")},
			{OptionName: proto.String("B")},
		},
		SelectableOptionsCount: proto.Uint32(1),
	}}
}

// A V4 poll must yield its options like every other version.
//
// V4 is the odd one out in the protobuf: V1, V2, V3, V5 and V6 are all a
// *PollCreationMessage, while V4 is a *FutureProofMessage wrapping one. The
// switch recognised V4 and returned a nil payload, so a V4 poll came back with
// no name, no options and no option count.
//
// Channels in the wild publish V4 — image polls among them. An order placed
// against one was cancelled for "the resolved poll has no options" while the
// poll plainly had two.
func TestNewsletterMessagePollUnwrapsV4(t *testing.T) {
	inner := &waE2E.PollCreationMessage{
		Name: proto.String("Who wins?"),
		Options: []*waE2E.PollCreationMessage_Option{
			{OptionName: proto.String("A")},
			{OptionName: proto.String("B")},
		},
		SelectableOptionsCount: proto.Uint32(1),
	}
	message := &waE2E.Message{
		PollCreationMessageV4: &waE2E.FutureProofMessage{
			Message: &waE2E.Message{PollCreationMessage: inner},
		},
	}

	field, poll := newsletterMessagePoll(message)

	if field != "pollCreationMessageV4" {
		t.Fatalf("poll field = %q, want pollCreationMessageV4", field)
	}
	if poll == nil {
		t.Fatal("poll payload is nil: a V4 poll reads as having no options at all")
	}
	if poll.GetName() != "Who wins?" {
		t.Errorf("name = %q, want the wrapped poll's question", poll.GetName())
	}
	if len(poll.GetOptions()) != 2 {
		t.Fatalf("options = %d, want 2", len(poll.GetOptions()))
	}
	if poll.GetOptions()[0].GetOptionName() != "A" || poll.GetOptions()[1].GetOptionName() != "B" {
		t.Errorf("options = %q/%q, want A/B",
			poll.GetOptions()[0].GetOptionName(), poll.GetOptions()[1].GetOptionName())
	}
}

// An empty wrapper is not a poll with options, and must not be reported as one.
func TestNewsletterMessagePollV4WithoutInnerMessage(t *testing.T) {
	message := &waE2E.Message{PollCreationMessageV4: &waE2E.FutureProofMessage{}}

	field, poll := newsletterMessagePoll(message)

	if field != "pollCreationMessageV4" {
		t.Fatalf("poll field = %q", field)
	}
	if poll != nil {
		t.Error("an empty V4 wrapper reported a poll payload")
	}
}
