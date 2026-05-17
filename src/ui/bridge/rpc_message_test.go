package bridge

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/png"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func TestBuildStatusMessageRejectsEmptyText(t *testing.T) {
	_, _, err := buildStatusMessage(t.Context(), nil, &bridgepb.SendStatusRequest{})
	if err == nil {
		t.Fatal("buildStatusMessage() error = nil, want error")
	}
}

func TestIsDisconnectedSendError(t *testing.T) {
	if !isDisconnectedSendError(errors.New("websocket disconnected before message send returned response")) {
		t.Fatal("websocket disconnected error should trigger reconnect")
	}
	if isDisconnectedSendError(errors.New("rate limited by WhatsApp")) {
		t.Fatal("non-connection error should not trigger reconnect")
	}
}

func TestShouldRecycleStatusClient(t *testing.T) {
	if !shouldRecycleStatusClient("ensureStatusRecipients", context.DeadlineExceeded) {
		t.Fatal("recipient timeout should recycle status client")
	}
	if !shouldRecycleStatusClient("SendMessage", context.Canceled) {
		t.Fatal("send cancellation should recycle status client")
	}
	if shouldRecycleStatusClient("queue", context.DeadlineExceeded) {
		t.Fatal("queue timeout should not recycle status client")
	}
	if shouldRecycleStatusClient("buildMessage", errors.New("status content or media_url is required")) {
		t.Fatal("validation error should not recycle status client")
	}
	if !shouldRecycleAccountClient(context.DeadlineExceeded) {
		t.Fatal("account operation deadline should recycle account client")
	}
	if shouldRecycleAccountClient(errors.New("rate limited by WhatsApp")) {
		t.Fatal("business error should not recycle account client")
	}
}

func TestBuildStatusMessageUsesExtendedText(t *testing.T) {
	msg, hasMedia, err := buildStatusMessage(t.Context(), nil, &bridgepb.SendStatusRequest{
		Content: "hello status",
		Color:   "#123456",
	})
	if err != nil {
		t.Fatalf("buildStatusMessage() error = %v", err)
	}
	if hasMedia {
		t.Fatal("hasMedia = true, want false")
	}
	if msg.GetExtendedTextMessage().GetText() != "hello status" {
		t.Fatalf("text = %q, want hello status", msg.GetExtendedTextMessage().GetText())
	}
	if got := msg.GetExtendedTextMessage().GetBackgroundArgb(); got != 0xFF123456 {
		t.Fatalf("background argb = %#x, want %#x", got, uint32(0xFF123456))
	}
	if got := msg.GetExtendedTextMessage().GetContextInfo().GetStatusSourceType(); got != waE2E.ContextInfo_TEXT {
		t.Fatalf("status source type = %s, want %s", got, waE2E.ContextInfo_TEXT)
	}
}

func TestStatusContextInfoUsesSourceType(t *testing.T) {
	tests := []waE2E.ContextInfo_StatusSourceType{
		waE2E.ContextInfo_TEXT,
		waE2E.ContextInfo_IMAGE,
		waE2E.ContextInfo_VIDEO,
		waE2E.ContextInfo_GIF,
	}
	for _, tt := range tests {
		t.Run(tt.String(), func(t *testing.T) {
			if got := statusContextInfo(tt).GetStatusSourceType(); got != tt {
				t.Fatalf("status source type = %s, want %s", got, tt)
			}
		})
	}
}

func TestStatusMessageKind(t *testing.T) {
	if got := statusMessageKind(&waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{}}); got != "text" {
		t.Fatalf("kind = %q, want text", got)
	}
	if got := statusMessageKind(&waE2E.Message{ImageMessage: &waE2E.ImageMessage{}}); got != "image" {
		t.Fatalf("kind = %q, want image", got)
	}
	if got := statusMessageKind(&waE2E.Message{VideoMessage: &waE2E.VideoMessage{}}); got != "video" {
		t.Fatalf("kind = %q, want video", got)
	}
	if got := statusMessageKind(&waE2E.Message{VideoMessage: &waE2E.VideoMessage{GifPlayback: proto.Bool(true)}}); got != "gif" {
		t.Fatalf("kind = %q, want gif", got)
	}
}

func TestNormalizeStatusMessageIDAcceptsSerializedWebID(t *testing.T) {
	got := normalizeStatusMessageID("false_status@broadcast_3EB0FFA6360E3CCA77AF70_16723028367@c.us")
	if got != "3EB0FFA6360E3CCA77AF70" {
		t.Fatalf("id = %q, want short status id", got)
	}
}

func TestBuildStatusReplyMessageQuotesStatusBroadcast(t *testing.T) {
	targetJID := types.NewJID("16723028367", types.DefaultUserServer)
	target := &statusReplyTarget{
		StatusMessageID: "3EB0FFA6360E3CCA77AF70",
		TargetJID:       targetJID,
		QuotedMessage:   &waE2E.Message{Conversation: proto.String("hello")},
	}
	msg := buildStatusReplyMessage(target, "👍")
	ctxInfo := msg.GetExtendedTextMessage().GetContextInfo()
	if ctxInfo.GetRemoteJID() != types.StatusBroadcastJID.String() {
		t.Fatalf("remote jid = %q, want %q", ctxInfo.GetRemoteJID(), types.StatusBroadcastJID.String())
	}
	if ctxInfo.GetParticipant() != targetJID.String() {
		t.Fatalf("participant = %q, want %q", ctxInfo.GetParticipant(), targetJID.String())
	}
	if ctxInfo.GetStanzaID() != target.StatusMessageID {
		t.Fatalf("stanza id = %q, want %q", ctxInfo.GetStanzaID(), target.StatusMessageID)
	}
}

func TestStatusReplyTargetFromMessageMatchesIMSFields(t *testing.T) {
	target, err := statusReplyTargetFromMessage(&domainChatStorage.Message{
		ID:      "3EB0FFA6360E3CCA77AF70",
		ChatJID: types.StatusBroadcastJID.String(),
		Sender:  "16723028367@s.whatsapp.net",
		Content: "hello",
	}, "userId")
	if err != nil {
		t.Fatalf("statusReplyTargetFromMessage() error = %v", err)
	}
	if target.Source != "userId" {
		t.Fatalf("source = %q, want userId", target.Source)
	}
	if got := imsTargetUserID(target.TargetJID); got != "16723028367@c.us" {
		t.Fatalf("target user = %q, want 16723028367@c.us", got)
	}
	if target.QuotedMessage.GetConversation() != "hello" {
		t.Fatalf("quoted text = %q, want hello", target.QuotedMessage.GetConversation())
	}
}

func TestLooksLikeLegacyReactionRecipient(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "bare phone", raw: "15812781311", want: true},
		{name: "formatted phone", raw: "+1 (581) 278-1311", want: true},
		{name: "legacy jid", raw: "15812781311@c.us", want: true},
		{name: "whatsmeow jid", raw: "15812781311@s.whatsapp.net", want: true},
		{name: "group jid", raw: "120363123456789@g.us", want: true},
		{name: "short hex message id", raw: "3EB08396C9C4B98BC66C45", want: false},
		{name: "serialized web message id", raw: "false_15812781311@c.us_3EB08396C9C4B98BC66C45", want: false},
		{name: "empty", raw: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := looksLikeLegacyReactionRecipient(tt.raw); got != tt.want {
				t.Fatalf("looksLikeLegacyReactionRecipient(%q) = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}

func TestFindLatestIncomingReactionTargetUsesLatestIncomingForDevice(t *testing.T) {
	now := time.Now()
	repo := &reactionTargetTestStore{messages: []*domainChatStorage.Message{
		{
			ID:        "old-incoming",
			DeviceID:  "16723028367@s.whatsapp.net",
			ChatJID:   "15812781311@s.whatsapp.net",
			Sender:    "15812781311@s.whatsapp.net",
			Timestamp: now.Add(-3 * time.Minute),
			IsFromMe:  false,
		},
		{
			ID:        "newer-from-me",
			DeviceID:  "16723028367@s.whatsapp.net",
			ChatJID:   "15812781311@s.whatsapp.net",
			Sender:    "16723028367@s.whatsapp.net",
			Timestamp: now.Add(-2 * time.Minute),
			IsFromMe:  true,
		},
		{
			ID:        "wanted-incoming",
			DeviceID:  "16723028367@s.whatsapp.net",
			ChatJID:   "15812781311@s.whatsapp.net",
			Sender:    "15812781311@s.whatsapp.net",
			Timestamp: now.Add(-1 * time.Minute),
			IsFromMe:  false,
		},
		{
			ID:        "other-device-incoming",
			DeviceID:  "15812781311@s.whatsapp.net",
			ChatJID:   "15812781311@s.whatsapp.net",
			Sender:    "15812781311@s.whatsapp.net",
			Timestamp: now,
			IsFromMe:  false,
		},
	}}

	msg, err := findLatestIncomingReactionTarget(repo, "16723028367@s.whatsapp.net", "15812781311@s.whatsapp.net", "15812781311")
	if err != nil {
		t.Fatalf("findLatestIncomingReactionTarget() error = %v", err)
	}
	if msg.ID != "wanted-incoming" {
		t.Fatalf("message id = %q, want wanted-incoming", msg.ID)
	}
}

func TestParseLegacyReactionRecipientJID(t *testing.T) {
	tests := []struct {
		raw  string
		want string
	}{
		{raw: "15812781311", want: "15812781311@s.whatsapp.net"},
		{raw: "+1 (581) 278-1311", want: "15812781311@s.whatsapp.net"},
		{raw: "15812781311@c.us", want: "15812781311@s.whatsapp.net"},
		{raw: "15812781311@s.whatsapp.net", want: "15812781311@s.whatsapp.net"},
	}

	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			jid, err := parseLegacyReactionRecipientJID(tt.raw)
			if err != nil {
				t.Fatalf("parseLegacyReactionRecipientJID() error = %v", err)
			}
			if got := jid.String(); got != tt.want {
				t.Fatalf("jid = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizeStatusMediaMIMEFallsBackToURLWithoutDot(t *testing.T) {
	got := normalizeStatusMediaMIME("application/octet-stream", "http://example.test/media/20260511/1D2455QYD(163)mp4", []byte("not enough to sniff"))
	if got != "video/mp4" {
		t.Fatalf("mime = %q, want video/mp4", got)
	}
}

type reactionTargetTestStore struct {
	messages []*domainChatStorage.Message
}

func (r *reactionTargetTestStore) GetMessageByID(id string) (*domainChatStorage.Message, error) {
	for _, msg := range r.messages {
		if msg != nil && msg.ID == id {
			return msg, nil
		}
	}
	return nil, nil
}

func (r *reactionTargetTestStore) GetMessages(filter *domainChatStorage.MessageFilter) ([]*domainChatStorage.Message, error) {
	var out []*domainChatStorage.Message
	for _, msg := range r.messages {
		if msg == nil {
			continue
		}
		if filter.DeviceID != "" && msg.DeviceID != filter.DeviceID {
			continue
		}
		if filter.ChatJID != "" && msg.ChatJID != filter.ChatJID {
			continue
		}
		if filter.IsFromMe != nil && msg.IsFromMe != *filter.IsFromMe {
			continue
		}
		out = append(out, msg)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Timestamp.After(out[j].Timestamp)
	})
	if filter.Limit > 0 && len(out) > filter.Limit {
		out = out[:filter.Limit]
	}
	return out, nil
}

func TestDownloadStatusMediaAcceptsInferredVideoType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte("not enough to sniff but url contains mp4"))
	}))
	defer server.Close()

	_, mimeType, err := downloadStatusMedia(server.URL + "/media/file(1)mp4")
	if err != nil {
		t.Fatalf("downloadStatusMedia() error = %v", err)
	}
	if mimeType != "video/mp4" {
		t.Fatalf("mime = %q, want video/mp4", mimeType)
	}
}

func TestPrepareStatusImageBuildsThumbnail(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 640, 480))
	var input bytes.Buffer
	if err := png.Encode(&input, src); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	data, mimeType, thumb, width, height, err := prepareStatusImage(input.Bytes(), "image/png")
	if err != nil {
		t.Fatalf("prepareStatusImage() error = %v", err)
	}
	if len(data) == 0 || len(thumb) == 0 {
		t.Fatal("prepareStatusImage() returned empty image or thumbnail")
	}
	if mimeType != "image/png" {
		t.Fatalf("mime = %q, want image/png", mimeType)
	}
	if width != 640 || height != 480 {
		t.Fatalf("dimensions = %dx%d, want 640x480", width, height)
	}
}
