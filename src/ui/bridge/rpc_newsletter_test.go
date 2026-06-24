package bridge

import (
	"testing"

	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	"go.mau.fi/whatsmeow/types"
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
	_ = &bridgepb.SendNewsletterPollRequest{
		AccountId:    "357",
		NewsletterId: "120363123456789@newsletter",
		Question:     "Pick one",
		Options:      []string{"A", "B"},
		MaxAnswer:    1,
	}
}
