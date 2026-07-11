package bridge

import (
	"testing"
	"time"

	domainGroup "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/group"
	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
)

func TestGroupInfoFromLinkToProtoPreservesResolvedFields(t *testing.T) {
	createdAt := time.Date(2026, time.July, 11, 13, 24, 0, 0, time.UTC)
	got := groupInfoFromLinkToProto("https://chat.whatsapp.com/abc", domainGroup.GetGroupInfoFromLinkResponse{
		GroupID:          "120363407910125315@g.us",
		Name:             "Public Leads",
		Topic:            "Daily buyer requests",
		CreatedAt:        createdAt,
		ParticipantCount: 42,
		IsLocked:         true,
		IsAnnounce:       false,
		IsEphemeral:      true,
		Description:      "Daily buyer requests",
	})

	if !got.GetSuccess() {
		t.Fatal("success = false, want true")
	}
	if got.GetInviteLink() != "https://chat.whatsapp.com/abc" {
		t.Fatalf("invite link = %q", got.GetInviteLink())
	}
	if got.GetGroupId() != "120363407910125315@g.us" || got.GetGroupName() != "Public Leads" {
		t.Fatalf("group fields = %q/%q", got.GetGroupId(), got.GetGroupName())
	}
	if got.GetTopic() != "Daily buyer requests" || got.GetDescription() != "Daily buyer requests" {
		t.Fatalf("text fields = %q/%q", got.GetTopic(), got.GetDescription())
	}
	if got.GetCreatedAt() != createdAt.Unix() {
		t.Fatalf("created_at = %d, want %d", got.GetCreatedAt(), createdAt.Unix())
	}
	if got.GetParticipantCount() != 42 {
		t.Fatalf("participant_count = %d", got.GetParticipantCount())
	}
	if !got.GetIsLocked() || got.GetIsAnnounce() || !got.GetIsEphemeral() {
		t.Fatalf("flags = locked:%v announce:%v ephemeral:%v", got.GetIsLocked(), got.GetIsAnnounce(), got.GetIsEphemeral())
	}

	_ = &bridgepb.GetGroupInfoFromLinkRequest{AccountId: "360", InviteLink: "https://chat.whatsapp.com/abc"}
}

func TestGroupInfoFromLinkToProtoUsesZeroForUnknownCreatedAt(t *testing.T) {
	got := groupInfoFromLinkToProto("https://chat.whatsapp.com/abc", domainGroup.GetGroupInfoFromLinkResponse{})

	if got.GetCreatedAt() != 0 {
		t.Fatalf("created_at = %d, want 0 for unknown time", got.GetCreatedAt())
	}
}
