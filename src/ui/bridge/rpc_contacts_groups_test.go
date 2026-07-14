package bridge

import (
	"bytes"
	"context"
	"image"
	"image/png"
	"testing"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types"
)

func TestLegacyContactChatID(t *testing.T) {
	tests := []struct {
		name  string
		phone string
		want  string
	}{
		{name: "bare number", phone: "15812751827", want: "15812751827@c.us"},
		{name: "formatted number", phone: "+1 (581) 275-1827", want: "15812751827@c.us"},
		{name: "whatsmeow jid", phone: "15812751827@s.whatsapp.net", want: "15812751827@c.us"},
		{name: "legacy jid", phone: "15812751827@c.us", want: "15812751827@c.us"},
		{name: "group jid unchanged", phone: "120363123456789@g.us", want: "120363123456789@g.us"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := legacyContactChatID(tt.phone); got != tt.want {
				t.Fatalf("legacyContactChatID(%q) = %q, want %q", tt.phone, got, tt.want)
			}
		})
	}
}

func TestLegacyContactNumber(t *testing.T) {
	tests := []struct {
		name  string
		phone string
		want  string
	}{
		{name: "bare number", phone: "15812751827", want: "15812751827"},
		{name: "formatted number", phone: "+1 (581) 275-1827", want: "15812751827"},
		{name: "whatsmeow jid", phone: "15812751827@s.whatsapp.net", want: "15812751827"},
		{name: "legacy jid", phone: "15812751827@c.us", want: "15812751827"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := legacyContactNumber(tt.phone); got != tt.want {
				t.Fatalf("legacyContactNumber(%q) = %q, want %q", tt.phone, got, tt.want)
			}
		})
	}
}

func TestAddContactNames(t *testing.T) {
	tests := []struct {
		name          string
		number        string
		firstName     string
		lastName      string
		wantFirstName string
		wantFullName  string
	}{
		{name: "provided first and last", number: "15812751827", firstName: "Garrett", lastName: "Allen", wantFirstName: "Garrett", wantFullName: "Garrett Allen"},
		{name: "provided first only", number: "15812751827", firstName: "Garrett", wantFirstName: "Garrett", wantFullName: "Garrett"},
		{name: "dot last name ignored", number: "15812751827", firstName: "Garrett", lastName: ".", wantFirstName: "Garrett", wantFullName: "Garrett"},
		{name: "fallback from phone suffix", number: "15812751827", wantFirstName: "1827", wantFullName: "1827"},
		{name: "short number fallback", number: "123", wantFirstName: "123", wantFullName: "123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFirstName, gotFullName := addContactNames(tt.number, tt.firstName, tt.lastName)
			if gotFirstName != tt.wantFirstName || gotFullName != tt.wantFullName {
				t.Fatalf("addContactNames() = %q/%q, want %q/%q", gotFirstName, gotFullName, tt.wantFirstName, tt.wantFullName)
			}
		})
	}
}

func TestBuildAddContactPatch(t *testing.T) {
	jid := types.NewJID("15812751827", types.DefaultUserServer)
	patch := buildAddContactPatch(jid, types.EmptyJID, "Garrett", "Garrett Allen")

	if patch.Type != appstate.WAPatchCriticalUnblockLow {
		t.Fatalf("patch type = %q, want %q", patch.Type, appstate.WAPatchCriticalUnblockLow)
	}
	if len(patch.Mutations) != 1 {
		t.Fatalf("mutation count = %d, want 1", len(patch.Mutations))
	}
	mutation := patch.Mutations[0]
	if mutation.Version != 2 {
		t.Fatalf("mutation version = %d, want 2", mutation.Version)
	}
	if len(mutation.Index) != 2 || mutation.Index[0] != appstate.IndexContact || mutation.Index[1] != jid.String() {
		t.Fatalf("mutation index = %#v, want contact/%s", mutation.Index, jid.String())
	}
	action := mutation.Value.GetContactAction()
	if action == nil {
		t.Fatal("contact action is nil")
	}
	if action.GetFirstName() != "Garrett" || action.GetFullName() != "Garrett Allen" {
		t.Fatalf("contact action names = %q/%q", action.GetFirstName(), action.GetFullName())
	}
	if action.GetPnJID() != jid.String() {
		t.Fatalf("contact action pn jid = %q, want %q", action.GetPnJID(), jid.String())
	}
	if !action.GetSaveOnPrimaryAddressbook() {
		t.Fatal("saveOnPrimaryAddressbook = false, want true")
	}
	if action.GetLidJID() != "" {
		t.Fatalf("contact action lid jid = %q, want empty", action.GetLidJID())
	}
}

func TestBuildAddContactPatchWithLID(t *testing.T) {
	jid := types.NewJID("15812751827", types.DefaultUserServer)
	lidJID := types.NewJID("1234567890", types.HiddenUserServer)
	patch := buildAddContactPatch(jid, lidJID, "Garrett", "Garrett Allen")

	if len(patch.Mutations) != 2 {
		t.Fatalf("mutation count = %d, want 2", len(patch.Mutations))
	}
	contactAction := patch.Mutations[0].Value.GetContactAction()
	if contactAction == nil {
		t.Fatal("contact action is nil")
	}
	if contactAction.GetLidJID() != lidJID.String() {
		t.Fatalf("contact action lid jid = %q, want %q", contactAction.GetLidJID(), lidJID.String())
	}
	lidMutation := patch.Mutations[1]
	if lidMutation.Version != 2 {
		t.Fatalf("lid mutation version = %d, want 2", lidMutation.Version)
	}
	if len(lidMutation.Index) != 2 || lidMutation.Index[0] != appstate.IndexLIDContact || lidMutation.Index[1] != lidJID.String() {
		t.Fatalf("lid mutation index = %#v, want lid_contact/%s", lidMutation.Index, lidJID.String())
	}
	lidAction := lidMutation.Value.GetLidContactAction()
	if lidAction == nil {
		t.Fatal("lid contact action is nil")
	}
	if lidAction.GetFirstName() != "Garrett" || lidAction.GetFullName() != "Garrett Allen" {
		t.Fatalf("lid contact action names = %q/%q", lidAction.GetFirstName(), lidAction.GetFullName())
	}
}

func TestFindJoinedGroupNameByJID(t *testing.T) {
	groups := []types.GroupInfo{
		{
			JID:       types.NewJID("120363111", types.GroupServer),
			GroupName: types.GroupName{Name: "Other Group"},
		},
		{
			JID:       types.NewJID("120363222", types.GroupServer),
			GroupName: types.GroupName{Name: "The M Team"},
		},
	}

	got := findJoinedGroupNameByJID(groups, "120363222@g.us")
	if got != "The M Team" {
		t.Fatalf("findJoinedGroupNameByJID() = %q, want %q", got, "The M Team")
	}
}

func TestBridgeGroupFromInfoIncludesResolvedAvatarURL(t *testing.T) {
	group := types.GroupInfo{
		JID:              types.NewJID("120363222", types.GroupServer),
		OwnerJID:         types.NewJID("628111", types.DefaultUserServer),
		GroupName:        types.GroupName{Name: "The M Team"},
		GroupTopic:       types.GroupTopic{Topic: "Hello"},
		ParticipantCount: 42,
	}
	got := bridgeGroupFromInfo(context.Background(), group, func(_ context.Context, jid types.JID) string {
		if jid != group.JID {
			t.Fatalf("resolver jid = %s, want %s", jid, group.JID)
		}
		return "https://pps.whatsapp.net/v/t61.24694-24/group.jpg?oh=token"
	})

	if got.GetAvatar() != "https://pps.whatsapp.net/v/t61.24694-24/group.jpg?oh=token" {
		t.Fatalf("avatar = %q, want resolved URL", got.GetAvatar())
	}
	if got.GetJid() != group.JID.String() || got.GetName() != "The M Team" || got.GetDescription() != "Hello" {
		t.Fatalf("group fields not preserved: %#v", got)
	}
}

func TestPrepareProfilePictureJPEG(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 1200, 800))
	var input bytes.Buffer
	if err := png.Encode(&input, src); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	output, err := prepareProfilePictureJPEG(input.Bytes())
	if err != nil {
		t.Fatalf("prepareProfilePictureJPEG() error = %v", err)
	}
	if len(output) == 0 {
		t.Fatal("prepareProfilePictureJPEG() returned empty output")
	}
	if len(output) > profilePictureMaxBytes {
		t.Fatalf("output size = %d, want <= %d", len(output), profilePictureMaxBytes)
	}

	got, format, err := image.Decode(bytes.NewReader(output))
	if err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if format != "jpeg" {
		t.Fatalf("format = %q, want jpeg", format)
	}
	bounds := got.Bounds()
	if bounds.Dx() != bounds.Dy() {
		t.Fatalf("output dimensions = %dx%d, want square", bounds.Dx(), bounds.Dy())
	}
	if bounds.Dx() > profilePictureMaxDimension {
		t.Fatalf("output dimension = %d, want <= %d", bounds.Dx(), profilePictureMaxDimension)
	}
}

func TestPrepareProfilePictureJPEGRejectsInvalidImage(t *testing.T) {
	if _, err := prepareProfilePictureJPEG([]byte("not an image")); err == nil {
		t.Fatal("prepareProfilePictureJPEG() error = nil, want error")
	}
}

func TestShouldApplyUpdateGroupFieldAllowsExplicitEmptyValue(t *testing.T) {
	if !shouldApplyUpdateGroupField(true, "") {
		t.Fatal("explicit empty field should be applied")
	}
	if shouldApplyUpdateGroupField(false, "") {
		t.Fatal("implicit empty field should not be applied")
	}
	if !shouldApplyUpdateGroupField(false, "WA2") {
		t.Fatal("non-empty field should be applied for backward compatibility")
	}
}
