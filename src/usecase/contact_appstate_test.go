package usecase

import (
	"testing"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types"
)

func TestBuildAddContactPatchMatchesBaileysContactAppState(t *testing.T) {
	target := types.NewJID("17423756832", types.DefaultUserServer)

	patch := buildAddContactPatch(target, types.EmptyJID, "IMS", "IMS 17423756832", true)

	if patch.Type != appstate.WAPatchCriticalUnblockLow {
		t.Fatalf("patch.Type = %q, want %q", patch.Type, appstate.WAPatchCriticalUnblockLow)
	}
	if len(patch.Mutations) != 1 {
		t.Fatalf("len(patch.Mutations) = %d, want 1", len(patch.Mutations))
	}

	mutation := patch.Mutations[0]
	if mutation.Version != 2 {
		t.Fatalf("mutation.Version = %d, want 2", mutation.Version)
	}
	if len(mutation.Index) != 2 || mutation.Index[0] != appstate.IndexContact || mutation.Index[1] != "17423756832@s.whatsapp.net" {
		t.Fatalf("mutation.Index = %#v, want []string{%q, %q}", mutation.Index, appstate.IndexContact, "17423756832@s.whatsapp.net")
	}

	action := mutation.Value.GetContactAction()
	if action == nil {
		t.Fatal("expected contact action")
	}
	if action.GetFirstName() != "IMS" {
		t.Fatalf("FirstName = %q, want %q", action.GetFirstName(), "IMS")
	}
	if action.GetFullName() != "IMS 17423756832" {
		t.Fatalf("FullName = %q, want %q", action.GetFullName(), "IMS 17423756832")
	}
	if action.GetPnJID() != "17423756832@s.whatsapp.net" {
		t.Fatalf("PnJID = %q, want %q", action.GetPnJID(), "17423756832@s.whatsapp.net")
	}
	if !action.GetSaveOnPrimaryAddressbook() {
		t.Fatal("SaveOnPrimaryAddressbook = false, want true")
	}
}
