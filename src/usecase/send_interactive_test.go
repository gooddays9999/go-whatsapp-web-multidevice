package usecase

import (
	"strings"
	"testing"
)

// TestParseInteractiveMessageNativeFlowCtaURL uses the exact wire format a
// caller sends (body + native_flow cta_url button) and proves it decodes into a
// whatsmeow InteractiveMessage with the button intact.
func TestParseInteractiveMessageNativeFlowCtaURL(t *testing.T) {
	protoJSON := `{"body":{"text":"Novo SAMBA - GO"},"nativeFlowMessage":{"buttons":[{"buttonParamsJSON":"{\"merchant_url\":\"https://bit.ly/42RTX6q\",\"display_text\":\"GO\",\"url\":\"https://bit.ly/42RTX6q\"}","name":"cta_url"}]}}`

	msg, err := parseInteractiveMessage(protoJSON)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := msg.GetBody().GetText(); got != "Novo SAMBA - GO" {
		t.Fatalf("body text = %q", got)
	}
	nf := msg.GetNativeFlowMessage()
	if nf == nil {
		t.Fatal("nativeFlowMessage not decoded")
	}
	if len(nf.GetButtons()) != 1 {
		t.Fatalf("buttons = %d, want 1", len(nf.GetButtons()))
	}
	btn := nf.GetButtons()[0]
	if btn.GetName() != "cta_url" {
		t.Fatalf("button name = %q, want cta_url", btn.GetName())
	}
	if !strings.Contains(btn.GetButtonParamsJSON(), "https://bit.ly/42RTX6q") {
		t.Fatalf("button params missing url: %s", btn.GetButtonParamsJSON())
	}
}

// TestParseInteractiveMessageDiscardsEnvelope confirms unknown envelope fields
// (messageId, subType, type) don't break decoding.
func TestParseInteractiveMessageDiscardsEnvelope(t *testing.T) {
	protoJSON := `{"messageId":"3EB0","type":"interactive","subType":"native_flow","body":{"text":"hi"}}`
	msg, err := parseInteractiveMessage(protoJSON)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if msg.GetBody().GetText() != "hi" {
		t.Fatalf("body = %q", msg.GetBody().GetText())
	}
}

func TestParseInteractiveMessageErrors(t *testing.T) {
	if _, err := parseInteractiveMessage("   "); err == nil {
		t.Fatal("expected error for empty proto_json")
	}
	if _, err := parseInteractiveMessage("{not-json"); err == nil {
		t.Fatal("expected error for invalid json")
	}
}
