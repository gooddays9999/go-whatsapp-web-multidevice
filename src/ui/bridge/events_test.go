package bridge

import (
	"testing"

	"go.mau.fi/whatsmeow/types/events"
)

func TestOutgoingSentStatusEventMatchesIMSAckShape(t *testing.T) {
	evt := &events.Message{}
	evt.Info.ID = "3EB0FROMDEVICE"
	message := map[string]any{
		"chatId": "15812751827@s.whatsapp.net",
		"from":   "16723028367@s.whatsapp.net",
		"to":     "15812751827@s.whatsapp.net",
	}

	got := outgoingSentStatusEvent(message, evt)

	if got["messageId"] != "3EB0FROMDEVICE" {
		t.Fatalf("messageId = %v, want 3EB0FROMDEVICE", got["messageId"])
	}
	if got["status"] != "sent" {
		t.Fatalf("status = %v, want sent", got["status"])
	}
	if got["fromMe"] != true {
		t.Fatalf("fromMe = %v, want true", got["fromMe"])
	}
	if got["chatId"] != "15812751827@s.whatsapp.net" {
		t.Fatalf("chatId = %v, want chat jid", got["chatId"])
	}
	if got["from"] != "16723028367@s.whatsapp.net" {
		t.Fatalf("from = %v, want sender jid", got["from"])
	}
	if got["to"] != "15812751827@s.whatsapp.net" {
		t.Fatalf("to = %v, want recipient jid", got["to"])
	}
}
