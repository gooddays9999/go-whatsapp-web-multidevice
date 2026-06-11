package bridge

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
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

func TestHistorySyncStatusEventsPublishesOutgoingDeliveredAndRead(t *testing.T) {
	syncType := waHistorySync.HistorySync_RECENT
	chatJID := "15812751827@s.whatsapp.net"
	deliveredAt := uint64(time.Date(2026, time.June, 11, 3, 23, 0, 0, time.UTC).Unix())
	readAt := uint64(time.Date(2026, time.June, 11, 3, 24, 0, 0, time.UTC).Unix())
	deliveredStatus := waWeb.WebMessageInfo_DELIVERY_ACK
	readStatus := waWeb.WebMessageInfo_READ
	serverAckStatus := waWeb.WebMessageInfo_SERVER_ACK
	data := &waHistorySync.HistorySync{
		SyncType: &syncType,
		Conversations: []*waHistorySync.Conversation{{
			ID: proto.String(chatJID),
			Messages: []*waHistorySync.HistorySyncMsg{
				historyStatusMessage("delivered-msg", chatJID, true, deliveredStatus, deliveredAt),
				historyStatusMessage("read-msg", chatJID, true, readStatus, readAt),
				historyStatusMessage("sent-only-msg", chatJID, true, serverAckStatus, deliveredAt),
				historyStatusMessage("incoming-delivered-msg", chatJID, false, deliveredStatus, deliveredAt),
			},
		}},
	}

	events := historySyncStatusEvents(context.Background(), nil, data)

	if len(events) != 2 {
		t.Fatalf("events len = %d, want 2: %#v", len(events), events)
	}
	if events[0]["messageId"] != "delivered-msg" || events[0]["status"] != "delivered" {
		t.Fatalf("first event = %#v, want delivered-msg delivered", events[0])
	}
	if events[1]["messageId"] != "read-msg" || events[1]["status"] != "read" {
		t.Fatalf("second event = %#v, want read-msg read", events[1])
	}
	for _, event := range events {
		if event["fromMe"] != true {
			t.Fatalf("fromMe = %v, want true in %#v", event["fromMe"], event)
		}
		if event["chatId"] != chatJID {
			t.Fatalf("chatId = %v, want %s", event["chatId"], chatJID)
		}
		if _, ok := event["timestamp"].(int64); !ok {
			t.Fatalf("timestamp type = %T, want int64 in %#v", event["timestamp"], event)
		}
	}
}

func TestHistorySyncStatusEventsUsesReceiptTimestamp(t *testing.T) {
	syncType := waHistorySync.HistorySync_RECENT
	chatJID := "15812751827@s.whatsapp.net"
	messageAt := uint64(time.Date(2026, time.June, 11, 3, 23, 0, 0, time.UTC).Unix())
	deliveredAt := time.Date(2026, time.June, 11, 3, 23, 7, 0, time.UTC).Unix()
	status := waWeb.WebMessageInfo_DELIVERY_ACK
	msg := historyStatusMessage("delivered-msg", chatJID, true, status, messageAt)
	msg.Message.UserReceipt = []*waWeb.UserReceipt{{
		UserJID:          proto.String(chatJID),
		ReceiptTimestamp: proto.Int64(deliveredAt),
	}}
	data := &waHistorySync.HistorySync{
		SyncType: &syncType,
		Conversations: []*waHistorySync.Conversation{{
			ID:       proto.String(chatJID),
			Messages: []*waHistorySync.HistorySyncMsg{msg},
		}},
	}

	events := historySyncStatusEvents(context.Background(), nil, data)

	if len(events) != 1 {
		t.Fatalf("events len = %d, want 1: %#v", len(events), events)
	}
	if events[0]["timestamp"] != deliveredAt*1000 {
		t.Fatalf("timestamp = %v, want receipt timestamp %d", events[0]["timestamp"], deliveredAt*1000)
	}
}

func TestWebMessageStatusEventUsesSourceWebMsgStatus(t *testing.T) {
	chatJID := "15812751827@s.whatsapp.net"
	readAt := time.Date(2026, time.June, 11, 3, 24, 7, 0, time.UTC).Unix()
	status := waWeb.WebMessageInfo_READ
	msg := historyStatusMessage("read-msg", chatJID, true, status, uint64(readAt-10)).Message
	msg.UserReceipt = []*waWeb.UserReceipt{{
		UserJID:         proto.String(chatJID),
		ReadTimestamp:   proto.Int64(readAt),
		PlayedTimestamp: proto.Int64(0),
	}}

	event, ok := webMessageStatusEvent(context.Background(), nil, msg)

	if !ok {
		t.Fatal("webMessageStatusEvent ok = false, want true")
	}
	if event["messageId"] != "read-msg" || event["status"] != "read" {
		t.Fatalf("event = %#v, want read message status", event)
	}
	if event["fromMe"] != true {
		t.Fatalf("fromMe = %v, want true", event["fromMe"])
	}
	if event["chatId"] != chatJID {
		t.Fatalf("chatId = %v, want %s", event["chatId"], chatJID)
	}
	if event["timestamp"] != readAt*1000 {
		t.Fatalf("timestamp = %v, want %d", event["timestamp"], readAt*1000)
	}
}

func historyStatusMessage(id, chatJID string, fromMe bool, status waWeb.WebMessageInfo_Status, ts uint64) *waHistorySync.HistorySyncMsg {
	return &waHistorySync.HistorySyncMsg{
		Message: &waWeb.WebMessageInfo{
			Key: &waCommon.MessageKey{
				RemoteJID: proto.String(chatJID),
				FromMe:    proto.Bool(fromMe),
				ID:        proto.String(id),
			},
			Message: &waE2E.Message{
				Conversation: proto.String("hello"),
			},
			MessageTimestamp: &ts,
			Status:           &status,
		},
	}
}
