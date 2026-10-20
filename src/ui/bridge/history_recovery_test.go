package bridge

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

func validRecoveryRequest() historyRecoveryRequest {
	now := time.Now().UTC().Truncate(time.Second)
	return historyRecoveryRequest{AccountID: "623", ChatJID: "15551234567@s.whatsapp.net", AnchorID: "real-id", AnchorAt: now, Since: now.Add(-time.Hour), Until: now, Count: 50}
}

func TestHistoryRecoveryValidation(t *testing.T) {
	r := validRecoveryRequest()
	if err := r.validate(time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*historyRecoveryRequest){
		func(r *historyRecoveryRequest) { r.ChatJID = "123@g.us" },
		func(r *historyRecoveryRequest) { r.ChatJID = "123@lid" },
		func(r *historyRecoveryRequest) { r.AnchorID = "" },
		func(r *historyRecoveryRequest) { r.Count = 101 },
		func(r *historyRecoveryRequest) { r.LegacyDeviceID = "123@g.us" },
		func(r *historyRecoveryRequest) { r.Since = r.Until.Add(-25 * time.Hour) },
		func(r *historyRecoveryRequest) { r.Until = time.Now().Add(time.Hour) },
	} {
		r := validRecoveryRequest()
		mutate(&r)
		if err := r.validate(time.Now()); err == nil {
			t.Fatalf("accepted invalid recovery: %+v", r)
		}
	}
}

func TestHistoryRecoveryRejectsRemoteAccess(t *testing.T) {
	s := &Service{}
	r := httptest.NewRequest(http.MethodPost, "/internal/history-sync", nil)
	r.RemoteAddr = "198.51.100.1:1234"
	r.Header.Set("X-Forwarded-For", "127.0.0.1")
	w := httptest.NewRecorder()
	s.handleHistoryRecovery(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("status = %d", w.Code)
	}
}

func TestHistoryRecoveryFiltersBeforePublishing(t *testing.T) {
	r := validRecoveryRequest()
	j := &historyRecovery{request: r, seen: make(map[string]bool)}
	s := &Service{}
	for _, mutate := range []func(map[string]any){
		func(m map[string]any) { m["isFromMe"] = true },
		func(m map[string]any) { m["isGroup"] = true },
		func(m map[string]any) { m["chatId"] = "other@s.whatsapp.net" },
		func(m map[string]any) { m["timestamp"] = r.Since.Add(-time.Second).UnixMilli() },
		func(m map[string]any) { m["timestamp"] = r.Until.UnixMilli() },
		func(m map[string]any) { m["content"] = map[string]any{"text": " "} },
	} {
		m := map[string]any{"id": "reply-id", "chatId": r.ChatJID, "timestamp": r.Since.Add(time.Minute).UnixMilli(), "isFromMe": false, "isGroup": false, "content": map[string]any{"text": "real reply"}}
		mutate(m)
		if err := s.publishRecoveredReply(context.Background(), j, m); err != nil {
			t.Fatal(err)
		}
	}
	if j.published != 0 {
		t.Fatal("invalid replies published")
	}
}

func TestHistoryRecoveryNilEventDoesNotPanic(t *testing.T) {
	(&Service{}).handleHistorySyncEvent(context.Background(), "623", nil, nil)
}

func TestHistoryRecoveryCacheFilterUsesUTC(t *testing.T) {
	r := validRecoveryRequest()
	r.Since = r.Since.In(time.FixedZone("CST", 8*60*60))
	r.Until = r.Until.In(time.FixedZone("CST", 8*60*60))
	f := historyRecoveryFilter(r, "legacy@s.whatsapp.net")
	if f.StartTime.Location() != time.UTC || f.EndTime.Location() != time.UTC || !f.StartTime.Equal(r.Since) || f.DeviceID != "legacy@s.whatsapp.net" {
		t.Fatalf("invalid UTC/account filter: %+v", f)
	}
}

type recoverySecretStore struct {
	store.NoopStore
}

func (*recoverySecretStore) GetMessageSecret(_ context.Context, chat, sender types.JID, id types.MessageID) ([]byte, types.JID, error) {
	if chat.String() == "15551234567@s.whatsapp.net" && chat == sender && id == "verified-id" {
		return []byte("test-only-key"), sender, nil
	}
	return nil, types.EmptyJID, nil
}

func TestHistoryRecoveryLegacyOwnershipRequiresAccountMessageStore(t *testing.T) {
	own := types.NewJID("15550000001", types.DefaultUserServer)
	client := &whatsmeow.Client{Store: &store.Device{ID: &own, MsgSecrets: &recoverySecretStore{}}}
	for _, tc := range []struct {
		chat string
		id   string
		want bool
	}{
		{"15551234567@s.whatsapp.net", "verified-id", true},
		{"15551234567@s.whatsapp.net", "unverified-id", false},
		{"15551234568@s.whatsapp.net", "verified-id", false},
	} {
		got, err := verifyLegacyReplyOwner(context.Background(), client, tc.chat, tc.id)
		if err != nil || got != tc.want {
			t.Fatalf("owner verification = %t, %v; want %t", got, err, tc.want)
		}
	}
}
