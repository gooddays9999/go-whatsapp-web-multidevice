package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/sirupsen/logrus"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/types"
)

type historyRecoveryRequest struct {
	AccountID      string    `json:"account_id"`
	ChatJID        string    `json:"chat_jid"`
	AnchorID       string    `json:"anchor_id"`
	AnchorAt       time.Time `json:"anchor_at"`
	FromMe         bool      `json:"anchor_from_me"`
	Since          time.Time `json:"since"`
	Until          time.Time `json:"until"`
	Count          int       `json:"count"`
	CacheOnly      bool      `json:"cache_only"`
	LegacyDeviceID string    `json:"legacy_device_id,omitempty"`
	MessageIDs     []string  `json:"message_ids,omitempty"`
}

type historyRecovery struct {
	mu         sync.Mutex
	request    historyRecoveryRequest
	expiresAt  time.Time
	seen       map[string]bool
	published  int
	responses  int
	unverified int
}

func (r historyRecoveryRequest) validate(now time.Time) error {
	jid, err := types.ParseJID(r.ChatJID)
	if r.AccountID == "" || err != nil || jid.Server != types.DefaultUserServer || jid.User == "" || jid.Device != 0 {
		return fmt.Errorf("account_id and a private phone chat_jid are required")
	}
	if r.LegacyDeviceID != "" {
		legacy, err := types.ParseJID(r.LegacyDeviceID)
		if err != nil || legacy.Server != types.DefaultUserServer || legacy.User == "" || legacy.Device != 0 {
			return fmt.Errorf("legacy_device_id must be a phone JID")
		}
	}
	if r.Since.IsZero() || !r.Until.After(r.Since) || r.Until.Sub(r.Since) > 24*time.Hour || r.Until.After(now.Add(time.Minute)) {
		return fmt.Errorf("since/until must specify a past window of at most 24 hours")
	}
	if r.Count < 1 || r.Count > 100 {
		return fmt.Errorf("count must be between 1 and 100")
	}
	if len(r.MessageIDs) > 100 {
		return fmt.Errorf("at most 100 message_ids are allowed")
	}
	for _, id := range r.MessageIDs {
		if strings.TrimSpace(id) == "" || len(id) > 128 || strings.TrimSpace(id) != id {
			return fmt.Errorf("message_ids must contain non-empty original IDs of at most 128 bytes")
		}
	}
	if !r.CacheOnly && (r.AnchorID == "" || r.AnchorAt.IsZero() || r.AnchorAt.Before(r.Since) || r.AnchorAt.After(now.Add(time.Minute))) {
		return fmt.Errorf("a real anchor_id and anchor_at at or after since are required")
	}
	return nil
}

func (s *Service) handleHistoryRecovery(w http.ResponseWriter, r *http.Request) {
	// The metrics listener may be public. Never trust forwarded headers here.
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || !net.ParseIP(host).IsLoopback() {
		http.Error(w, "loopback access only", http.StatusForbidden)
		return
	}
	if r.Method == http.MethodGet {
		s.mu.RLock()
		job := s.historyRecoveries[r.URL.Query().Get("account_id")+"\x00"+r.URL.Query().Get("chat_jid")]
		s.mu.RUnlock()
		if job == nil || time.Now().After(job.expiresAt) {
			http.Error(w, "recovery not found or expired", http.StatusNotFound)
			return
		}
		job.respond(w, "observing")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req historyRecoveryRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32768))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if err := req.validate(time.Now()); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if s.deps.DeviceManager == nil || !s.publisher.IsConnected() {
		http.Error(w, "bridge or NATS unavailable", http.StatusServiceUnavailable)
		return
	}
	inst, ok := s.deps.DeviceManager.GetDevice(req.AccountID)
	if !ok || inst == nil || inst.GetClient() == nil || inst.JID() == "" {
		http.Error(w, "account session unavailable", http.StatusConflict)
		return
	}
	client := inst.GetClient()
	if !req.CacheOnly && (!client.IsConnected() || !client.IsLoggedIn() || s.isExplicitOffline(req.AccountID)) {
		http.Error(w, "account must already be connected", http.StatusConflict)
		return
	}
	// One manual request may run independently of startup's bulk sync queue.
	if !s.historyRecoverySlot.TryLock() {
		http.Error(w, "history recovery busy", http.StatusTooManyRequests)
		return
	}
	defer s.historyRecoverySlot.Unlock()
	job := &historyRecovery{request: req, expiresAt: time.Now().Add(30 * time.Minute), seen: make(map[string]bool)}
	key := req.AccountID + "\x00" + req.ChatJID
	s.mu.Lock()
	if s.historyRecoveries == nil {
		s.historyRecoveries = make(map[string]*historyRecovery)
	}
	for k, j := range s.historyRecoveries {
		if time.Now().After(j.expiresAt) {
			delete(s.historyRecoveries, k)
		}
	}
	if len(s.historyRecoveries) >= 256 {
		s.mu.Unlock()
		http.Error(w, "too many recovery windows", http.StatusTooManyRequests)
		return
	}
	if existing := s.historyRecoveries[key]; existing != nil {
		s.mu.Unlock()
		http.Error(w, "recovery already observing this chat", http.StatusConflict)
		return
	}
	s.historyRecoveries[key] = job
	s.mu.Unlock()
	keepJob := false
	defer func() {
		if !keepJob {
			s.mu.Lock()
			if s.historyRecoveries[key] == job {
				delete(s.historyRecoveries, key)
			}
			s.mu.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := s.recoverCachedReplies(ctx, inst, job); err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if req.CacheOnly {
		keepJob = true
		job.respond(w, "cache_replayed")
		return
	}
	chat, _ := types.ParseJID(req.ChatJID)
	anchor := &types.MessageInfo{MessageSource: types.MessageSource{Chat: chat, IsFromMe: req.FromMe}, ID: req.AnchorID, Timestamp: req.AnchorAt}
	if err := requestHistoryAroundMessage(ctx, client, anchor, req.Count); err != nil {
		http.Error(w, "history request failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	keepJob = true
	job.respond(w, "requested_not_yet_confirmed")
}

func (j *historyRecovery) respond(w http.ResponseWriter, status string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"status": status, "published": j.published, "history_responses": j.responses, "unverified_skipped": j.unverified, "expires_at": j.expiresAt})
}

func (s *Service) recoverCachedReplies(ctx context.Context, inst *whatsapp.DeviceInstance, job *historyRecovery) error {
	if s.deps.DB == nil {
		return fmt.Errorf("chat storage unavailable")
	}
	req := job.request
	storageDeviceID := inst.JID()
	if req.LegacyDeviceID != "" {
		storageDeviceID = req.LegacyDeviceID
	}
	rows, err := readHistoryRecoveryMessages(ctx, s.deps.DB, req, storageDeviceID)
	if err != nil {
		return err
	}
	if len(rows) > 500 {
		return fmt.Errorf("more than 500 cached replies; narrow the recovery window")
	}
	for _, m := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		if m == nil || m.IsFromMe || m.DeviceID != storageDeviceID || m.ChatJID != req.ChatJID {
			continue
		}
		// Old builds could save incoming messages under the default device.
		// Never trust that identity: require this account's cryptographic message
		// store to confirm the same private chat, sender and original message ID.
		verified, err := verifyLegacyReplyOwner(ctx, inst.GetClient(), req.ChatJID, m.ID)
		if err != nil {
			return fmt.Errorf("verify cached reply owner: %w", err)
		}
		if !verified {
			job.mu.Lock()
			job.unverified++
			job.mu.Unlock()
			continue
		}
		payload := map[string]any{"id": m.ID, "accountId": req.AccountID, "chatId": m.ChatJID, "from": m.ChatJID, "to": inst.JID(), "senderPhone": strings.Split(m.ChatJID, "@")[0], "type": "text", "content": map[string]any{"text": m.Content}, "timestamp": m.Timestamp.UnixMilli(), "isFromMe": false, "isGroup": false}
		// Only replay content we can actually recover, not media placeholders.
		if m.MediaType != "" {
			continue
		}
		if err := s.publishRecoveredReply(ctx, job, payload); err != nil {
			return err
		}
	}
	return nil
}

func historyRecoveryFilter(req historyRecoveryRequest, deviceID string) *domainChatStorage.MessageFilter {
	// SQLite stores these timestamps in UTC text. Bind UTC rather than the
	// caller's timezone so its indexed text comparison uses the same clock.
	since, until := req.Since.UTC(), req.Until.UTC()
	fromMe := false
	return &domainChatStorage.MessageFilter{DeviceID: deviceID, ChatJID: req.ChatJID, StartTime: &since, EndTime: &until, IsFromMe: &fromMe, Limit: 501}
}

func verifyLegacyReplyOwner(ctx context.Context, client *whatsmeow.Client, chat string, id string) (bool, error) {
	if client == nil || client.Store == nil || client.Store.ID == nil || client.Store.MsgSecrets == nil || id == "" {
		return false, nil
	}
	jid, err := types.ParseJID(chat)
	if err != nil || jid.Server != types.DefaultUserServer {
		return false, nil
	}
	key, _, err := client.Store.MsgSecrets.GetMessageSecret(ctx, jid, jid, id)
	if err != nil || len(key) > 0 {
		return len(key) > 0, err
	}
	if client.Store.LIDs == nil {
		return false, nil
	}
	lid, err := client.Store.LIDs.GetLIDForPN(ctx, jid)
	if err != nil || lid.IsEmpty() {
		return false, err
	}
	key, _, err = client.Store.MsgSecrets.GetMessageSecret(ctx, lid, lid, id)
	return len(key) > 0, err
}

func (s *Service) recoverHistoryReplies(ctx context.Context, accountID string, inst *whatsapp.DeviceInstance, data *waHistorySync.HistorySync) {
	if inst == nil || inst.GetClient() == nil || data == nil {
		return
	}
	for _, conv := range data.GetConversations() {
		if conv == nil {
			continue
		}
		chat := normalizedHistoryJID(ctx, inst, conv.GetID())
		s.mu.RLock()
		job := s.historyRecoveries[accountID+"\x00"+chat]
		s.mu.RUnlock()
		if job == nil || time.Now().After(job.expiresAt) {
			continue
		}
		job.mu.Lock()
		job.responses++
		job.mu.Unlock()
		jid, _ := types.ParseJID(conv.GetID())
		for _, hm := range conv.GetMessages() {
			if hm == nil || hm.Message == nil || hm.Message.GetKey() == nil || hm.Message.GetKey().GetFromMe() || hm.Message.GetMessage() == nil {
				continue
			}
			evt, err := inst.GetClient().ParseWebMessage(jid, hm.Message)
			if err != nil {
				continue
			}
			payload := s.toWhatsAppMessage(ctx, accountID, inst, evt)
			if err := s.publishRecoveredReply(ctx, job, payload); err != nil {
				logrus.WithError(err).WithField("account_id", accountID).Warn("history reply recovery publish failed")
			}
		}
	}
}

func (s *Service) publishRecoveredReply(ctx context.Context, job *historyRecovery, message map[string]any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id, _ := message["id"].(string)
	chat, _ := message["chatId"].(string)
	ts, _ := message["timestamp"].(int64)
	content, _ := message["content"].(map[string]any)
	body, _ := content["text"].(string)
	at := time.UnixMilli(ts)
	if id == "" || chat != job.request.ChatJID || message["isFromMe"] != false || message["isGroup"] != false || strings.TrimSpace(body) == "" || at.Before(job.request.Since) || !at.Before(job.request.Until) {
		return nil
	}
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.seen[id] {
		return nil
	}
	if !s.publisher.IsConnected() {
		return fmt.Errorf("NATS unavailable")
	}
	data, err := json.Marshal(map[string]any{"type": "message.received", "accountId": job.request.AccountID, "message": message, "timestamp": ts, "isBackfill": true, "source": "targeted_history_recovery"})
	if err != nil {
		return err
	}
	if err = s.publisher.conn.Publish("message.received", data); err != nil {
		return err
	}
	flushCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = s.publisher.conn.FlushWithContext(flushCtx); err != nil {
		return err
	}
	job.seen[id] = true
	job.published++
	return nil
}
