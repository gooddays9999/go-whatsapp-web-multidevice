package bridge

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/utils"
	"github.com/sirupsen/logrus"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func (s *Service) HandleWhatsAppEvent(ctx context.Context, instance *whatsapp.DeviceInstance, rawEvt any) {
	if instance == nil {
		return
	}
	accountID := s.eventAccountID(instance)
	switch evt := rawEvt.(type) {
	case *events.Connected:
		s.markConnected(accountID)
		s.publish("account.connected", accountID, map[string]any{
			"phoneNumber": instance.PhoneNumber(),
			"workerId":    s.workerID,
			"connectedAt": time.Now().UnixMilli(),
			"verified":    instance.IsLoggedIn(),
		})
	case *events.PairSuccess:
		s.publish("account.authenticated", accountID, map[string]any{"phoneNumber": evt.ID.User})
	case *events.LoggedOut:
		s.markDisconnected(accountID)
		s.publish("account.logout", accountID, map[string]any{"reason": "logged_out"})
	case *events.Message:
		s.handleMessageEvent(ctx, accountID, instance, evt)
	case *events.Receipt:
		s.handleReceiptEvent(ctx, accountID, instance, evt)
	case *events.GroupInfo:
		s.handleGroupInfoEvent(accountID, evt)
	case *events.JoinedGroup:
		s.publish("group.joined", accountID, map[string]any{"groupJid": evt.JID.String()})
	}
}

func (s *Service) handleMessageEvent(ctx context.Context, accountID string, instance *whatsapp.DeviceInstance, evt *events.Message) {
	msg := utils.UnwrapMessage(evt.Message)
	if protocol := msg.GetProtocolMessage(); protocol != nil && protocol.GetType().String() == "REVOKE" {
		if key := protocol.GetKey(); key != nil {
			s.publish("message.revoked", accountID, map[string]any{"messageId": key.GetID(), "revokedBy": normalizedEventJID(ctx, instance, evt.Info.Sender).String()})
		}
		return
	}
	if reaction := msg.GetReactionMessage(); reaction != nil {
		s.publish("message.status", accountID, map[string]any{"messageId": reaction.GetKey().GetID(), "status": "reaction"})
		return
	}

	message := s.toWhatsAppMessage(ctx, accountID, instance, evt)
	s.publish("message.received", accountID, map[string]any{
		"message": message,
		"source":  "live",
	})
	if downloadable := downloadableMessage(msg); downloadable != nil {
		go s.downloadAndPublishMedia(ctx, accountID, instance, evt, downloadable, message)
	}
}

func (s *Service) toWhatsAppMessage(ctx context.Context, accountID string, instance *whatsapp.DeviceInstance, evt *events.Message) map[string]any {
	msg := utils.BuildEventMessage(evt)
	unwrapped := utils.UnwrapMessage(evt.Message)
	mediaType, filename, _, _, _, _, _ := utils.ExtractMediaInfo(unwrapped)
	content := msg.Text
	if content == "" {
		content = utils.ExtractMediaCaption(unwrapped)
	}
	chatJID := normalizedEventJID(ctx, instance, evt.Info.Chat).ToNonAD()
	senderJID := normalizedEventJID(ctx, instance, evt.Info.Sender).ToNonAD()
	if senderJID.IsEmpty() {
		senderJID = chatJID
	}
	from := senderJID.String()
	if from == "" {
		from = chatJID.String()
	}
	to := ""
	if evt.Info.IsFromMe && instance.JID() != "" {
		from = normalizedJIDString(instance.JID())
		to = chatJID.String()
	}
	payload := map[string]any{
		"id":            evt.Info.ID,
		"accountId":     accountID,
		"chatId":        chatJID.String(),
		"from":          from,
		"to":            to,
		"type":          bridgeMessageType(mediaType, unwrapped),
		"content":       map[string]any{"text": content},
		"timestamp":     evt.Info.Timestamp.UnixMilli(),
		"isGroup":       chatJID.Server == types.GroupServer,
		"isFromMe":      evt.Info.IsFromMe,
		"quotedMessage": msg.QuotedMessage,
		"mimetype":      mediaMime(unwrapped),
		"author":        senderJID.String(),
		"hasMedia":      mediaType != "",
		"senderName":    evt.Info.PushName,
		"senderPhone":   senderJID.User,
		"filename":      filename,
	}
	if evt.Info.Sender.Server == types.HiddenUserServer {
		payload["fromLid"] = evt.Info.Sender.ToNonAD().String()
	}
	if evt.Info.Chat.Server == types.HiddenUserServer {
		payload["chatLid"] = evt.Info.Chat.ToNonAD().String()
	}
	return payload
}

func (s *Service) downloadAndPublishMedia(ctx context.Context, accountID string, instance *whatsapp.DeviceInstance, evt *events.Message, media whatsmeow.DownloadableMessage, message map[string]any) {
	client := instance.GetClient()
	if client == nil {
		return
	}
	if err := ensureDir(filepath.Join(s.cfg.MediaDownloadPath, "files")); err != nil {
		logrus.WithError(err).Warn("failed to create media directory")
		return
	}
	extracted, err := utils.ExtractMedia(ctx, client, filepath.Join(s.cfg.MediaDownloadPath, "files"), media)
	if err != nil {
		logrus.WithError(err).Warn("failed to download incoming media")
		return
	}
	message["mediaLocalPath"] = extracted.MediaPath
	s.publish("message.media_ready", accountID, map[string]any{
		"messageId":      evt.Info.ID,
		"mediaLocalPath": extracted.MediaPath,
		"mimetype":       extracted.MimeType,
	})
	if s.cfg.UploadMediaURL != "" {
		if err := s.uploadMedia(extracted.MediaPath, evt.Info.ID, bridgeMessageType("", utils.UnwrapMessage(evt.Message)), accountID, instance, extracted.MimeType); err != nil {
			logrus.WithError(err).Warn("failed to upload incoming media")
		}
	}
}

func (s *Service) eventAccountID(instance *whatsapp.DeviceInstance) string {
	if instance == nil {
		return ""
	}
	rawID := strings.TrimSpace(instance.ID())
	if rawID == "" || !strings.Contains(rawID, "@") {
		return rawID
	}

	targets := map[string]struct{}{}
	addTarget := func(value string) {
		if normalized := normalizedJIDString(value); normalized != "" {
			targets[normalized] = struct{}{}
		}
	}
	addTarget(rawID)
	addTarget(instance.JID())
	if client := instance.GetClient(); client != nil && client.Store != nil && client.Store.ID != nil {
		addTarget(client.Store.ID.ToNonAD().String())
	}
	if len(targets) == 0 {
		return rawID
	}

	if s.deps.DeviceManager != nil {
		for _, inst := range s.deps.DeviceManager.ListDevices() {
			if inst == nil {
				continue
			}
			deviceID := strings.TrimSpace(inst.ID())
			if deviceID == "" || strings.Contains(deviceID, "@") {
				continue
			}
			if _, ok := targets[normalizedJIDString(inst.JID())]; ok {
				return deviceID
			}
			if client := inst.GetClient(); client != nil && client.Store != nil && client.Store.ID != nil {
				if _, ok := targets[normalizedJIDString(client.Store.ID.ToNonAD().String())]; ok {
					return deviceID
				}
			}
		}
	}

	if s.deps.ChatStorageRepo != nil {
		if records, err := s.deps.ChatStorageRepo.ListDeviceRecords(); err == nil {
			for _, rec := range records {
				if rec == nil {
					continue
				}
				deviceID := strings.TrimSpace(rec.DeviceID)
				if deviceID == "" || strings.Contains(deviceID, "@") {
					continue
				}
				if _, ok := targets[normalizedJIDString(rec.JID)]; ok {
					return deviceID
				}
			}
		}
	}

	return rawID
}

func normalizedEventJID(ctx context.Context, instance *whatsapp.DeviceInstance, jid types.JID) types.JID {
	if instance == nil {
		return jid
	}
	return whatsapp.NormalizeJIDFromLID(ctx, jid, instance.GetClient())
}

func normalizedJIDString(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	jid, err := types.ParseJID(value)
	if err != nil || jid.IsEmpty() {
		return value
	}
	return jid.ToNonAD().String()
}

func (s *Service) handleReceiptEvent(ctx context.Context, accountID string, instance *whatsapp.DeviceInstance, evt *events.Receipt) {
	status := "sent"
	switch evt.Type {
	case types.ReceiptTypeDelivered:
		status = "delivered"
	case types.ReceiptTypeRead, types.ReceiptTypeReadSelf:
		status = "read"
	case types.ReceiptTypeRetry:
		status = "failed"
	}
	for _, id := range evt.MessageIDs {
		s.publish("message.status", accountID, map[string]any{
			"messageId": id,
			"status":    status,
			"fromMe":    s.receiptAppliesToOutgoing(ctx, instance, id, evt),
			"chatId":    normalizedEventJID(ctx, instance, evt.Chat).ToNonAD().String(),
			"from":      normalizedEventJID(ctx, instance, evt.Sender).ToNonAD().String(),
			"timestamp": evt.Timestamp.UnixMilli(),
		})
	}
}

func (s *Service) receiptAppliesToOutgoing(ctx context.Context, instance *whatsapp.DeviceInstance, id types.MessageID, evt *events.Receipt) bool {
	if instance != nil {
		if repo := instance.GetChatStorage(); repo != nil {
			if msg, err := repo.GetMessageByID(id); err == nil && msg != nil {
				return msg.IsFromMe
			}
		}
	}
	if s.deps.ChatStorageRepo != nil {
		if msg, err := s.deps.ChatStorageRepo.GetMessageByID(id); err == nil && msg != nil {
			return msg.IsFromMe
		}
	}

	switch evt.Type {
	case types.ReceiptTypeDelivered, types.ReceiptTypeRead, types.ReceiptTypePlayed:
		return true
	default:
		return false
	}
}

func (s *Service) handleGroupInfoEvent(accountID string, evt *events.GroupInfo) {
	for _, jid := range evt.Join {
		s.publish("group.join", accountID, map[string]any{"groupJid": evt.JID.String(), "participant": jid.String()})
	}
	for _, jid := range evt.Leave {
		s.publish("group.leave", accountID, map[string]any{"groupJid": evt.JID.String(), "participant": jid.String()})
	}
}

func downloadableMessage(msg *waE2E.Message) whatsmeow.DownloadableMessage {
	switch {
	case msg.GetImageMessage() != nil:
		return msg.GetImageMessage()
	case msg.GetAudioMessage() != nil:
		return msg.GetAudioMessage()
	case msg.GetVideoMessage() != nil:
		return msg.GetVideoMessage()
	case msg.GetDocumentMessage() != nil:
		return msg.GetDocumentMessage()
	case msg.GetStickerMessage() != nil:
		return msg.GetStickerMessage()
	default:
		return nil
	}
}

func bridgeMessageType(mediaType string, msg *waE2E.Message) string {
	if mediaType != "" {
		return strings.ToLower(mediaType)
	}
	switch {
	case msg.GetImageMessage() != nil:
		return "image"
	case msg.GetVideoMessage() != nil:
		return "video"
	case msg.GetAudioMessage() != nil:
		return "audio"
	case msg.GetDocumentMessage() != nil:
		return "document"
	case msg.GetStickerMessage() != nil:
		return "sticker"
	default:
		return "text"
	}
}

func mediaMime(msg *waE2E.Message) string {
	switch {
	case msg.GetImageMessage() != nil:
		return msg.GetImageMessage().GetMimetype()
	case msg.GetVideoMessage() != nil:
		return msg.GetVideoMessage().GetMimetype()
	case msg.GetAudioMessage() != nil:
		return msg.GetAudioMessage().GetMimetype()
	case msg.GetDocumentMessage() != nil:
		return msg.GetDocumentMessage().GetMimetype()
	case msg.GetStickerMessage() != nil:
		return msg.GetStickerMessage().GetMimetype()
	default:
		return ""
	}
}

func ensureDir(path string) error {
	return os.MkdirAll(path, 0750)
}
