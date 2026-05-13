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
	accountID := instance.ID()
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
		s.handleMessageEvent(ctx, instance, evt)
	case *events.Receipt:
		s.handleReceiptEvent(accountID, evt)
	case *events.GroupInfo:
		s.handleGroupInfoEvent(accountID, evt)
	case *events.JoinedGroup:
		s.publish("group.joined", accountID, map[string]any{"groupJid": evt.JID.String()})
	}
}

func (s *Service) handleMessageEvent(ctx context.Context, instance *whatsapp.DeviceInstance, evt *events.Message) {
	msg := utils.UnwrapMessage(evt.Message)
	if protocol := msg.GetProtocolMessage(); protocol != nil && protocol.GetType().String() == "REVOKE" {
		if key := protocol.GetKey(); key != nil {
			s.publish("message.revoked", instance.ID(), map[string]any{"messageId": key.GetID(), "revokedBy": evt.Info.Sender.String()})
		}
		return
	}
	if reaction := msg.GetReactionMessage(); reaction != nil {
		s.publish("message.status", instance.ID(), map[string]any{"messageId": reaction.GetKey().GetID(), "status": "reaction"})
		return
	}

	message := s.toWhatsAppMessage(instance, evt)
	s.publish("message.received", instance.ID(), map[string]any{
		"message": message,
		"source":  "live",
	})
	if downloadable := downloadableMessage(msg); downloadable != nil {
		go s.downloadAndPublishMedia(ctx, instance, evt, downloadable, message)
	}
}

func (s *Service) toWhatsAppMessage(instance *whatsapp.DeviceInstance, evt *events.Message) map[string]any {
	msg := utils.BuildEventMessage(evt)
	unwrapped := utils.UnwrapMessage(evt.Message)
	mediaType, filename, _, _, _, _, _ := utils.ExtractMediaInfo(unwrapped)
	content := msg.Text
	if content == "" {
		content = utils.ExtractMediaCaption(unwrapped)
	}
	from := evt.Info.Sender.ToNonAD().String()
	if from == "" || evt.Info.Sender.IsEmpty() {
		from = evt.Info.Chat.ToNonAD().String()
	}
	to := ""
	if evt.Info.IsFromMe && instance.JID() != "" {
		from = instance.JID()
		to = evt.Info.Chat.ToNonAD().String()
	}
	return map[string]any{
		"id":            evt.Info.ID,
		"accountId":     instance.ID(),
		"chatId":        evt.Info.Chat.ToNonAD().String(),
		"from":          from,
		"to":            to,
		"type":          bridgeMessageType(mediaType, unwrapped),
		"content":       map[string]any{"text": content},
		"timestamp":     evt.Info.Timestamp.Unix(),
		"isGroup":       evt.Info.Chat.Server == types.GroupServer,
		"isFromMe":      evt.Info.IsFromMe,
		"quotedMessage": msg.QuotedMessage,
		"mimetype":      mediaMime(unwrapped),
		"author":        evt.Info.Sender.ToNonAD().String(),
		"hasMedia":      mediaType != "",
		"senderName":    evt.Info.PushName,
		"senderPhone":   strings.TrimSuffix(evt.Info.Sender.User, "@s.whatsapp.net"),
		"filename":      filename,
	}
}

func (s *Service) downloadAndPublishMedia(ctx context.Context, instance *whatsapp.DeviceInstance, evt *events.Message, media whatsmeow.DownloadableMessage, message map[string]any) {
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
	s.publish("message.media_ready", instance.ID(), map[string]any{
		"messageId":      evt.Info.ID,
		"mediaLocalPath": extracted.MediaPath,
		"mimetype":       extracted.MimeType,
	})
	if s.cfg.UploadMediaURL != "" {
		if err := s.uploadMedia(extracted.MediaPath, evt.Info.ID, bridgeMessageType("", utils.UnwrapMessage(evt.Message)), instance, extracted.MimeType); err != nil {
			logrus.WithError(err).Warn("failed to upload incoming media")
		}
	}
}

func (s *Service) handleReceiptEvent(accountID string, evt *events.Receipt) {
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
		s.publish("message.status", accountID, map[string]any{"messageId": id, "status": status})
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
