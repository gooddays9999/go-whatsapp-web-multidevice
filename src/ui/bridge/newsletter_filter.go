package bridge

import (
	"strings"

	"go.mau.fi/whatsmeow/types"
)

func isNewsletterChat(chat string) bool {
	jid, err := types.ParseJID(strings.TrimSpace(chat))
	return err == nil && jid.Server == types.NewsletterServer
}

func (s *Service) skipNewsletterChat(chat string) bool {
	if !isNewsletterChat(chat) {
		return false
	}
	s.newsletterSkipped.Add(1)
	return true
}

// Inspect only the primary chat, never quoted messages or message text.
func (s *Service) skipNewsletterPayload(eventType string, payload map[string]any) bool {
	if !strings.HasPrefix(eventType, "message.") {
		return false
	}
	if chat, _ := payload["chatId"].(string); isNewsletterChat(chat) {
		s.newsletterSkipped.Add(1)
		return true
	}
	if message, ok := payload["message"].(map[string]any); ok {
		chat, _ := message["chatId"].(string)
		return s.skipNewsletterChat(chat)
	}
	return false
}
