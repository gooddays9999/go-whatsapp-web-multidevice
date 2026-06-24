package bridge

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	domainSend "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/send"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/utils"
	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/validations"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waTypes "go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

const (
	newsletterTOSNoticeID = "20601218"
	newsletterTOSStage    = "5"
	newsletterPollType    = "pollCreation"

	defaultNewsletterMessageCount = 20
	maxNewsletterMessageCount     = 100
)

func (s *Service) CreateNewsletter(ctx context.Context, req *bridgepb.CreateNewsletterRequest) (*bridgepb.CreateNewsletterResponse, error) {
	if strings.TrimSpace(req.GetAccountId()) == "" || strings.TrimSpace(req.GetName()) == "" {
		return nil, grpcError(fmt.Errorf("account_id and name are required"))
	}
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	client, err := clientFromScopedContext(scoped)
	if err != nil {
		return nil, grpcError(err)
	}
	if req.GetAcceptTos() {
		if err := client.AcceptTOSNotice(scoped, newsletterTOSNoticeID, newsletterTOSStage); err != nil {
			return &bridgepb.CreateNewsletterResponse{Success: false, Status: "failed", Error: err.Error()}, nil
		}
	}
	meta, err := client.CreateNewsletter(scoped, whatsmeow.CreateNewsletterParams{
		Name:        strings.TrimSpace(req.GetName()),
		Description: strings.TrimSpace(req.GetDescription()),
	})
	if err != nil {
		return &bridgepb.CreateNewsletterResponse{Success: false, Status: "failed", Error: err.Error()}, nil
	}
	s.publish("newsletter.created", req.GetAccountId(), map[string]any{"newsletterId": meta.ID.String()})
	return &bridgepb.CreateNewsletterResponse{Success: true, Newsletter: newsletterMetadataToProto(meta), Status: "created"}, nil
}

func (s *Service) FollowNewsletter(ctx context.Context, req *bridgepb.FollowNewsletterRequest) (*bridgepb.FollowNewsletterResponse, error) {
	if strings.TrimSpace(req.GetAccountId()) == "" || strings.TrimSpace(req.GetNewsletterId()) == "" {
		return nil, grpcError(fmt.Errorf("account_id and newsletter_id are required"))
	}
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	client, err := clientFromScopedContext(scoped)
	if err != nil {
		return nil, grpcError(err)
	}
	jid, err := resolveNewsletterJID(scoped, client, req.GetNewsletterId())
	if err != nil {
		return &bridgepb.FollowNewsletterResponse{Success: false, Status: "failed", Error: err.Error()}, nil
	}
	if err := client.FollowNewsletter(scoped, jid); err != nil {
		return &bridgepb.FollowNewsletterResponse{Success: false, Status: "failed", Error: err.Error()}, nil
	}
	s.publish("newsletter.followed", req.GetAccountId(), map[string]any{"newsletterId": jid.String()})
	return &bridgepb.FollowNewsletterResponse{Success: true, Status: "followed"}, nil
}

func (s *Service) GetNewsletters(ctx context.Context, req *bridgepb.GetNewslettersRequest) (*bridgepb.GetNewslettersResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	client, err := clientFromScopedContext(scoped)
	if err != nil {
		return nil, grpcError(err)
	}
	items, err := client.GetSubscribedNewsletters(scoped)
	if err != nil {
		return nil, grpcError(err)
	}
	newsletters := make([]*bridgepb.Newsletter, 0, len(items))
	for _, item := range items {
		if converted := newsletterMetadataToProto(item); converted != nil {
			newsletters = append(newsletters, converted)
		}
	}
	return &bridgepb.GetNewslettersResponse{Newsletters: newsletters}, nil
}

func (s *Service) GetNewsletterMessages(ctx context.Context, req *bridgepb.GetNewsletterMessagesRequest) (*bridgepb.GetNewsletterMessagesResponse, error) {
	if strings.TrimSpace(req.GetAccountId()) == "" || strings.TrimSpace(req.GetNewsletterId()) == "" {
		return nil, grpcError(fmt.Errorf("account_id and newsletter_id are required"))
	}
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	client, err := clientFromScopedContext(scoped)
	if err != nil {
		return nil, grpcError(err)
	}
	jid, err := resolveNewsletterJID(scoped, client, req.GetNewsletterId())
	if err != nil {
		return nil, grpcError(err)
	}
	count := int(req.GetCount())
	if count <= 0 {
		count = defaultNewsletterMessageCount
	}
	if count > maxNewsletterMessageCount {
		count = maxNewsletterMessageCount
	}
	params := &whatsmeow.GetNewsletterMessagesParams{Count: count}
	if req.GetBefore() > 0 {
		params.Before = waTypes.MessageServerID(req.GetBefore())
	}
	items, err := client.GetNewsletterMessages(scoped, jid, params)
	if err != nil {
		return nil, grpcError(err)
	}
	messages := make([]*bridgepb.NewsletterMessage, 0, len(items))
	for _, item := range items {
		if converted := newsletterMessageToProto(item); converted != nil {
			messages = append(messages, converted)
		}
	}
	return &bridgepb.GetNewsletterMessagesResponse{Messages: messages}, nil
}

func (s *Service) SendNewsletterPoll(ctx context.Context, req *bridgepb.SendNewsletterPollRequest) (*bridgepb.SendNewsletterPollResponse, error) {
	if strings.TrimSpace(req.GetAccountId()) == "" || strings.TrimSpace(req.GetNewsletterId()) == "" {
		return nil, grpcError(fmt.Errorf("account_id and newsletter_id are required"))
	}
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		s.publish("message.failed", req.GetAccountId(), map[string]any{"to": req.GetNewsletterId(), "error": err.Error()})
		return nil, grpcError(err)
	}
	client, err := clientFromScopedContext(scoped)
	if err != nil {
		return nil, grpcError(err)
	}
	jid, err := resolveNewsletterJID(scoped, client, req.GetNewsletterId())
	if err != nil {
		return &bridgepb.SendNewsletterPollResponse{Success: false, Status: "failed", Error: err.Error()}, nil
	}
	pollReq := domainSend.PollRequest{
		BaseRequest: domainSend.BaseRequest{Phone: jid.String()},
		Question:    strings.TrimSpace(req.GetQuestion()),
		Options:     req.GetOptions(),
		MaxAnswer:   int(req.GetMaxAnswer()),
	}
	if req.GetDuration() > 0 {
		duration := int(req.GetDuration())
		pollReq.Duration = &duration
	}
	if err := validations.ValidateSendPoll(scoped, pollReq); err != nil {
		return &bridgepb.SendNewsletterPollResponse{Success: false, Status: "failed", Error: err.Error()}, nil
	}
	inst, _ := whatsapp.DeviceFromContext(scoped)
	timeout := statusTimeout(s.cfg.MessageSendTimeout, 25*time.Second)
	sendCtx, cancel := statusDeviceContext(ctx, inst, timeout)
	message := client.BuildPollCreation(pollReq.Question, pollReq.Options, pollReq.MaxAnswer)
	if pollReq.Duration != nil && *pollReq.Duration > 0 {
		if message.PollCreationMessage.ContextInfo == nil {
			message.PollCreationMessage.ContextInfo = &waE2E.ContextInfo{}
		}
		message.PollCreationMessage.ContextInfo.Expiration = proto.Uint32(uint32(*pollReq.Duration))
	}
	messageID, err := sendNewsletterPollNode(sendCtx, client, jid, message)
	cancel()
	if err != nil {
		stageErr := accountOperationError("SendNewsletterPoll", timeout, err)
		s.handleAccountOperationFailure(req.GetAccountId(), inst, "SendNewsletterPoll", err)
		s.publish("message.failed", req.GetAccountId(), map[string]any{"to": req.GetNewsletterId(), "error": stageErr.Error()})
		return &bridgepb.SendNewsletterPollResponse{Success: false, Status: "failed", Error: stageErr.Error()}, nil
	}
	s.publish("message.sent", req.GetAccountId(), map[string]any{"messageId": string(messageID), "to": jid.String(), "type": "poll"})
	return &bridgepb.SendNewsletterPollResponse{Success: true, MessageId: string(messageID), Status: "sent"}, nil
}

func sendNewsletterPollNode(ctx context.Context, client *whatsmeow.Client, to waTypes.JID, message *waE2E.Message) (waTypes.MessageID, error) {
	messageID := client.GenerateMessageID()
	node, err := buildNewsletterPollNode(to, messageID, message)
	if err != nil {
		return "", err
	}
	if _, err := client.DangerousInternals().SendNodeAndGetData(ctx, node); err != nil {
		return "", fmt.Errorf("failed to send newsletter poll node: %w", err)
	}
	if secret := message.GetMessageContextInfo().GetMessageSecret(); len(secret) > 0 && client.Store != nil && client.Store.MsgSecrets != nil && client.Store.ID != nil {
		if err := client.Store.MsgSecrets.PutMessageSecret(ctx, to, *client.Store.ID, messageID, secret); err != nil {
			client.Log.Warnf("Failed to store message secret key for outgoing newsletter poll %s: %v", messageID, err)
		}
	}
	return messageID, nil
}

func buildNewsletterPollNode(to waTypes.JID, id waTypes.MessageID, message *waE2E.Message) (waBinary.Node, error) {
	if to.IsEmpty() {
		return waBinary.Node{}, fmt.Errorf("newsletter JID is required")
	}
	if id == "" {
		return waBinary.Node{}, fmt.Errorf("message ID is required")
	}
	if message == nil || message.GetPollCreationMessage() == nil {
		return waBinary.Node{}, fmt.Errorf("poll creation message is required")
	}
	plaintext, err := proto.Marshal(message)
	if err != nil {
		return waBinary.Node{}, err
	}
	return waBinary.Node{
		Tag: "message",
		Attrs: waBinary.Attrs{
			"to":   to,
			"id":   id,
			"type": newsletterPollType,
		},
		Content: []waBinary.Node{{
			Tag:     "plaintext",
			Content: plaintext,
			Attrs:   waBinary.Attrs{},
		}},
	}, nil
}

func clientFromScopedContext(ctx context.Context) (*whatsmeow.Client, error) {
	inst, ok := whatsapp.DeviceFromContext(ctx)
	if !ok || inst == nil || inst.GetClient() == nil {
		return nil, fmt.Errorf("account not connected")
	}
	return inst.GetClient(), nil
}

func resolveNewsletterJID(ctx context.Context, client *whatsmeow.Client, raw string) (waTypes.JID, error) {
	target := strings.TrimSpace(raw)
	if target == "" {
		return waTypes.EmptyJID, fmt.Errorf("newsletter_id is required")
	}
	if inviteCode := newsletterInviteCode(target); inviteCode != "" {
		meta, err := client.GetNewsletterInfoWithInvite(ctx, inviteCode)
		if err != nil {
			return waTypes.EmptyJID, err
		}
		if meta == nil || meta.ID.IsEmpty() {
			return waTypes.EmptyJID, fmt.Errorf("newsletter invite did not resolve to a channel")
		}
		return meta.ID, nil
	}
	jid, err := utils.ParseJID(target)
	if err != nil {
		return waTypes.EmptyJID, err
	}
	if jid.Server != waTypes.NewsletterServer {
		return waTypes.EmptyJID, fmt.Errorf("newsletter_id must be a @newsletter JID or WhatsApp channel link")
	}
	return jid, nil
}

func newsletterInviteCode(raw string) string {
	target := strings.TrimSpace(raw)
	if target == "" {
		return ""
	}
	if !strings.Contains(target, "://") && !strings.Contains(target, "/") && !strings.Contains(target, "@") {
		return target
	}
	parsed, err := url.Parse(target)
	if err != nil || parsed.Host == "" {
		return ""
	}
	host := strings.ToLower(parsed.Host)
	if host != "whatsapp.com" && host != "www.whatsapp.com" {
		return ""
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) == 2 && parts[0] == "channel" && parts[1] != "" {
		return parts[1]
	}
	return ""
}

func newsletterMetadataToProto(meta *waTypes.NewsletterMetadata) *bridgepb.Newsletter {
	if meta == nil {
		return nil
	}
	item := &bridgepb.Newsletter{
		Id:              meta.ID.String(),
		Name:            meta.ThreadMeta.Name.Text,
		Description:     meta.ThreadMeta.Description.Text,
		SubscriberCount: int32(meta.ThreadMeta.SubscriberCount),
		State:           string(meta.State.Type),
	}
	if meta.ThreadMeta.InviteCode != "" {
		item.InviteLink = "https://whatsapp.com/channel/" + meta.ThreadMeta.InviteCode
	}
	if meta.ViewerMeta != nil {
		item.Role = string(meta.ViewerMeta.Role)
	}
	return item
}

func newsletterMessageToProto(msg *waTypes.NewsletterMessage) *bridgepb.NewsletterMessage {
	if msg == nil {
		return nil
	}
	item := &bridgepb.NewsletterMessage{
		ServerId:   fmt.Sprint(msg.MessageServerID),
		MessageId:  string(msg.MessageID),
		Type:       msg.Type,
		ViewsCount: int32(msg.ViewsCount),
		Text:       newsletterMessageText(msg.Message),
	}
	if !msg.Timestamp.IsZero() {
		item.Timestamp = msg.Timestamp.Unix()
	}
	if len(msg.ReactionCounts) > 0 {
		item.ReactionCounts = make(map[string]int32, len(msg.ReactionCounts))
		for code, count := range msg.ReactionCounts {
			item.ReactionCounts[code] = int32(count)
		}
	}
	if field, poll := newsletterMessagePoll(msg.Message); field != "" {
		item.HasPoll = true
		item.PollField = field
		if poll != nil {
			item.PollName = poll.GetName()
			item.OptionCount = int32(len(poll.GetOptions()))
			item.SelectableOptionsCount = int32(poll.GetSelectableOptionsCount())
		}
	}
	return item
}

func newsletterMessageText(message *waE2E.Message) string {
	if message == nil {
		return ""
	}
	if text := message.GetConversation(); text != "" {
		return text
	}
	return message.GetExtendedTextMessage().GetText()
}

func newsletterMessagePoll(message *waE2E.Message) (string, *waE2E.PollCreationMessage) {
	if message == nil {
		return "", nil
	}
	switch {
	case message.GetPollCreationMessage() != nil:
		return "pollCreationMessage", message.GetPollCreationMessage()
	case message.GetPollCreationMessageV2() != nil:
		return "pollCreationMessageV2", message.GetPollCreationMessageV2()
	case message.GetPollCreationMessageV3() != nil:
		return "pollCreationMessageV3", message.GetPollCreationMessageV3()
	case message.GetPollCreationMessageV4() != nil:
		return "pollCreationMessageV4", nil
	case message.GetPollCreationMessageV5() != nil:
		return "pollCreationMessageV5", message.GetPollCreationMessageV5()
	case message.GetPollCreationMessageV6() != nil:
		return "pollCreationMessageV6", message.GetPollCreationMessageV6()
	case message.GetPollUpdateMessage() != nil:
		return "pollUpdateMessage", nil
	default:
		return "", nil
	}
}
