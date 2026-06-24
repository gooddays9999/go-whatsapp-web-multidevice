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
	"go.mau.fi/whatsmeow"
	waTypes "go.mau.fi/whatsmeow/types"
)

const (
	newsletterTOSNoticeID = "20601218"
	newsletterTOSStage    = "5"
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
	inst, _ := whatsapp.DeviceFromContext(scoped)
	timeout := statusTimeout(s.cfg.MessageSendTimeout, 25*time.Second)
	sendCtx, cancel := statusDeviceContext(ctx, inst, timeout)
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
	resp, err := s.deps.SendUsecase.SendPoll(sendCtx, pollReq)
	cancel()
	if err != nil {
		stageErr := accountOperationError("SendNewsletterPoll", timeout, err)
		s.handleAccountOperationFailure(req.GetAccountId(), inst, "SendNewsletterPoll", err)
		s.publish("message.failed", req.GetAccountId(), map[string]any{"to": req.GetNewsletterId(), "error": stageErr.Error()})
		return &bridgepb.SendNewsletterPollResponse{Success: false, Status: "failed", Error: stageErr.Error()}, nil
	}
	s.publish("message.sent", req.GetAccountId(), map[string]any{"messageId": resp.MessageID, "to": jid.String(), "type": "poll"})
	return &bridgepb.SendNewsletterPollResponse{Success: true, MessageId: resp.MessageID, Status: "sent"}, nil
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
