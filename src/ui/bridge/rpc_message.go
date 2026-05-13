package bridge

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	domainMessage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/message"
	domainSend "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/send"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func (s *Service) accountContext(ctx context.Context, accountID string) (context.Context, error) {
	if accountID == "" {
		return nil, fmt.Errorf("account_id is required")
	}
	env, err := s.envStore.Get(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if env == nil {
		return nil, fmt.Errorf("account not connected")
	}
	proxyURL, err := env.ProxyURL()
	if err != nil {
		return nil, err
	}
	inst, err := s.deps.DeviceManager.EnsureClientWithEnvironment(ctx, accountID, whatsapp.ClientEnvironment{
		ProxyAddress:  proxyURL,
		UserAgent:     env.UserAgent,
		BrowserFamily: env.BrowserFamily,
		OSName:        env.OSName,
	})
	if err != nil {
		return nil, err
	}
	client := inst.GetClient()
	if client == nil || client.Store == nil || client.Store.ID == nil {
		return nil, fmt.Errorf("account not connected")
	}
	if !client.IsConnected() {
		if err := client.Connect(); err != nil {
			return nil, err
		}
	}
	if !client.IsLoggedIn() {
		return nil, fmt.Errorf("account not logged in")
	}
	return whatsapp.ContextWithDevice(ctx, inst), nil
}

func (s *Service) SendMessage(ctx context.Context, req *bridgepb.SendMessageRequest) (*bridgepb.SendMessageResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		s.publish("message.failed", req.GetAccountId(), map[string]any{"to": req.GetTo(), "error": err.Error()})
		return nil, grpcError(err)
	}
	resp, err := s.deps.SendUsecase.SendText(scoped, domainSend.MessageRequest{
		BaseRequest:    domainSend.BaseRequest{Phone: req.GetTo()},
		Message:        req.GetContent().GetText(),
		ReplyMessageID: optionalString(req.GetQuotedMsgId()),
	})
	if err != nil {
		s.publish("message.failed", req.GetAccountId(), map[string]any{"to": req.GetTo(), "error": err.Error()})
		return &bridgepb.SendMessageResponse{Success: false, Status: "failed", Error: err.Error()}, nil
	}
	s.publish("message.sent", req.GetAccountId(), map[string]any{"messageId": resp.MessageID, "to": req.GetTo()})
	return &bridgepb.SendMessageResponse{Success: true, MessageId: resp.MessageID, Status: "sent"}, nil
}

func (s *Service) SendMedia(ctx context.Context, req *bridgepb.SendMediaRequest) (*bridgepb.SendMediaResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		s.publish("message.failed", req.GetAccountId(), map[string]any{"to": req.GetTo(), "error": err.Error()})
		return nil, grpcError(err)
	}
	mediaURL := req.GetMediaUrl()
	var msgID string
	switch strings.ToLower(req.GetType()) {
	case "image":
		resp, err := s.deps.SendUsecase.SendImage(scoped, domainSend.ImageRequest{BaseRequest: domainSend.BaseRequest{Phone: req.GetTo()}, ImageURL: &mediaURL, Caption: req.GetCaption()})
		if err != nil {
			return mediaFailed(s, req, err), nil
		}
		msgID = resp.MessageID
	case "video":
		resp, err := s.deps.SendUsecase.SendVideo(scoped, domainSend.VideoRequest{BaseRequest: domainSend.BaseRequest{Phone: req.GetTo()}, VideoURL: &mediaURL, Caption: req.GetCaption()})
		if err != nil {
			return mediaFailed(s, req, err), nil
		}
		msgID = resp.MessageID
	case "audio":
		resp, err := s.deps.SendUsecase.SendAudio(scoped, domainSend.AudioRequest{BaseRequest: domainSend.BaseRequest{Phone: req.GetTo()}, AudioURL: &mediaURL, PTT: req.GetSendAudioAsVoice()})
		if err != nil {
			return mediaFailed(s, req, err), nil
		}
		msgID = resp.MessageID
	default:
		resp, err := s.deps.SendUsecase.SendFile(scoped, domainSend.FileRequest{BaseRequest: domainSend.BaseRequest{Phone: req.GetTo()}, FileURL: &mediaURL, Caption: req.GetCaption()})
		if err != nil {
			return mediaFailed(s, req, err), nil
		}
		msgID = resp.MessageID
	}
	s.publish("message.sent", req.GetAccountId(), map[string]any{"messageId": msgID, "to": req.GetTo()})
	return &bridgepb.SendMediaResponse{Success: true, MessageId: msgID, Status: "sent"}, nil
}

func mediaFailed(s *Service, req *bridgepb.SendMediaRequest, err error) *bridgepb.SendMediaResponse {
	s.publish("message.failed", req.GetAccountId(), map[string]any{"to": req.GetTo(), "error": err.Error()})
	return &bridgepb.SendMediaResponse{Success: false, Status: "failed", Error: err.Error()}
}

func (s *Service) SendContact(ctx context.Context, req *bridgepb.SendContactRequest) (*bridgepb.SendContactResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	name, phone := parseContactData(req.GetContactData())
	resp, err := s.deps.SendUsecase.SendContact(scoped, domainSend.ContactRequest{
		BaseRequest:  domainSend.BaseRequest{Phone: req.GetTo()},
		ContactName:  name,
		ContactPhone: phone,
	})
	if err != nil {
		return &bridgepb.SendContactResponse{Success: false, Status: "failed", Error: err.Error()}, nil
	}
	return &bridgepb.SendContactResponse{Success: true, MessageId: resp.MessageID, Status: "sent"}, nil
}

func (s *Service) GetMessageStatus(ctx context.Context, req *bridgepb.MessageStatusRequest) (*bridgepb.MessageStatusResponse, error) {
	if req.GetAccountId() == "" || req.GetMessageId() == "" {
		return nil, grpcError(fmt.Errorf("account_id and message_id are required"))
	}
	if s.deps.ChatStorageRepo != nil {
		if msg, _ := s.deps.ChatStorageRepo.GetMessageByID(req.GetMessageId()); msg != nil {
			return &bridgepb.MessageStatusResponse{MessageId: req.GetMessageId(), Status: "sent", Timestamp: msg.Timestamp.UnixMilli()}, nil
		}
	}
	return &bridgepb.MessageStatusResponse{MessageId: req.GetMessageId(), Status: "unknown", Timestamp: 0}, nil
}

func (s *Service) ReactToMessage(ctx context.Context, req *bridgepb.ReactToMessageRequest) (*bridgepb.ReactToMessageResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	msg, _ := s.deps.ChatStorageRepo.GetMessageByID(req.GetMessageId())
	if msg == nil {
		return nil, grpcError(fmt.Errorf("message not found: %s", req.GetMessageId()))
	}
	resp, err := s.deps.MessageUsecase.ReactMessage(scoped, domainMessage.ReactionRequest{
		MessageID: req.GetMessageId(),
		Phone:     msg.ChatJID,
		Emoji:     req.GetEmoji(),
	})
	if err != nil {
		return &bridgepb.ReactToMessageResponse{Success: false, MessageId: req.GetMessageId(), Emoji: req.GetEmoji(), Error: err.Error()}, nil
	}
	action := "add"
	if req.GetEmoji() == "" {
		action = "remove"
	}
	return &bridgepb.ReactToMessageResponse{Success: true, MessageId: resp.MessageID, Emoji: req.GetEmoji(), Action: action}, nil
}

func (s *Service) GetMessageReactions(ctx context.Context, req *bridgepb.GetMessageReactionsRequest) (*bridgepb.GetMessageReactionsResponse, error) {
	if req.GetAccountId() == "" || req.GetMessageId() == "" {
		return nil, grpcError(fmt.Errorf("account_id and message_id are required"))
	}
	return &bridgepb.GetMessageReactionsResponse{Success: true, MessageId: req.GetMessageId(), HasReaction: false, Reactions: []*bridgepb.ReactionGroup{}}, nil
}

func (s *Service) SendStatus(ctx context.Context, req *bridgepb.SendStatusRequest) (*bridgepb.SendStatusResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	inst, _ := whatsapp.DeviceFromContext(scoped)
	client := inst.GetClient()
	msg := &waE2E.Message{Conversation: proto.String(req.GetContent())}
	ts, err := client.SendMessage(scoped, types.StatusBroadcastJID, msg)
	if err != nil {
		return &bridgepb.SendStatusResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.SendStatusResponse{Success: true, MessageId: ts.ID, HasMedia: false}, nil
}

func (s *Service) CommentStatus(ctx context.Context, req *bridgepb.CommentStatusRequest) (*bridgepb.CommentStatusResponse, error) {
	if req.GetAccountId() == "" || (req.GetMessageId() == "" && req.GetUserId() == "") {
		return nil, grpcError(fmt.Errorf("account_id and message_id or user_id are required"))
	}
	return &bridgepb.CommentStatusResponse{Success: false, Error: "status comment is not supported by this bridge"}, nil
}

func (s *Service) LikeStatus(ctx context.Context, req *bridgepb.LikeStatusRequest) (*bridgepb.LikeStatusResponse, error) {
	if req.GetAccountId() == "" || (req.GetMessageId() == "" && req.GetUserId() == "") {
		return nil, grpcError(fmt.Errorf("account_id and message_id or user_id are required"))
	}
	return &bridgepb.LikeStatusResponse{Success: false, Error: "status like is not supported by this bridge"}, nil
}

func (s *Service) GetStatusViewers(ctx context.Context, req *bridgepb.GetStatusViewersRequest) (*bridgepb.GetStatusViewersResponse, error) {
	if req.GetAccountId() == "" || req.GetMessageId() == "" {
		return nil, grpcError(fmt.Errorf("account_id and message_id are required"))
	}
	return &bridgepb.GetStatusViewersResponse{Success: true, Viewers: []*bridgepb.StatusViewer{}, TotalCount: 0, RemainingCount: 0}, nil
}

func (s *Service) DeleteMessage(ctx context.Context, req *bridgepb.DeleteMessageRequest) (*bridgepb.DeleteMessageResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.deps.MessageUsecase.DeleteMessage(scoped, domainMessage.DeleteRequest{MessageID: req.GetMessageId(), Phone: req.GetChatId()}); err != nil {
		return &bridgepb.DeleteMessageResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.DeleteMessageResponse{Success: true}, nil
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

var phonePattern = regexp.MustCompile(`\+?\d{7,18}`)

func parseContactData(data string) (string, string) {
	phone := phonePattern.FindString(data)
	phone = strings.TrimPrefix(phone, "+")
	name := "Contact"
	for _, line := range strings.Split(data, "\n") {
		if strings.HasPrefix(strings.ToUpper(line), "FN:") && len(line) > 3 {
			name = strings.TrimSpace(line[3:])
			break
		}
	}
	return name, phone
}
