package bridge

import (
	"context"
	"fmt"
	"strings"
	"time"

	domainGroup "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/group"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/utils"
	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	"go.mau.fi/whatsmeow"
)

func groupApprovalAction(value string) (whatsmeow.ParticipantRequestChange, error) {
	switch strings.TrimSpace(value) {
	case "approve":
		return whatsmeow.ParticipantChangeApprove, nil
	case "reject":
		return whatsmeow.ParticipantChangeReject, nil
	default:
		return "", fmt.Errorf("invalid join request action")
	}
}

func (s *Service) SetGroupJoinApproval(ctx context.Context, req *bridgepb.SetGroupJoinApprovalRequest) (*bridgepb.SetGroupJoinApprovalResponse, error) {
	if req.GetAccountId() == "" || !strings.HasSuffix(req.GetGroupJid(), "@g.us") {
		return nil, grpcError(fmt.Errorf("account_id and group_jid are required"))
	}
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	client := whatsapp.ClientFromContext(scoped)
	if client == nil {
		return nil, grpcError(fmt.Errorf("account not connected"))
	}
	jid, err := utils.ValidateJidWithLogin(client, req.GetGroupJid())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := client.SetGroupJoinApprovalMode(scoped, jid, req.GetEnabled()); err != nil {
		return &bridgepb.SetGroupJoinApprovalResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.SetGroupJoinApprovalResponse{Success: true}, nil
}

func (s *Service) GetGroupJoinRequests(ctx context.Context, req *bridgepb.GetGroupJoinRequestsRequest) (*bridgepb.GetGroupJoinRequestsResponse, error) {
	if req.GetAccountId() == "" || !strings.HasSuffix(req.GetGroupJid(), "@g.us") {
		return nil, grpcError(fmt.Errorf("account_id and group_jid are required"))
	}
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	items, err := s.deps.GroupUsecase.GetGroupRequestParticipants(scoped, domainGroup.GetGroupRequestParticipantsRequest{GroupID: req.GetGroupJid()})
	if err != nil {
		return nil, grpcError(err)
	}
	out := &bridgepb.GetGroupJoinRequestsResponse{Requests: make([]*bridgepb.GroupJoinRequest, 0, len(items))}
	for _, item := range items {
		out.Requests = append(out.Requests, &bridgepb.GroupJoinRequest{Jid: item.JID, PhoneNumber: item.PhoneNumber, DisplayName: item.DisplayName, RequestedAt: item.RequestedAt.Unix()})
	}
	return out, nil
}

func (s *Service) ActOnGroupJoinRequests(ctx context.Context, req *bridgepb.ActOnGroupJoinRequestsRequest) (*bridgepb.ActOnGroupJoinRequestsResponse, error) {
	if req.GetAccountId() == "" || !strings.HasSuffix(req.GetGroupJid(), "@g.us") || len(req.GetParticipants()) == 0 {
		return nil, grpcError(fmt.Errorf("account_id, group_jid and participants are required"))
	}
	action, err := groupApprovalAction(req.GetAction())
	if err != nil {
		return nil, grpcError(err)
	}
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	items, err := s.deps.GroupUsecase.ManageGroupRequestParticipants(scoped, domainGroup.GroupRequestParticipantsRequest{GroupID: req.GetGroupJid(), Participants: req.GetParticipants(), Action: action})
	if err != nil {
		return nil, grpcError(err)
	}
	out := &bridgepb.ActOnGroupJoinRequestsResponse{Success: len(items) == len(req.GetParticipants()), Results: make([]*bridgepb.GroupJoinRequestResult, 0, len(items))}
	for _, item := range items {
		out.Results = append(out.Results, &bridgepb.GroupJoinRequestResult{Jid: item.Participant, Status: item.Status, Message: item.Message})
		if item.Status != "success" {
			out.Success = false
		}
	}
	if !out.Success {
		out.Error = "one or more join requests failed"
	}
	return out, nil
}

func (s *Service) SetGroupEphemeral(ctx context.Context, req *bridgepb.SetGroupEphemeralRequest) (*bridgepb.SetGroupEphemeralResponse, error) {
	if req.GetAccountId() == "" || !strings.HasSuffix(req.GetGroupJid(), "@g.us") {
		return nil, grpcError(fmt.Errorf("account_id and group_jid are required"))
	}
	if !validGroupExpiration(req.GetExpiration()) {
		return nil, grpcError(fmt.Errorf("invalid group expiration"))
	}
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	client := whatsapp.ClientFromContext(scoped)
	if client == nil {
		return nil, grpcError(fmt.Errorf("account not connected"))
	}
	jid, err := utils.ValidateJidWithLogin(client, req.GetGroupJid())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := client.SetDisappearingTimer(scoped, jid, time.Duration(req.GetExpiration())*time.Second, time.Time{}); err != nil {
		return &bridgepb.SetGroupEphemeralResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.SetGroupEphemeralResponse{Success: true}, nil
}

func validGroupExpiration(seconds int64) bool {
	return seconds == 0 || seconds == 86400 || seconds == 604800 || seconds == 7776000
}
