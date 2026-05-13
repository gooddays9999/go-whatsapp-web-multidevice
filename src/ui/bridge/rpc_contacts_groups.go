package bridge

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	domainGroup "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/group"
	domainUser "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/user"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

func (s *Service) GetContacts(ctx context.Context, req *bridgepb.GetContactsRequest) (*bridgepb.GetContactsResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	resp, err := s.deps.UserUsecase.MyListContacts(scoped)
	if err != nil {
		return nil, grpcError(err)
	}
	contacts := make([]*bridgepb.Contact, 0, len(resp.Data))
	for _, item := range resp.Data {
		contacts = append(contacts, &bridgepb.Contact{
			Jid:         item.JID.String(),
			Phone:       item.JID.User,
			Name:        item.Name,
			PushName:    item.Name,
			IsMyContact: true,
		})
	}
	return &bridgepb.GetContactsResponse{Contacts: contacts}, nil
}

func (s *Service) CheckNumber(ctx context.Context, req *bridgepb.CheckNumberRequest) (*bridgepb.CheckNumberResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	results := make(map[string]bool, len(req.GetPhoneNumbers()))
	for _, phone := range req.GetPhoneNumbers() {
		resp, err := s.deps.UserUsecase.IsOnWhatsApp(scoped, domainUser.CheckRequest{Phone: phone})
		results[phone] = err == nil && resp.IsOnWhatsApp
	}
	return &bridgepb.CheckNumberResponse{Results: results}, nil
}

func (s *Service) AddContact(ctx context.Context, req *bridgepb.AddContactRequest) (*bridgepb.AddContactResponse, error) {
	if req.GetAccountId() == "" || req.GetPhone() == "" {
		return nil, grpcError(fmt.Errorf("account_id and phone are required"))
	}
	if _, err := s.accountContext(ctx, req.GetAccountId()); err != nil {
		return nil, grpcError(err)
	}
	return &bridgepb.AddContactResponse{Success: true}, nil
}

func (s *Service) GetContactDetail(ctx context.Context, req *bridgepb.GetContactDetailRequest) (*bridgepb.GetContactDetailResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	info, _ := s.deps.UserUsecase.Info(scoped, domainUser.InfoRequest{Phone: req.GetPhone()})
	avatar, _ := s.deps.UserUsecase.Avatar(scoped, domainUser.AvatarRequest{Phone: req.GetPhone(), IsPreview: true})
	detail := &bridgepb.ContactDetail{
		Id:              req.GetPhone(),
		Number:          strings.TrimSuffix(strings.TrimSuffix(req.GetPhone(), "@s.whatsapp.net"), "@c.us"),
		FormattedNumber: req.GetPhone(),
		ProfilePicUrl:   avatar.URL,
		IsWaContact:     len(info.Data) > 0,
	}
	if len(info.Data) > 0 {
		detail.Name = info.Data[0].Name
		detail.PushName = info.Data[0].Name
		detail.VerifiedName = info.Data[0].VerifiedName
		detail.About = info.Data[0].Status
	}
	return &bridgepb.GetContactDetailResponse{Contact: detail}, nil
}

func (s *Service) SetProfilePicture(ctx context.Context, req *bridgepb.SetProfilePictureRequest) (*bridgepb.SetProfilePictureResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	inst, _ := whatsapp.DeviceFromContext(scoped)
	client := inst.GetClient()
	data, err := downloadBytes(req.GetImageUrl())
	if err != nil {
		return &bridgepb.SetProfilePictureResponse{Success: false, Error: err.Error()}, nil
	}
	if _, err := client.SetGroupPhoto(scoped, types.JID{}, data); err != nil {
		return &bridgepb.SetProfilePictureResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.SetProfilePictureResponse{Success: true}, nil
}

func (s *Service) SetStatus(ctx context.Context, req *bridgepb.SetStatusRequest) (*bridgepb.SetStatusResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	inst, _ := whatsapp.DeviceFromContext(scoped)
	if err := inst.GetClient().SetStatusMessage(scoped, req.GetStatusText()); err != nil {
		return &bridgepb.SetStatusResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.SetStatusResponse{Success: true}, nil
}

func (s *Service) SetDisplayName(ctx context.Context, req *bridgepb.SetDisplayNameRequest) (*bridgepb.SetDisplayNameResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.deps.UserUsecase.ChangePushName(scoped, domainUser.ChangePushNameRequest{PushName: req.GetDisplayName()}); err != nil {
		return &bridgepb.SetDisplayNameResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.SetDisplayNameResponse{Success: true}, nil
}

func (s *Service) GetGroups(ctx context.Context, req *bridgepb.GetGroupsRequest) (*bridgepb.GetGroupsResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	resp, err := s.deps.UserUsecase.MyListGroups(scoped)
	if err != nil {
		return nil, grpcError(err)
	}
	groups := make([]*bridgepb.Group, 0, len(resp.Data))
	for _, group := range resp.Data {
		groups = append(groups, &bridgepb.Group{
			Jid:              group.JID.String(),
			Name:             group.Name,
			Description:      group.Topic,
			ParticipantCount: int32(group.ParticipantCount),
			Owner:            group.OwnerJID.String(),
			CreatedAt:        group.GroupCreated.UnixMilli(),
		})
	}
	return &bridgepb.GetGroupsResponse{Groups: groups}, nil
}

func (s *Service) GetGroupMembers(ctx context.Context, req *bridgepb.GetGroupMembersRequest) (*bridgepb.GetGroupMembersResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	resp, err := s.deps.GroupUsecase.GetGroupParticipants(scoped, domainGroup.GetGroupParticipantsRequest{GroupID: req.GetGroupJid()})
	if err != nil {
		return nil, grpcError(err)
	}
	members := make([]*bridgepb.GroupMember, 0, len(resp.Participants))
	for _, item := range resp.Participants {
		members = append(members, &bridgepb.GroupMember{
			Jid:          item.JID,
			Phone:        item.PhoneNumber,
			Name:         item.DisplayName,
			IsAdmin:      item.IsAdmin,
			IsSuperAdmin: item.IsSuperAdmin,
		})
	}
	return &bridgepb.GetGroupMembersResponse{Members: members}, nil
}

func (s *Service) CreateGroup(ctx context.Context, req *bridgepb.CreateGroupRequest) (*bridgepb.CreateGroupResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	groupID, err := s.deps.GroupUsecase.CreateGroup(scoped, domainGroup.CreateGroupRequest{Title: req.GetName(), Participants: req.GetParticipants()})
	if err != nil {
		return &bridgepb.CreateGroupResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.CreateGroupResponse{Success: true, GroupJid: groupID}, nil
}

func (s *Service) UpdateGroup(ctx context.Context, req *bridgepb.UpdateGroupRequest) (*bridgepb.UpdateGroupResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	if req.GetName() != "" {
		if err := s.deps.GroupUsecase.SetGroupName(scoped, domainGroup.SetGroupNameRequest{GroupID: req.GetGroupJid(), Name: req.GetName()}); err != nil {
			return &bridgepb.UpdateGroupResponse{Success: false, Error: err.Error()}, nil
		}
	}
	if req.GetDescription() != "" {
		if err := s.deps.GroupUsecase.SetGroupTopic(scoped, domainGroup.SetGroupTopicRequest{GroupID: req.GetGroupJid(), Topic: req.GetDescription()}); err != nil {
			return &bridgepb.UpdateGroupResponse{Success: false, Error: err.Error()}, nil
		}
	}
	return &bridgepb.UpdateGroupResponse{Success: true}, nil
}

func (s *Service) AddGroupMembers(ctx context.Context, req *bridgepb.AddGroupMembersRequest) (*bridgepb.AddGroupMembersResponse, error) {
	added, failed, err := s.changeParticipants(ctx, req.GetAccountId(), req.GetGroupJid(), req.GetParticipants(), whatsmeow.ParticipantChangeAdd)
	if err != nil {
		return nil, grpcError(err)
	}
	return &bridgepb.AddGroupMembersResponse{Success: len(failed) == 0, Added: added, Failed: failed}, nil
}

func (s *Service) RemoveGroupMembers(ctx context.Context, req *bridgepb.RemoveGroupMembersRequest) (*bridgepb.RemoveGroupMembersResponse, error) {
	removed, failed, err := s.changeParticipants(ctx, req.GetAccountId(), req.GetGroupJid(), req.GetParticipants(), whatsmeow.ParticipantChangeRemove)
	if err != nil {
		return nil, grpcError(err)
	}
	return &bridgepb.RemoveGroupMembersResponse{Success: len(failed) == 0, Removed: removed, Failed: failed}, nil
}

func (s *Service) PromoteGroupMembers(ctx context.Context, req *bridgepb.PromoteGroupMembersRequest) (*bridgepb.PromoteGroupMembersResponse, error) {
	promoted, failed, err := s.changeParticipants(ctx, req.GetAccountId(), req.GetGroupJid(), req.GetParticipants(), whatsmeow.ParticipantChangePromote)
	if err != nil {
		return nil, grpcError(err)
	}
	return &bridgepb.PromoteGroupMembersResponse{Success: len(failed) == 0, Promoted: promoted, Failed: failed}, nil
}

func (s *Service) DemoteGroupMembers(ctx context.Context, req *bridgepb.DemoteGroupMembersRequest) (*bridgepb.DemoteGroupMembersResponse, error) {
	demoted, failed, err := s.changeParticipants(ctx, req.GetAccountId(), req.GetGroupJid(), req.GetParticipants(), whatsmeow.ParticipantChangeDemote)
	if err != nil {
		return nil, grpcError(err)
	}
	return &bridgepb.DemoteGroupMembersResponse{Success: len(failed) == 0, Demoted: demoted, Failed: failed}, nil
}

func (s *Service) LeaveGroup(ctx context.Context, req *bridgepb.LeaveGroupRequest) (*bridgepb.LeaveGroupResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.deps.GroupUsecase.LeaveGroup(scoped, domainGroup.LeaveGroupRequest{GroupID: req.GetGroupJid()}); err != nil {
		return &bridgepb.LeaveGroupResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.LeaveGroupResponse{Success: true}, nil
}

func (s *Service) SetGroupAdminsOnly(ctx context.Context, req *bridgepb.SetGroupAdminsOnlyRequest) (*bridgepb.SetGroupAdminsOnlyResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.deps.GroupUsecase.SetGroupAnnounce(scoped, domainGroup.SetGroupAnnounceRequest{GroupID: req.GetGroupJid(), Announce: req.GetAdminsOnly()}); err != nil {
		return &bridgepb.SetGroupAdminsOnlyResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.SetGroupAdminsOnlyResponse{Success: true}, nil
}

func (s *Service) JoinGroupByLink(ctx context.Context, req *bridgepb.JoinGroupByLinkRequest) (*bridgepb.JoinGroupByLinkResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	groupID, err := s.deps.GroupUsecase.JoinGroupWithLink(scoped, domainGroup.JoinGroupWithLinkRequest{Link: req.GetInviteLink()})
	if err != nil {
		return &bridgepb.JoinGroupByLinkResponse{Success: false, InviteLink: req.GetInviteLink(), Error: err.Error()}, nil
	}
	return &bridgepb.JoinGroupByLinkResponse{Success: true, InviteLink: req.GetInviteLink(), GroupId: groupID}, nil
}

func (s *Service) changeParticipants(ctx context.Context, accountID, groupJID string, participants []string, action whatsmeow.ParticipantChange) ([]string, []string, error) {
	scoped, err := s.accountContext(ctx, accountID)
	if err != nil {
		return nil, nil, err
	}
	result, err := s.deps.GroupUsecase.ManageParticipant(scoped, domainGroup.ParticipantRequest{GroupID: groupJID, Participants: participants, Action: action})
	if err != nil {
		return nil, nil, err
	}
	ok := make([]string, 0, len(result))
	failed := make([]string, 0)
	for _, item := range result {
		if item.Status == "success" {
			ok = append(ok, item.Participant)
		} else {
			failed = append(failed, item.Participant)
		}
	}
	return ok, failed, nil
}

func downloadBytes(rawURL string) ([]byte, error) {
	if rawURL == "" {
		return nil, fmt.Errorf("image_url is required")
	}
	resp, err := http.Get(rawURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("download failed: HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 50*1024*1024))
}
