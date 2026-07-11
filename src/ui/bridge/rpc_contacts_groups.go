package bridge

import (
	"bytes"
	"context"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"net/http"
	"strings"

	domainGroup "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/group"
	domainUser "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/user"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/utils"
	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	"github.com/disintegration/imaging"
	"github.com/sirupsen/logrus"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	_ "golang.org/x/image/webp"
	"google.golang.org/protobuf/proto"
)

const (
	profilePictureMaxDimension = 640
	profilePictureMaxBytes     = 100 * 1024
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
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	inst, _ := whatsapp.DeviceFromContext(scoped)
	if inst == nil || inst.GetClient() == nil {
		return nil, grpcError(fmt.Errorf("account not connected"))
	}
	number := legacyContactNumber(req.GetPhone())
	if number == "" {
		return nil, grpcError(fmt.Errorf("valid phone is required"))
	}
	jid := types.NewJID(number, types.DefaultUserServer)
	firstName, fullName := addContactNames(number, req.GetFirstName(), req.GetLastName())
	client := inst.GetClient()
	lidJID := resolveContactLID(scoped, client, jid)
	logFields := logrus.Fields{
		"account_id": req.GetAccountId(),
		"phone":      number,
		"jid":        jid.String(),
		"lid_jid":    lidJID.String(),
		"first_name": firstName,
		"full_name":  fullName,
	}
	logrus.WithFields(logFields).Info("bridge AddContact app state started")
	if err := client.SendAppState(scoped, buildAddContactPatch(jid, lidJID, firstName, fullName)); err != nil {
		logrus.WithError(err).WithFields(logFields).Warn("bridge AddContact app state failed")
		return &bridgepb.AddContactResponse{Success: false, Error: err.Error()}, nil
	}
	if client.Store != nil && client.Store.Contacts != nil {
		if err := client.Store.Contacts.PutContactName(scoped, jid, firstName, fullName); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"account_id": req.GetAccountId(),
				"jid":        jid.String(),
			}).Warn("failed to update local contact cache after AddContact")
		}
		if !lidJID.IsEmpty() {
			if err := client.Store.Contacts.PutContactName(scoped, lidJID, firstName, fullName); err != nil {
				logrus.WithError(err).WithFields(logrus.Fields{
					"account_id": req.GetAccountId(),
					"jid":        lidJID.String(),
				}).Warn("failed to update local LID contact cache after AddContact")
			}
		}
	}
	logrus.WithFields(logFields).Info("bridge AddContact app state completed")
	return &bridgepb.AddContactResponse{Success: true}, nil
}

func (s *Service) GetContactDetail(ctx context.Context, req *bridgepb.GetContactDetailRequest) (*bridgepb.GetContactDetailResponse, error) {
	if req.GetAccountId() == "" || req.GetPhone() == "" {
		return nil, grpcError(fmt.Errorf("account_id and phone are required"))
	}
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	legacyID := legacyContactChatID(req.GetPhone())
	number := legacyContactNumber(req.GetPhone())
	jid := types.NewJID(number, types.DefaultUserServer)

	var contactName, pushName, verifiedName string
	var isMyContact bool
	var canQueryRemote bool
	if inst, ok := whatsapp.DeviceFromContext(scoped); ok && inst != nil {
		canQueryRemote = cachedLoggedIn(inst.Snapshot().State)
		if client := inst.GetClient(); client != nil {
			if client.Store != nil && client.Store.Contacts != nil && !jid.IsEmpty() {
				if contact, err := client.Store.Contacts.GetContact(scoped, jid); err == nil && contact.Found {
					isMyContact = true
					contactName = firstNonEmpty(contact.FullName, contact.FirstName)
					pushName = contact.PushName
					verifiedName = contact.BusinessName
				}
			}
		}
	}

	profilePicURL := ""
	isWaContact := isMyContact
	if canQueryRemote {
		info, _ := s.deps.UserUsecase.Info(scoped, domainUser.InfoRequest{Phone: req.GetPhone()})
		avatar, _ := s.deps.UserUsecase.Avatar(scoped, domainUser.AvatarRequest{Phone: req.GetPhone(), IsPreview: true})
		profilePicURL = avatar.URL
		if len(info.Data) > 0 {
			isWaContact = true
			contactName = firstNonEmpty(contactName, info.Data[0].Name)
			pushName = firstNonEmpty(pushName, info.Data[0].Name)
			verifiedName = firstNonEmpty(verifiedName, info.Data[0].VerifiedName)
		}
	}
	detail := &bridgepb.ContactDetail{
		Id:              legacyID,
		Number:          number,
		FormattedNumber: number,
		Name:            contactName,
		PushName:        pushName,
		VerifiedName:    verifiedName,
		ProfilePicUrl:   profilePicURL,
		IsMyContact:     isMyContact,
		IsWaContact:     isWaContact,
	}
	return &bridgepb.GetContactDetailResponse{Contact: detail}, nil
}

func legacyContactChatID(phone string) string {
	trimmed := strings.TrimSpace(phone)
	if strings.Contains(trimmed, "@s.whatsapp.net") {
		return strings.Replace(trimmed, "@s.whatsapp.net", "@c.us", 1)
	}
	if strings.Contains(trimmed, "@") {
		return trimmed
	}
	return legacyContactNumber(trimmed) + "@c.us"
}

func legacyContactNumber(phone string) string {
	trimmed := strings.TrimSpace(phone)
	if idx := strings.Index(trimmed, "@"); idx >= 0 {
		return trimmed[:idx]
	}
	var b strings.Builder
	for _, r := range trimmed {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func addContactNames(number, firstName, lastName string) (string, string) {
	firstName = strings.TrimSpace(firstName)
	lastName = strings.TrimSpace(lastName)
	if lastName == "." {
		lastName = ""
	}
	if firstName == "" {
		if len(number) > 4 {
			firstName = number[len(number)-4:]
		} else {
			firstName = number
		}
	}
	fullName := firstName
	if lastName != "" {
		fullName = strings.TrimSpace(firstName + " " + lastName)
	}
	return firstName, fullName
}

func resolveContactLID(ctx context.Context, client *whatsmeow.Client, pnJID types.JID) types.JID {
	if client == nil || client.Store == nil || client.Store.LIDs == nil || pnJID.IsEmpty() || pnJID.Server != types.DefaultUserServer {
		return types.EmptyJID
	}
	lidJID, err := client.Store.LIDs.GetLIDForPN(ctx, pnJID)
	if err == nil && !lidJID.IsEmpty() && lidJID.Server == types.HiddenUserServer {
		return lidJID
	}
	info, err := client.GetUserInfo(ctx, []types.JID{pnJID})
	if err != nil {
		logrus.WithError(err).WithField("jid", pnJID.String()).Warn("failed to resolve contact LID for AddContact")
		return types.EmptyJID
	}
	if userInfo, ok := info[pnJID]; ok && !userInfo.LID.IsEmpty() && userInfo.LID.Server == types.HiddenUserServer {
		return userInfo.LID
	}
	return types.EmptyJID
}

func buildAddContactPatch(pnJID, lidJID types.JID, firstName, fullName string) appstate.PatchInfo {
	pnJIDString := pnJID.String()
	contactAction := &waSyncAction.ContactAction{
		FirstName:                proto.String(firstName),
		FullName:                 proto.String(fullName),
		PnJID:                    proto.String(pnJIDString),
		SaveOnPrimaryAddressbook: proto.Bool(true),
	}
	if !lidJID.IsEmpty() && lidJID.Server == types.HiddenUserServer {
		contactAction.LidJID = proto.String(lidJID.String())
	}
	mutations := []appstate.MutationInfo{{
		Index:   []string{appstate.IndexContact, pnJIDString},
		Version: 2,
		Value: &waSyncAction.SyncActionValue{
			ContactAction: contactAction,
		},
	}}
	if !lidJID.IsEmpty() && lidJID.Server == types.HiddenUserServer {
		mutations = append(mutations, appstate.MutationInfo{
			Index:   []string{appstate.IndexLIDContact, lidJID.String()},
			Version: 2,
			Value: &waSyncAction.SyncActionValue{
				LidContactAction: &waSyncAction.LidContactAction{
					FirstName: proto.String(firstName),
					FullName:  proto.String(fullName),
				},
			},
		})
	}
	return appstate.PatchInfo{
		Type:      appstate.WAPatchCriticalUnblockLow,
		Mutations: mutations,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func shouldApplyUpdateGroupField(explicit bool, value string) bool {
	return explicit || value != ""
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
	photo, err := prepareProfilePictureJPEG(data)
	if err != nil {
		return &bridgepb.SetProfilePictureResponse{Success: false, Error: err.Error()}, nil
	}
	pictureID, err := client.SetGroupPhoto(scoped, types.JID{}, photo)
	if err != nil {
		return &bridgepb.SetProfilePictureResponse{Success: false, Error: err.Error()}, nil
	}
	if pictureID == "" {
		return &bridgepb.SetProfilePictureResponse{Success: false, Error: "empty picture id from WhatsApp"}, nil
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
	if inst, ok := whatsapp.DeviceFromContext(scoped); ok && inst != nil {
		inst.UpdateStateFromClient()
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
	if shouldApplyUpdateGroupField(req.GetUpdateName(), req.GetName()) {
		if err := s.deps.GroupUsecase.SetGroupName(scoped, domainGroup.SetGroupNameRequest{GroupID: req.GetGroupJid(), Name: req.GetName()}); err != nil {
			return &bridgepb.UpdateGroupResponse{Success: false, Error: err.Error()}, nil
		}
	}
	if shouldApplyUpdateGroupField(req.GetUpdateDescription(), req.GetDescription()) {
		if err := s.deps.GroupUsecase.SetGroupTopic(scoped, domainGroup.SetGroupTopicRequest{GroupID: req.GetGroupJid(), Topic: req.GetDescription()}); err != nil {
			return &bridgepb.UpdateGroupResponse{Success: false, Error: err.Error()}, nil
		}
	}
	if shouldApplyUpdateGroupField(req.GetUpdateAvatar(), req.GetAvatarUrl()) {
		if err := setGroupPhotoFromURL(scoped, req.GetGroupJid(), req.GetAvatarUrl()); err != nil {
			return &bridgepb.UpdateGroupResponse{Success: false, Error: err.Error()}, nil
		}
	}
	return &bridgepb.UpdateGroupResponse{Success: true}, nil
}

func setGroupPhotoFromURL(ctx context.Context, groupJID, avatarURL string) error {
	inst, ok := whatsapp.DeviceFromContext(ctx)
	if !ok || inst == nil || inst.GetClient() == nil {
		return fmt.Errorf("account not connected")
	}
	client := inst.GetClient()
	parsedGroupJID, err := utils.ValidateJidWithLogin(client, groupJID)
	if err != nil {
		return err
	}

	var photo []byte
	if strings.TrimSpace(avatarURL) != "" {
		data, err := downloadBytes(avatarURL)
		if err != nil {
			return err
		}
		photo, err = prepareProfilePictureJPEG(data)
		if err != nil {
			return err
		}
	}

	pictureID, err := client.SetGroupPhoto(ctx, parsedGroupJID, photo)
	if err != nil {
		return err
	}
	if photo != nil && pictureID == "" {
		return fmt.Errorf("empty picture id from WhatsApp")
	}
	return nil
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

func (s *Service) SetGroupInfoAdminsOnly(ctx context.Context, req *bridgepb.SetGroupInfoAdminsOnlyRequest) (*bridgepb.SetGroupInfoAdminsOnlyResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.deps.GroupUsecase.SetGroupLocked(scoped, domainGroup.SetGroupLockedRequest{GroupID: req.GetGroupJid(), Locked: req.GetAdminsOnly()}); err != nil {
		return &bridgepb.SetGroupInfoAdminsOnlyResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.SetGroupInfoAdminsOnlyResponse{Success: true}, nil
}

func (s *Service) SetGroupAddMembersAdminsOnly(ctx context.Context, req *bridgepb.SetGroupAddMembersAdminsOnlyRequest) (*bridgepb.SetGroupAddMembersAdminsOnlyResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	if err := s.deps.GroupUsecase.SetGroupMemberAddMode(scoped, domainGroup.SetGroupMemberAddModeRequest{GroupID: req.GetGroupJid(), AdminsOnly: req.GetAdminsOnly()}); err != nil {
		return &bridgepb.SetGroupAddMembersAdminsOnlyResponse{Success: false, Error: err.Error()}, nil
	}
	return &bridgepb.SetGroupAddMembersAdminsOnlyResponse{Success: true}, nil
}

func (s *Service) GetGroupInfoFromLink(ctx context.Context, req *bridgepb.GetGroupInfoFromLinkRequest) (*bridgepb.GetGroupInfoFromLinkResponse, error) {
	scoped, err := s.accountContext(ctx, req.GetAccountId())
	if err != nil {
		return nil, grpcError(err)
	}
	info, err := s.deps.GroupUsecase.GetGroupInfoFromLink(scoped, domainGroup.GetGroupInfoFromLinkRequest{Link: req.GetInviteLink()})
	if err != nil {
		return &bridgepb.GetGroupInfoFromLinkResponse{Success: false, InviteLink: req.GetInviteLink(), Error: err.Error()}, nil
	}
	return groupInfoFromLinkToProto(req.GetInviteLink(), info), nil
}

func groupInfoFromLinkToProto(inviteLink string, info domainGroup.GetGroupInfoFromLinkResponse) *bridgepb.GetGroupInfoFromLinkResponse {
	createdAt := int64(0)
	if !info.CreatedAt.IsZero() {
		createdAt = info.CreatedAt.Unix()
	}
	return &bridgepb.GetGroupInfoFromLinkResponse{
		Success:              true,
		InviteLink:           inviteLink,
		GroupId:              info.GroupID,
		GroupName:            strings.TrimSpace(info.Name),
		Topic:                info.Topic,
		Description:          info.Description,
		CreatedAt:            createdAt,
		ParticipantCount:     int32(info.ParticipantCount),
		IsLocked:             info.IsLocked,
		IsAnnounce:           info.IsAnnounce,
		IsEphemeral:          info.IsEphemeral,
		JoinApprovalRequired: info.JoinApprovalRequired,
	}
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
	groupName := ""
	if groupID != "" && s.deps.UserUsecase != nil {
		if groups, err := s.deps.UserUsecase.MyListGroups(scoped); err == nil {
			groupName = findJoinedGroupNameByJID(groups.Data, groupID)
		} else {
			logrus.WithError(err).WithField("group_jid", groupID).Debug("failed to resolve joined group name")
		}
	}
	return &bridgepb.JoinGroupByLinkResponse{Success: true, InviteLink: req.GetInviteLink(), GroupId: groupID, GroupName: groupName}, nil
}

func findJoinedGroupNameByJID(groups []types.GroupInfo, groupJID string) string {
	for _, group := range groups {
		if group.JID.String() == groupJID {
			return strings.TrimSpace(group.Name)
		}
	}
	return ""
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

func prepareProfilePictureJPEG(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty image")
	}
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("decode image: %w", err)
	}
	img = cropProfilePicture(img)
	if img.Bounds().Dx() > profilePictureMaxDimension || img.Bounds().Dy() > profilePictureMaxDimension {
		img = imaging.Resize(img, profilePictureMaxDimension, profilePictureMaxDimension, imaging.Lanczos)
	}
	return encodeProfilePictureJPEG(img, 80, 0)
}

func cropProfilePicture(img image.Image) image.Image {
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	if width == height {
		return img
	}
	size := width
	if height < size {
		size = height
	}
	left := bounds.Min.X + (width-size)/2
	top := bounds.Min.Y + (height-size)/2
	return imaging.Crop(img, image.Rect(left, top, left+size, top+size))
}

func encodeProfilePictureJPEG(img image.Image, quality, depth int) ([]byte, error) {
	if depth > 12 {
		return nil, fmt.Errorf("image cannot be compressed enough to meet WhatsApp requirements")
	}
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: quality}); err != nil {
		return nil, fmt.Errorf("encode JPEG: %w", err)
	}
	if buf.Len() <= profilePictureMaxBytes {
		return buf.Bytes(), nil
	}
	if quality > 30 {
		return encodeProfilePictureJPEG(img, quality-10, depth+1)
	}
	bounds := img.Bounds()
	nextSize := int(float64(bounds.Dx()) * 0.85)
	if nextSize < 96 {
		return nil, fmt.Errorf("image cannot be compressed below %d bytes", profilePictureMaxBytes)
	}
	return encodeProfilePictureJPEG(imaging.Resize(img, nextSize, nextSize, imaging.Lanczos), 80, depth+1)
}
