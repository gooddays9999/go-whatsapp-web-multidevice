package usecase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"strings"
	"sync"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	domainUser "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/user"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	pkgError "github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/error"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/utils"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/validations"
	"github.com/disintegration/imaging"
	"github.com/sirupsen/logrus"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

type serviceUser struct {
	chatStorageRepo domainChatStorage.IChatStorageRepository
}

func NewUserService(chatStorageRepo domainChatStorage.IChatStorageRepository) domainUser.IUserUsecase {
	return &serviceUser{
		chatStorageRepo: chatStorageRepo,
	}
}

func (service serviceUser) Info(ctx context.Context, request domainUser.InfoRequest) (response domainUser.InfoResponse, err error) {
	err = validations.ValidateUserInfo(ctx, request)
	if err != nil {
		return response, err
	}

	client := whatsapp.ClientFromContext(ctx)
	if client == nil {
		return response, pkgError.ErrWaCLI
	}

	var jids []types.JID
	dataWaRecipient, err := utils.ValidateJidWithLogin(client, request.Phone)
	if err != nil {
		return response, err
	}

	// Parse original input to check if it was a LID
	originalJID, _ := utils.ParseJID(request.Phone)
	wasLID := originalJID.Server == "lid"

	// If input was LID and resolved to phone, include resolved phone
	if wasLID && dataWaRecipient.Server == types.DefaultUserServer {
		response.ResolvedPhone = dataWaRecipient.User
	}

	// If input was phone number, try to get corresponding LID
	if dataWaRecipient.Server == types.DefaultUserServer {
		lid := utils.ResolvePhoneToLID(ctx, dataWaRecipient, client)
		if !lid.IsEmpty() {
			response.ResolvedLID = lid.String()
		}
	}

	jids = append(jids, dataWaRecipient)
	resp, err := client.GetUserInfo(ctx, jids)
	if err != nil {
		return response, err
	}

	// Get device ID for scoped storage lookup
	deviceID := ""
	if inst, ok := whatsapp.DeviceFromContext(ctx); ok && inst != nil {
		deviceID = inst.JID()
		if deviceID == "" {
			deviceID = inst.ID()
		}
	}
	if deviceID == "" && client.Store != nil && client.Store.ID != nil {
		deviceID = client.Store.ID.ToNonAD().String()
	}

	for jid, userInfo := range resp {
		var device []domainUser.InfoResponseDataDevice
		for _, j := range userInfo.Devices {
			device = append(device, domainUser.InfoResponseDataDevice{
				User:   j.User,
				Agent:  j.RawAgent,
				Device: utils.GetPlatformName(int(j.Device)),
				Server: j.Server,
				AD:     j.ADString(),
			})
		}

		data := domainUser.InfoResponseData{
			Status:    userInfo.Status,
			PictureID: userInfo.PictureID,
			Devices:   device,
		}

		// Try to get name from storage if available (device-scoped to prevent data leak)
		if service.chatStorageRepo != nil && deviceID != "" {
			if chat, err := service.chatStorageRepo.GetChatByDevice(deviceID, jid.String()); err == nil && chat != nil {
				data.Name = chat.Name
			} else if err != nil {
				logrus.Debugf("Could not fetch chat name from storage for %s: %v", jid.String(), err)
			}
		}

		if userInfo.VerifiedName != nil {
			data.VerifiedName = fmt.Sprintf("%v", *userInfo.VerifiedName)
		}
		response.Data = append(response.Data, data)
	}

	return response, nil
}

func (service serviceUser) Avatar(ctx context.Context, request domainUser.AvatarRequest) (response domainUser.AvatarResponse, err error) {
	client := whatsapp.ClientFromContext(ctx)
	if client == nil {
		return response, pkgError.ErrWaCLI
	}

	err = validations.ValidateUserAvatar(ctx, request)
	if err != nil {
		return response, err
	}

	dataWaRecipient, err := utils.ValidateJidWithLogin(client, request.Phone)
	if err != nil {
		return response, err
	}

	// IsCommunity should only be true for group JIDs (communities)
	// For regular user JIDs (@s.whatsapp.net), force IsCommunity to false to prevent timeout
	isCommunity := request.IsCommunity
	if dataWaRecipient.Server == types.DefaultUserServer {
		isCommunity = false
	}

	avatarCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	pic, err := client.GetProfilePictureInfo(avatarCtx, dataWaRecipient, &whatsmeow.GetProfilePictureParams{
		Preview:     request.IsPreview,
		IsCommunity: isCommunity,
	})
	if err != nil {
		if avatarCtx.Err() == context.DeadlineExceeded {
			return response, pkgError.ContextError("Error timeout get avatar!")
		}
		// If is_community=true failed, retry with is_community=false as fallback
		if isCommunity {
			avatarCtx2, cancel2 := context.WithTimeout(ctx, 5*time.Second)
			defer cancel2()

			pic, err = client.GetProfilePictureInfo(avatarCtx2, dataWaRecipient, &whatsmeow.GetProfilePictureParams{
				Preview:     request.IsPreview,
				IsCommunity: false,
			})
			if err != nil {
				if avatarCtx2.Err() == context.DeadlineExceeded {
					return response, pkgError.ContextError("Error timeout get avatar!")
				}
				return response, err
			}
		} else {
			return response, err
		}
	}

	if pic == nil {
		return response, errors.New("no avatar found")
	}

	response.URL = pic.URL
	response.ID = pic.ID
	response.Type = pic.Type
	return response, nil
}

// MyListGroups returns all groups the user has joined.
//
// ⚠️ KNOWN LIMITATION: This endpoint returns a maximum of 500 groups due to a WhatsApp protocol limitation.
// The underlying whatsmeow library's GetJoinedGroups() function sends a single "participating" IQ query
// to WhatsApp servers, which enforces this limit server-side. This is not a bug - it's a constraint
// imposed by WhatsApp's multi-device protocol. Pagination is not supported by WhatsApp for this query.
//
// For more details, see: https://github.com/tulir/whatsmeow/blob/main/group.go
// Related issue: https://github.com/aldinokemal/go-whatsapp-web-multidevice/issues/553
func (service serviceUser) MyListGroups(ctx context.Context) (response domainUser.MyListGroupsResponse, err error) {
	client := whatsapp.ClientFromContext(ctx)
	if client == nil {
		return response, pkgError.ErrWaCLI
	}
	utils.MustLogin(client)

	groups, err := client.GetJoinedGroups(ctx)
	if err != nil {
		return
	}

	for _, group := range groups {
		response.Data = append(response.Data, *group)
	}
	return response, nil
}

func (service serviceUser) MyListNewsletter(ctx context.Context) (response domainUser.MyListNewsletterResponse, err error) {
	client := whatsapp.ClientFromContext(ctx)
	if client == nil {
		return response, pkgError.ErrWaCLI
	}
	utils.MustLogin(client)

	datas, err := client.GetSubscribedNewsletters(ctx)
	if err != nil {
		return
	}

	// GetSubscribedNewsletters may return incomplete metadata from WhatsApp,
	// especially subscribers_count. Enrich entries that are missing the count via
	// GetNewsletterInfo with bounded parallelism so we don't fan out unboundedly
	// to the WhatsApp socket, and a per-call timeout so one slow newsletter
	// cannot stall the whole request.
	const (
		newsletterDetailParallelism = 5
		newsletterDetailTimeout     = 5 * time.Second
	)

	sem := make(chan struct{}, newsletterDetailParallelism)
	var wg sync.WaitGroup
	for _, data := range datas {
		if data == nil {
			continue
		}
		// Skip the detail fetch when the base response already populated the count.
		if data.ThreadMeta.SubscriberCount > 0 {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(d *types.NewsletterMetadata) {
			defer wg.Done()
			defer func() { <-sem }()

			detailCtx, cancel := context.WithTimeout(ctx, newsletterDetailTimeout)
			defer cancel()

			detail, detailErr := client.GetNewsletterInfo(detailCtx, d.ID)
			if detailErr != nil {
				logrus.Debugf("Could not fetch newsletter detail for %s: %v", d.ID.String(), detailErr)
				return
			}
			if detail != nil {
				d.ThreadMeta.SubscriberCount = detail.ThreadMeta.SubscriberCount
			}
		}(data)
	}
	wg.Wait()

	for _, data := range datas {
		if data == nil {
			continue
		}
		response.Data = append(response.Data, *data)
	}
	return response, nil
}

func (service serviceUser) MyPrivacySetting(ctx context.Context) (response domainUser.MyPrivacySettingResponse, err error) {
	client := whatsapp.ClientFromContext(ctx)
	if client == nil {
		return response, pkgError.ErrWaCLI
	}
	utils.MustLogin(client)

	resp, err := client.TryFetchPrivacySettings(ctx, true)
	if err != nil {
		return
	}

	response.GroupAdd = string(resp.GroupAdd)
	response.Status = string(resp.Status)
	response.ReadReceipts = string(resp.ReadReceipts)
	response.Profile = string(resp.Profile)
	return response, nil
}

func (service serviceUser) MyListContacts(ctx context.Context) (response domainUser.MyListContactsResponse, err error) {
	client := whatsapp.ClientFromContext(ctx)
	if client == nil {
		return response, pkgError.ErrWaCLI
	}
	utils.MustLogin(client)

	contacts, err := client.Store.Contacts.GetAllContacts(ctx)
	if err != nil {
		return
	}

	for jid, contact := range contacts {
		response.Data = append(response.Data, domainUser.MyListContactsResponseData{
			JID:  jid,
			Name: contactInfoDisplayName(contact),
		})
	}

	return response, nil
}

func (service serviceUser) AddContact(ctx context.Context, request domainUser.AddContactRequest) (response domainUser.AddContactResponse, err error) {
	if err = validations.ValidateAddContact(ctx, request); err != nil {
		return response, err
	}

	client := whatsapp.ClientFromContext(ctx)
	if client == nil {
		return response, pkgError.ErrWaCLI
	}

	number := contactNumber(request.Phone)
	if number == "" {
		return response, pkgError.ValidationError("phone: cannot be blank.")
	}

	jid := types.NewJID(number, types.DefaultUserServer)
	if _, err = utils.ValidateJidWithLogin(client, jid.String()); err != nil {
		return response, err
	}

	firstName, fullName := addContactNames(number, requestFirstName(request), requestLastName(request))
	if request.FullName != "" {
		fullName = strings.TrimSpace(request.FullName)
	} else if request.ContactName != "" {
		fullName = strings.TrimSpace(request.ContactName)
	}
	if fullName == "" {
		fullName = firstName
	}

	lidJID := resolveContactLID(ctx, client, jid)
	logFields := logrus.Fields{
		"phone":      number,
		"jid":        jid.String(),
		"lid_jid":    lidJID.String(),
		"first_name": firstName,
		"full_name":  fullName,
	}
	logrus.WithFields(logFields).Info("AddContact app state started")
	if err = client.SendAppState(ctx, buildAddContactPatch(jid, lidJID, firstName, fullName, true)); err != nil {
		logrus.WithError(err).WithFields(logFields).Warn("AddContact app state failed")
		return response, err
	}

	if client.Store != nil && client.Store.Contacts != nil {
		if errCache := client.Store.Contacts.PutContactName(ctx, jid, firstName, fullName); errCache != nil {
			logrus.WithError(errCache).WithField("jid", jid.String()).Warn("failed to update local contact cache after AddContact")
		}
		if !lidJID.IsEmpty() {
			if errCache := client.Store.Contacts.PutContactName(ctx, lidJID, firstName, fullName); errCache != nil {
				logrus.WithError(errCache).WithField("jid", lidJID.String()).Warn("failed to update local LID contact cache after AddContact")
			}
		}
	}

	response.Success = true
	response.JID = jid.String()
	response.LIDJID = lidJID.String()
	response.FirstName = firstName
	response.FullName = fullName
	logrus.WithFields(logFields).Info("AddContact app state completed")
	return response, nil
}

func (service serviceUser) ChangeAvatar(ctx context.Context, request domainUser.ChangeAvatarRequest) (err error) {
	client := whatsapp.ClientFromContext(ctx)
	if client == nil {
		return pkgError.ErrWaCLI
	}
	utils.MustLogin(client)

	file, err := request.Avatar.Open()
	if err != nil {
		return err
	}
	defer file.Close()

	// Read original image
	srcImage, err := imaging.Decode(file)
	if err != nil {
		return fmt.Errorf("failed to decode image: %v", err)
	}

	// Get original dimensions
	bounds := srcImage.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()

	// Calculate new dimensions for 1:1 aspect ratio
	size := width
	if height < width {
		size = height
	}
	if size > 640 {
		size = 640
	}

	// Create a square crop from the center
	left := (width - size) / 2
	top := (height - size) / 2
	croppedImage := imaging.Crop(srcImage, image.Rect(left, top, left+size, top+size))

	// Resize if needed
	if size > 640 {
		croppedImage = imaging.Resize(croppedImage, 640, 640, imaging.Lanczos)
	}

	// Convert to bytes
	var buf bytes.Buffer
	err = imaging.Encode(&buf, croppedImage, imaging.JPEG, imaging.JPEGQuality(80))
	if err != nil {
		return fmt.Errorf("failed to encode image: %v", err)
	}

	_, err = client.SetGroupPhoto(ctx, types.JID{}, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (service serviceUser) ChangePushName(ctx context.Context, request domainUser.ChangePushNameRequest) (err error) {
	client := whatsapp.ClientFromContext(ctx)
	if client == nil {
		return pkgError.ErrWaCLI
	}
	utils.MustLogin(client)

	pushName := strings.TrimSpace(request.PushName)
	if pushName == "" {
		return fmt.Errorf("push_name is required")
	}

	err = client.SendAppState(ctx, appstate.BuildSettingPushName(pushName))
	if err != nil {
		return err
	}
	client.Store.PushName = pushName
	if err = client.Store.Save(ctx); err != nil {
		return fmt.Errorf("save push name: %w", err)
	}
	if err = client.SendPresence(ctx, types.PresenceAvailable); err != nil {
		return fmt.Errorf("send presence with updated push name: %w", err)
	}
	return nil
}

func (service serviceUser) IsOnWhatsApp(ctx context.Context, request domainUser.CheckRequest) (response domainUser.CheckResponse, err error) {
	client := whatsapp.ClientFromContext(ctx)
	if client == nil {
		return response, pkgError.ErrWaCLI
	}
	utils.MustLogin(client)

	utils.SanitizePhone(&request.Phone)

	response.IsOnWhatsApp = utils.IsOnWhatsapp(client, request.Phone)

	return response, nil
}

func requestFirstName(request domainUser.AddContactRequest) string {
	return firstNonEmpty(request.FirstName, request.FirstNameCamel, request.FirstNameTypo)
}

func requestLastName(request domainUser.AddContactRequest) string {
	return firstNonEmpty(request.LastName, request.LastNameCamel)
}

func contactNumber(phone string) string {
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

func buildAddContactPatch(pnJID, lidJID types.JID, firstName, fullName string, saveOnPrimaryAddressbook bool) appstate.PatchInfo {
	pnJIDString := pnJID.String()
	contactAction := &waSyncAction.ContactAction{
		FirstName:                proto.String(firstName),
		FullName:                 proto.String(fullName),
		PnJID:                    proto.String(pnJIDString),
		SaveOnPrimaryAddressbook: proto.Bool(saveOnPrimaryAddressbook),
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

func (service serviceUser) BusinessProfile(ctx context.Context, request domainUser.BusinessProfileRequest) (response domainUser.BusinessProfileResponse, err error) {
	err = validations.ValidateBusinessProfile(ctx, request)
	if err != nil {
		return response, err
	}

	client := whatsapp.ClientFromContext(ctx)
	if client == nil {
		return response, pkgError.ErrWaCLI
	}

	dataWaRecipient, err := utils.ValidateJidWithLogin(client, request.Phone)
	if err != nil {
		return response, err
	}

	profile, err := client.GetBusinessProfile(ctx, dataWaRecipient)
	if err != nil {
		return response, err
	}

	// Convert profile to response format
	response.JID = dataWaRecipient.String()
	response.Email = profile.Email
	response.Address = profile.Address

	// Convert categories
	for _, category := range profile.Categories {
		response.Categories = append(response.Categories, domainUser.BusinessProfileCategory{
			ID:   category.ID,
			Name: category.Name,
		})
	}

	// Convert profile options
	if profile.ProfileOptions != nil {
		response.ProfileOptions = make(map[string]string)
		for key, value := range profile.ProfileOptions {
			response.ProfileOptions[key] = value
		}
	}

	response.BusinessHoursTimeZone = profile.BusinessHoursTimeZone

	// Convert business hours
	for _, hours := range profile.BusinessHours {
		response.BusinessHours = append(response.BusinessHours, domainUser.BusinessProfileHoursConfig{
			DayOfWeek: hours.DayOfWeek,
			Mode:      hours.Mode,
			OpenTime:  utils.FormatBusinessHourTime(hours.OpenTime),
			CloseTime: utils.FormatBusinessHourTime(hours.CloseTime),
		})
	}

	return response, nil
}
