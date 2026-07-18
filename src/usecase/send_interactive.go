package usecase

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	domainSend "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/send"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	pkgError "github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/error"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/utils"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// parseInteractiveMessage decodes a WhatsApp InteractiveMessage from its
// protobuf-JSON form, e.g. {"body":{"text":"..."},"nativeFlowMessage":
// {"buttons":[{"name":"cta_url","buttonParamsJSON":"..."}]}}. DiscardUnknown
// keeps callers from breaking on envelope fields (messageId, subType, ...).
func parseInteractiveMessage(protoJSON string) (*waE2E.InteractiveMessage, error) {
	if strings.TrimSpace(protoJSON) == "" {
		return nil, fmt.Errorf("proto_json is required")
	}
	msg := &waE2E.InteractiveMessage{}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal([]byte(protoJSON), msg); err != nil {
		return nil, fmt.Errorf("invalid interactive proto_json: %w", err)
	}
	return msg, nil
}

// SendInteractive sends a native_flow interactive message (e.g. a cta_url
// button), optionally with an image header downloaded from ImageURL.
func (service serviceSend) SendInteractive(ctx context.Context, request domainSend.InteractiveRequest) (response domainSend.GenericResponse, err error) {
	client := whatsapp.ClientFromContext(ctx)
	if client == nil {
		return response, pkgError.ErrWaCLI
	}

	dataWaRecipient, err := utils.ValidateJidWithLogin(client, request.BaseRequest.Phone)
	if err != nil {
		return response, err
	}

	interactive, err := parseInteractiveMessage(request.ProtoJSON)
	if err != nil {
		return response, pkgError.InternalServerError(err.Error())
	}

	// Optional image header: download and upload, then attach as the header
	// media (the URL alone is not enough — WhatsApp needs the encrypted upload).
	if strings.TrimSpace(request.ImageURL) != "" {
		imageData, _, dErr := utils.DownloadImageFromURL(request.ImageURL)
		if dErr != nil {
			return response, pkgError.InternalServerError(fmt.Sprintf("failed to download header image: %v", dErr))
		}
		uploaded, uErr := service.uploadMedia(ctx, client, whatsmeow.MediaImage, imageData, dataWaRecipient)
		if uErr != nil {
			return response, pkgError.InternalServerError(fmt.Sprintf("failed to upload header image: %v", uErr))
		}
		interactive.Header = &waE2E.InteractiveMessage_Header{
			Media: &waE2E.InteractiveMessage_Header_ImageMessage{
				ImageMessage: &waE2E.ImageMessage{
					URL:           proto.String(uploaded.URL),
					DirectPath:    proto.String(uploaded.DirectPath),
					MediaKey:      uploaded.MediaKey,
					Mimetype:      proto.String(http.DetectContentType(imageData)),
					FileEncSHA256: uploaded.FileEncSHA256,
					FileSHA256:    uploaded.FileSHA256,
					FileLength:    proto.Uint64(uint64(len(imageData))),
				},
			},
			HasMediaAttachment: proto.Bool(true),
		}
	}

	interactive.ContextInfo = service.mergeReplyContext(ctx, interactive.ContextInfo, request.ReplyMessageID)

	msg := &waE2E.Message{InteractiveMessage: interactive}
	ts, err := service.wrapSendMessage(ctx, client, dataWaRecipient, msg, "interactive")
	if err != nil {
		return response, err
	}

	response.MessageID = ts.ID
	response.Status = fmt.Sprintf("Interactive message sent to %s (server ID: %s)", request.BaseRequest.Phone, ts.ID)
	return response, nil
}
