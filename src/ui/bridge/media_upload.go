package bridge

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
)

func (s *Service) uploadMedia(filePath, msgID, msgType string, instance *whatsapp.DeviceInstance, mimeType string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", filepath.Base(filePath))
	if err != nil {
		return err
	}
	if _, err := io.Copy(part, file); err != nil {
		return err
	}
	fields := map[string]string{
		"uploadFileTypeEnum": strings.ToUpper(msgType),
		"appType":            "WHATSAPP",
		"msg":                "",
		"id":                 msgID,
		"thirdAppUserId":     instance.JID(),
		"account_id":         instance.ID(),
		"external_msg_id":    msgID,
	}
	for key, value := range fields {
		if err := writer.WriteField(key, value); err != nil {
			return err
		}
	}
	if err := writer.Close(); err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, s.cfg.UploadMediaURL, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-Internal-API-Key", s.cfg.UploadAPIKey)
	if mimeType != "" {
		req.Header.Set("X-Media-Mime-Type", mimeType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("upload failed: HTTP %d", resp.StatusCode)
	}
	return nil
}
