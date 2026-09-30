package bridge

import (
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
)

func TestUploadMediaSetsMultipartFileContentTypeFromMIME(t *testing.T) {
	const mediaMIME = "application/ogg; codecs=opus"

	var gotFileContentType string
	var gotRequestMIME string
	var gotFileName string
	var sawFile bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotRequestMIME = r.Header.Get("X-Media-Mime-Type")
		contentType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			t.Errorf("parse request content type: %v", err)
			http.Error(w, "bad content type", http.StatusBadRequest)
			return
		}
		if contentType != "multipart/form-data" {
			t.Errorf("Content-Type = %q, want multipart/form-data", contentType)
			http.Error(w, "bad content type", http.StatusBadRequest)
			return
		}

		reader := multipart.NewReader(r.Body, params["boundary"])
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Errorf("read multipart part: %v", err)
				http.Error(w, "bad multipart", http.StatusBadRequest)
				return
			}
			if part.FormName() != "file" {
				continue
			}
			sawFile = true
			gotFileName = part.FileName()
			gotFileContentType = part.Header.Get("Content-Type")
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	filePath := filepath.Join(t.TempDir(), "voice.oga")
	if err := os.WriteFile(filePath, []byte("ogg audio"), 0644); err != nil {
		t.Fatalf("write media file: %v", err)
	}

	service := &Service{cfg: Config{UploadMediaURL: server.URL, UploadAPIKey: "test-key"}}
	instance := whatsapp.NewDeviceInstance("15551234567@s.whatsapp.net", nil, nil)

	if err := service.uploadMedia(filePath, "msg-1", "audio", "257", instance, mediaMIME); err != nil {
		t.Fatalf("uploadMedia() error = %v", err)
	}
	if gotRequestMIME != mediaMIME {
		t.Fatalf("X-Media-Mime-Type = %q, want %q", gotRequestMIME, mediaMIME)
	}
	if !sawFile {
		t.Fatal("multipart file part was not sent")
	}
	if gotFileName != "voice.oga" {
		t.Fatalf("file name = %q, want voice.oga", gotFileName)
	}
	if gotFileContentType != mediaMIME {
		t.Fatalf("file Content-Type = %q, want %q", gotFileContentType, mediaMIME)
	}
}

// Incoming media is downloaded to MediaDownloadPath (default /tmp/media) only
// to be uploaded to the platform; the platform uses the uploaded copy, never
// the local path. Keeping the file filled api02's 1G /tmp partition.
func TestUploadAndRemoveMediaDeletesLocalFileOnlyAfterSuccess(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		wantErr    bool
		wantExists bool
	}{
		{name: "upload ok removes local file", status: http.StatusOK, wantErr: false, wantExists: false},
		{name: "upload failure keeps local file", status: http.StatusBadGateway, wantErr: true, wantExists: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(tt.status)
			}))
			defer server.Close()

			filePath := filepath.Join(t.TempDir(), "photo.jpg")
			if err := os.WriteFile(filePath, []byte("jpeg"), 0644); err != nil {
				t.Fatalf("write media file: %v", err)
			}
			service := &Service{cfg: Config{UploadMediaURL: server.URL, UploadAPIKey: "test-key"}}
			instance := whatsapp.NewDeviceInstance("15551234567@s.whatsapp.net", nil, nil)

			err := service.uploadAndRemoveMedia(filePath, "msg-1", "image", "257", instance, "image/jpeg")

			if (err != nil) != tt.wantErr {
				t.Fatalf("uploadAndRemoveMedia() error = %v, wantErr %v", err, tt.wantErr)
			}
			_, statErr := os.Stat(filePath)
			if exists := statErr == nil; exists != tt.wantExists {
				t.Fatalf("local file exists = %v, want %v (stat err: %v)", exists, tt.wantExists, statErr)
			}
		})
	}
}
