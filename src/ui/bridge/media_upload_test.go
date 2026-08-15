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
