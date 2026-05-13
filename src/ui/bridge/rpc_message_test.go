package bridge

import (
	"bytes"
	"image"
	"image/png"
	"net/http"
	"net/http/httptest"
	"testing"

	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
)

func TestBuildStatusMessageRejectsEmptyText(t *testing.T) {
	_, _, err := buildStatusMessage(t.Context(), nil, &bridgepb.SendStatusRequest{})
	if err == nil {
		t.Fatal("buildStatusMessage() error = nil, want error")
	}
}

func TestBuildStatusMessageUsesExtendedText(t *testing.T) {
	msg, hasMedia, err := buildStatusMessage(t.Context(), nil, &bridgepb.SendStatusRequest{
		Content: "hello status",
		Color:   "#123456",
	})
	if err != nil {
		t.Fatalf("buildStatusMessage() error = %v", err)
	}
	if hasMedia {
		t.Fatal("hasMedia = true, want false")
	}
	if msg.GetExtendedTextMessage().GetText() != "hello status" {
		t.Fatalf("text = %q, want hello status", msg.GetExtendedTextMessage().GetText())
	}
	if got := msg.GetExtendedTextMessage().GetBackgroundArgb(); got != 0xFF123456 {
		t.Fatalf("background argb = %#x, want %#x", got, uint32(0xFF123456))
	}
}

func TestNormalizeStatusMediaMIMEFallsBackToURLWithoutDot(t *testing.T) {
	got := normalizeStatusMediaMIME("application/octet-stream", "http://example.test/media/20260511/1D2455QYD(163)mp4", []byte("not enough to sniff"))
	if got != "video/mp4" {
		t.Fatalf("mime = %q, want video/mp4", got)
	}
}

func TestDownloadStatusMediaAcceptsInferredVideoType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write([]byte("not enough to sniff but url contains mp4"))
	}))
	defer server.Close()

	_, mimeType, err := downloadStatusMedia(server.URL + "/media/file(1)mp4")
	if err != nil {
		t.Fatalf("downloadStatusMedia() error = %v", err)
	}
	if mimeType != "video/mp4" {
		t.Fatalf("mime = %q, want video/mp4", mimeType)
	}
}

func TestPrepareStatusImageBuildsThumbnail(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 640, 480))
	var input bytes.Buffer
	if err := png.Encode(&input, src); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	data, mimeType, thumb, width, height, err := prepareStatusImage(input.Bytes(), "image/png")
	if err != nil {
		t.Fatalf("prepareStatusImage() error = %v", err)
	}
	if len(data) == 0 || len(thumb) == 0 {
		t.Fatal("prepareStatusImage() returned empty image or thumbnail")
	}
	if mimeType != "image/png" {
		t.Fatalf("mime = %q, want image/png", mimeType)
	}
	if width != 640 || height != 480 {
		t.Fatalf("dimensions = %dx%d, want 640x480", width, height)
	}
}
