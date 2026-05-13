package bridge

import (
	"bytes"
	"image"
	"image/png"
	"testing"
)

func TestLegacyContactChatID(t *testing.T) {
	tests := []struct {
		name  string
		phone string
		want  string
	}{
		{name: "bare number", phone: "15812751827", want: "15812751827@c.us"},
		{name: "formatted number", phone: "+1 (581) 275-1827", want: "15812751827@c.us"},
		{name: "whatsmeow jid", phone: "15812751827@s.whatsapp.net", want: "15812751827@c.us"},
		{name: "legacy jid", phone: "15812751827@c.us", want: "15812751827@c.us"},
		{name: "group jid unchanged", phone: "120363123456789@g.us", want: "120363123456789@g.us"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := legacyContactChatID(tt.phone); got != tt.want {
				t.Fatalf("legacyContactChatID(%q) = %q, want %q", tt.phone, got, tt.want)
			}
		})
	}
}

func TestLegacyContactNumber(t *testing.T) {
	tests := []struct {
		name  string
		phone string
		want  string
	}{
		{name: "bare number", phone: "15812751827", want: "15812751827"},
		{name: "formatted number", phone: "+1 (581) 275-1827", want: "15812751827"},
		{name: "whatsmeow jid", phone: "15812751827@s.whatsapp.net", want: "15812751827"},
		{name: "legacy jid", phone: "15812751827@c.us", want: "15812751827"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := legacyContactNumber(tt.phone); got != tt.want {
				t.Fatalf("legacyContactNumber(%q) = %q, want %q", tt.phone, got, tt.want)
			}
		})
	}
}

func TestPrepareProfilePictureJPEG(t *testing.T) {
	src := image.NewRGBA(image.Rect(0, 0, 1200, 800))
	var input bytes.Buffer
	if err := png.Encode(&input, src); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	output, err := prepareProfilePictureJPEG(input.Bytes())
	if err != nil {
		t.Fatalf("prepareProfilePictureJPEG() error = %v", err)
	}
	if len(output) == 0 {
		t.Fatal("prepareProfilePictureJPEG() returned empty output")
	}
	if len(output) > profilePictureMaxBytes {
		t.Fatalf("output size = %d, want <= %d", len(output), profilePictureMaxBytes)
	}

	got, format, err := image.Decode(bytes.NewReader(output))
	if err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if format != "jpeg" {
		t.Fatalf("format = %q, want jpeg", format)
	}
	bounds := got.Bounds()
	if bounds.Dx() != bounds.Dy() {
		t.Fatalf("output dimensions = %dx%d, want square", bounds.Dx(), bounds.Dy())
	}
	if bounds.Dx() > profilePictureMaxDimension {
		t.Fatalf("output dimension = %d, want <= %d", bounds.Dx(), profilePictureMaxDimension)
	}
}

func TestPrepareProfilePictureJPEGRejectsInvalidImage(t *testing.T) {
	if _, err := prepareProfilePictureJPEG([]byte("not an image")); err == nil {
		t.Fatal("prepareProfilePictureJPEG() error = nil, want error")
	}
}
