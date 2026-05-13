package bridge

import "testing"

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
