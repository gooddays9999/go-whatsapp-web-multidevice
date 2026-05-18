package bridge

import (
	"testing"

	"go.mau.fi/whatsmeow"
)

func TestPairClientInfoUsesEnvironmentDevice(t *testing.T) {
	tests := []struct {
		name    string
		env     *BridgeEnvironment
		wantTyp whatsmeow.PairClientType
		want    string
	}{
		{
			name: "chrome windows",
			env: &BridgeEnvironment{
				BrowserFamily: "Chrome",
				OSName:        "Windows",
			},
			wantTyp: whatsmeow.PairClientChrome,
			want:    "Chrome (Windows)",
		},
		{
			name: "edge macos",
			env: &BridgeEnvironment{
				BrowserFamily: "Edge",
				OSName:        "macOS",
			},
			wantTyp: whatsmeow.PairClientEdge,
			want:    "Edge (macOS)",
		},
		{
			name: "firefox linux",
			env: &BridgeEnvironment{
				BrowserFamily: "Firefox",
				OSName:        "Linux",
			},
			wantTyp: whatsmeow.PairClientFirefox,
			want:    "Firefox (Linux)",
		},
		{
			name: "safari macos",
			env: &BridgeEnvironment{
				BrowserFamily: "Safari",
				OSName:        "mac os x",
			},
			wantTyp: whatsmeow.PairClientSafari,
			want:    "Safari (macOS)",
		},
		{
			name:    "fallback",
			env:     &BridgeEnvironment{},
			wantTyp: whatsmeow.PairClientChrome,
			want:    "Chrome (Linux)",
		},
		{
			name: "parse persisted user agent when family fields are empty",
			env: &BridgeEnvironment{
				UserAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.3179.85",
			},
			wantTyp: whatsmeow.PairClientEdge,
			want:    "Edge (Windows)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTyp, got := pairClientInfo(tt.env)
			if gotTyp != tt.wantTyp || got != tt.want {
				t.Fatalf("pairClientInfo() = %v %q, want %v %q", gotTyp, got, tt.wantTyp, tt.want)
			}
		})
	}
}
