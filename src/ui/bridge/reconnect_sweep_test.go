package bridge

import "testing"

func TestShouldSweepReconnect(t *testing.T) {
	tests := []struct {
		name            string
		hasSession      bool
		connected       bool
		explicitOffline bool
		reconnecting    bool
		want            bool
	}{
		{name: "session, disconnected, idle -> reconnect", hasSession: true, connected: false, explicitOffline: false, reconnecting: false, want: true},
		{name: "already connected -> skip", hasSession: true, connected: true, explicitOffline: false, reconnecting: false, want: false},
		{name: "no session (logged out) -> skip", hasSession: false, connected: false, explicitOffline: false, reconnecting: false, want: false},
		{name: "explicitly offline -> skip", hasSession: true, connected: false, explicitOffline: true, reconnecting: false, want: false},
		{name: "already reconnecting -> skip", hasSession: true, connected: false, explicitOffline: false, reconnecting: true, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldSweepReconnect(tt.hasSession, tt.connected, tt.explicitOffline, tt.reconnecting); got != tt.want {
				t.Fatalf("shouldSweepReconnect(%v,%v,%v,%v) = %v, want %v", tt.hasSession, tt.connected, tt.explicitOffline, tt.reconnecting, got, tt.want)
			}
		})
	}
}
