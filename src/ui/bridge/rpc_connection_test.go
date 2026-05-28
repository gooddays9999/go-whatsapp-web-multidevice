package bridge

import (
	"context"
	"testing"
	"time"

	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
)

func TestExplicitOfflineStatusDoesNotScheduleReconnect(t *testing.T) {
	t.Parallel()

	service := &Service{
		connected:       make(map[string]time.Time),
		statuses:        make(map[string]string),
		reconnecting:    make(map[string]time.Time),
		explicitOffline: make(map[string]time.Time),
	}
	service.markExplicitOffline("1")

	resp, err := service.GetAccountStatus(context.Background(), &bridgepb.AccountStatusRequest{AccountId: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != "offline" {
		t.Fatalf("status = %q, want offline", resp.Status)
	}
	if resp.IsUsable {
		t.Fatal("explicitly disconnected account must not be usable")
	}
	if service.canScheduleReconnect(context.Background(), "1", nil) {
		t.Fatal("explicitly disconnected account must not schedule reconnect")
	}
}
