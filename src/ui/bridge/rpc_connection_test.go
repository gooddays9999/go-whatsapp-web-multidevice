package bridge

import (
	"context"
	"testing"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
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

func TestScheduledReconnectRequestsRecentHistorySync(t *testing.T) {
	service := &Service{
		cfg: Config{
			HistorySyncOnConnect:   true,
			HistorySyncMinInterval: time.Millisecond,
		},
		reconnecting:         make(map[string]time.Time),
		explicitOffline:      make(map[string]time.Time),
		historySyncRequested: make(map[string]time.Time),
	}
	inst := whatsapp.NewDeviceInstance("15488084637@s.whatsapp.net", nil, nil)
	scoped := whatsapp.ContextWithDevice(context.Background(), inst)
	accountContextCalled := make(chan struct{}, 1)
	service.accountContextForReconnect = func(ctx context.Context, accountID string) (context.Context, error) {
		if accountID != "229" {
			t.Fatalf("accountID = %q, want 229", accountID)
		}
		accountContextCalled <- struct{}{}
		return scoped, nil
	}

	service.scheduleReconnectWithMode("229", "unit test reconnect", false)

	select {
	case <-accountContextCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("scheduled reconnect did not call account context")
	}
	deadline := time.After(500 * time.Millisecond)
	for {
		service.mu.RLock()
		requestedAt := service.historySyncRequested["229"]
		service.mu.RUnlock()
		if !requestedAt.IsZero() {
			return
		}
		select {
		case <-deadline:
			t.Fatal("scheduled reconnect did not request recent history sync")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}
