package bridge

import (
	"context"
	"errors"
	"testing"
	"time"

	domainDevice "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/device"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/sqlite"
	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
)

// newSessionTestStore creates an isolated on-disk sqlstore container for tests.
func newSessionTestStore(t *testing.T) *sqlstore.Container {
	t.Helper()
	uri := sqlite.FormatChatStorageURI("file:"+t.TempDir()+"/whatsmeow.db", true, true)
	container, err := sqlstore.New(context.Background(), sqlite.DriverName, uri, nil)
	if err != nil {
		t.Fatalf("create sqlstore: %v", err)
	}
	t.Cleanup(func() { _ = container.Close() })
	return container
}

// seedStoreDevice persists a device with the given JID so the store reports a
// recoverable session for it.
func seedStoreDevice(t *testing.T, container *sqlstore.Container, jid types.JID) *whatsmeow.Client {
	t.Helper()
	device := container.NewDevice()
	device.ID = &jid
	device.PushName = "Seeded"
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             []byte{1},
		AccountSignature:    make([]byte, 64),
		AccountSignatureKey: make([]byte, 32),
		DeviceSignature:     make([]byte, 64),
	}
	if err := container.PutDevice(context.Background(), device); err != nil {
		t.Fatalf("put device: %v", err)
	}
	return whatsmeow.NewClient(device, nil)
}

// newSessionTestService builds a Service wired to a real DeviceManager with the
// given instance, plus the minimum maps/seams needed for status checks.
func newSessionTestService(manager *whatsapp.DeviceManager) *Service {
	svc := &Service{
		deps:                 Dependencies{DeviceManager: manager},
		connected:            make(map[string]time.Time),
		statuses:             make(map[string]string),
		reconnecting:         make(map[string]time.Time),
		explicitOffline:      make(map[string]time.Time),
		historySyncRequested: make(map[string]time.Time),
	}
	// Make any scheduled reconnect bail immediately instead of doing real work.
	svc.accountContextForReconnect = func(ctx context.Context, accountID string) (context.Context, error) {
		return nil, errors.New("test: skip reconnect")
	}
	return svc
}

// A client that exists but has not loaded its persisted device (Store.ID == nil)
// must NOT be reported as logged out when the store still holds a valid session.
// Otherwise upstream reconciliation falsely flips the account to "logged out"
// and strands a healthy account.
func TestGetAccountStatusRecoverableSessionReportsConnecting(t *testing.T) {
	ctx := context.Background()
	container := newSessionTestStore(t)
	adJID := types.NewADJID("6281444444444", types.WhatsAppDomain, 14)

	// Persisted, valid session in the store + a loaded client to seed the JID.
	loadedClient := seedStoreDevice(t, container, adJID)
	inst := whatsapp.NewDeviceInstance("1", loadedClient, nil)
	// Swap in a fresh client without a loaded session (Store.ID == nil); the
	// instance keeps its JID from the loaded client.
	inst.SetClient(whatsmeow.NewClient(container.NewDevice(), nil))
	inst.SetState(domainDevice.DeviceStateDisconnected)

	manager := whatsapp.NewDeviceManager(container, nil, nil)
	manager.AddDevice(inst)

	svc := newSessionTestService(manager)

	resp, err := svc.GetAccountStatus(ctx, &bridgepb.AccountStatusRequest{AccountId: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != "connecting" {
		t.Fatalf("status = %q, want connecting (recoverable session must not look logged out)", resp.Status)
	}
	if resp.StatusDetail == "No persisted WhatsApp session; login required" {
		t.Fatalf("recoverable session reported as login required: %q", resp.StatusDetail)
	}
}

// When the store has NO persisted device, a client without a session genuinely
// needs a fresh login and must still be reported as qr_pending.
func TestGetAccountStatusGenuinelyLoggedOutReportsQRPending(t *testing.T) {
	ctx := context.Background()
	container := newSessionTestStore(t)

	// Loaded client seeds a JID, but that JID is NOT persisted in the store.
	jid := types.NewADJID("6281555555555", types.WhatsAppDomain, 15)
	loadedDev := container.NewDevice()
	loadedDev.ID = &jid
	loadedClient := whatsmeow.NewClient(loadedDev, nil)
	inst := whatsapp.NewDeviceInstance("1", loadedClient, nil)
	inst.SetClient(whatsmeow.NewClient(container.NewDevice(), nil))
	inst.SetState(domainDevice.DeviceStateDisconnected)

	manager := whatsapp.NewDeviceManager(container, nil, nil)
	manager.AddDevice(inst)

	svc := newSessionTestService(manager)

	resp, err := svc.GetAccountStatus(ctx, &bridgepb.AccountStatusRequest{AccountId: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != "qr_pending" {
		t.Fatalf("status = %q, want qr_pending (no persisted session)", resp.Status)
	}
	if resp.StatusDetail != "No persisted WhatsApp session; login required" {
		t.Fatalf("detail = %q, want login required", resp.StatusDetail)
	}
}
