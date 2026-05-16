package whatsapp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	domainDevice "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/device"
	"go.mau.fi/whatsmeow"
)

var ErrDeviceConnectInProgress = errors.New("device connect already in progress")

type DeviceSnapshot struct {
	State            domainDevice.DeviceState
	DisplayName      string
	PhoneNumber      string
	JID              string
	ProxyAddress     string
	UserAgent        string
	BrowserFamily    string
	OSName           string
	Connecting       bool
	ConnectingSince  time.Time
	LastConnectAt    time.Time
	LastConnectError string
}

// DeviceInstance bundles a WhatsApp client with device metadata and scoped storage.
type DeviceInstance struct {
	mu              sync.RWMutex
	connectMu       sync.Mutex
	id              string
	client          *whatsmeow.Client
	chatStorageRepo domainChatStorage.IChatStorageRepository
	state           domainDevice.DeviceState
	displayName     string
	phoneNumber     string
	jid             string
	proxyAddress    string
	userAgent       string
	browserFamily   string
	osName          string
	createdAt       time.Time
	connecting      bool
	connectingSince time.Time
	lastConnectAt   time.Time
	lastConnectErr  string
	onLoggedOut     func(deviceID string) // Callback for remote logout cleanup
}

func NewDeviceInstance(deviceID string, client *whatsmeow.Client, chatStorageRepo domainChatStorage.IChatStorageRepository) *DeviceInstance {
	jid := ""
	display := ""
	if client != nil && client.Store != nil && client.Store.ID != nil {
		jid = client.Store.ID.ToNonAD().String()
		display = client.Store.PushName
	}

	return &DeviceInstance{
		id:              deviceID,
		client:          client,
		chatStorageRepo: chatStorageRepo,
		state:           domainDevice.DeviceStateDisconnected,
		displayName:     display,
		jid:             jid,
		createdAt:       time.Now(),
	}
}

func (d *DeviceInstance) ID() string {
	return d.id
}

func (d *DeviceInstance) GetClient() *whatsmeow.Client {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.client
}

func (d *DeviceInstance) GetChatStorage() domainChatStorage.IChatStorageRepository {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.chatStorageRepo
}

func (d *DeviceInstance) SetState(state domainDevice.DeviceState) {
	d.mu.Lock()
	d.state = state
	d.mu.Unlock()
}

func (d *DeviceInstance) MarkDisconnected() domainDevice.DeviceState {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.state = domainDevice.DeviceStateDisconnected
	d.refreshIdentityLocked()
	return d.state
}

func (d *DeviceInstance) UpdateStateFromLoginFlag() domainDevice.DeviceState {
	d.mu.RLock()
	client := d.client
	d.mu.RUnlock()
	loggedIn := false
	if client != nil {
		loggedIn = client.IsLoggedIn()
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if loggedIn {
		d.state = domainDevice.DeviceStateLoggedIn
	} else {
		d.state = domainDevice.DeviceStateConnected
	}
	d.refreshIdentityLocked()
	return d.state
}

func (d *DeviceInstance) State() domainDevice.DeviceState {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.state
}

func (d *DeviceInstance) Snapshot() DeviceSnapshot {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return DeviceSnapshot{
		State:            d.state,
		DisplayName:      d.displayName,
		PhoneNumber:      d.phoneNumber,
		JID:              d.jid,
		ProxyAddress:     d.proxyAddress,
		UserAgent:        d.userAgent,
		BrowserFamily:    d.browserFamily,
		OSName:           d.osName,
		Connecting:       d.connecting,
		ConnectingSince:  d.connectingSince,
		LastConnectAt:    d.lastConnectAt,
		LastConnectError: d.lastConnectErr,
	}
}

func (d *DeviceInstance) DisplayName() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.displayName
}

func (d *DeviceInstance) PhoneNumber() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.phoneNumber
}

func (d *DeviceInstance) JID() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.jid
}

func (d *DeviceInstance) ProxyAddress() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.proxyAddress
}

func (d *DeviceInstance) UserAgent() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.userAgent
}

func (d *DeviceInstance) BrowserFamily() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.browserFamily
}

func (d *DeviceInstance) OSName() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.osName
}

func (d *DeviceInstance) CreatedAt() time.Time {
	return d.createdAt
}

// SetClient attaches a WhatsApp client to this instance and updates metadata.
func (d *DeviceInstance) SetClient(client *whatsmeow.Client) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.client = client
	d.refreshIdentityLocked()
	d.state = domainDevice.DeviceStateDisconnected
}

// SetChatStorage swaps the chat storage repository for this device.
func (d *DeviceInstance) SetChatStorage(repo domainChatStorage.IChatStorageRepository) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.chatStorageRepo = repo
}

func (d *DeviceInstance) SetEnvironment(proxyAddress, userAgent, browserFamily, osName string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.proxyAddress = proxyAddress
	d.userAgent = userAgent
	d.browserFamily = browserFamily
	d.osName = osName
}

// IsConnected returns the live connection flag if a client exists.
func (d *DeviceInstance) IsConnected() bool {
	d.mu.RLock()
	client := d.client
	d.mu.RUnlock()
	if client == nil {
		return false
	}
	return client.IsConnected()
}

// IsLoggedIn returns the login status if a client exists.
func (d *DeviceInstance) IsLoggedIn() bool {
	d.mu.RLock()
	client := d.client
	d.mu.RUnlock()
	if client == nil {
		return false
	}
	return client.IsLoggedIn()
}

func (d *DeviceInstance) ConnectWithTimeout(ctx context.Context, timeout time.Duration, reason string) error {
	if d == nil {
		return fmt.Errorf("device instance is nil")
	}
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	if !d.connectMu.TryLock() {
		return ErrDeviceConnectInProgress
	}
	defer d.connectMu.Unlock()

	d.mu.Lock()
	client := d.client
	now := time.Now()
	d.connecting = true
	d.connectingSince = now
	d.lastConnectAt = now
	d.lastConnectErr = ""
	d.state = domainDevice.DeviceStateConnecting
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		d.connecting = false
		d.connectingSince = time.Time{}
		d.mu.Unlock()
	}()

	if client == nil {
		err := fmt.Errorf("account client is nil")
		d.recordConnectResult(err)
		return err
	}

	if ctx == nil {
		ctx = context.Background()
	}
	connectCtx, cancelConnect := context.WithCancel(context.Background())
	var timedOut atomic.Bool
	timer := time.AfterFunc(timeout, func() {
		timedOut.Store(true)
		cancelConnect()
	})
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cancelConnect()
		case <-done:
		}
	}()
	err := client.ConnectContext(connectCtx)
	close(done)
	if !timer.Stop() && timedOut.Load() {
		// The timer already cancelled the connection attempt.
	} else if err != nil {
		cancelConnect()
	}
	if errors.Is(err, whatsmeow.ErrAlreadyConnected) {
		err = nil
	}
	if err == nil && timedOut.Load() {
		err = fmt.Errorf("connect timeout after %s: %w", timeout, context.DeadlineExceeded)
	}
	if err == nil && ctx.Err() != nil {
		err = fmt.Errorf("connect cancelled: %w", ctx.Err())
	}
	if err != nil {
		if timedOut.Load() {
			err = fmt.Errorf("connect timeout after %s: %w", timeout, context.DeadlineExceeded)
		} else if ctx.Err() != nil {
			err = fmt.Errorf("connect cancelled: %w", ctx.Err())
		}
		d.recordConnectResult(err)
		d.SetState(domainDevice.DeviceStateDisconnected)
		return err
	}

	state := d.UpdateStateFromClient()
	if state == domainDevice.DeviceStateDisconnected {
		err = fmt.Errorf("connect returned without an active websocket")
		if reason != "" {
			err = fmt.Errorf("%s: %w", reason, err)
		}
		d.recordConnectResult(err)
		return err
	}
	d.recordConnectResult(nil)
	return nil
}

// UpdateStateFromClient refreshes the snapshot state based on the client flags.
func (d *DeviceInstance) UpdateStateFromClient() domainDevice.DeviceState {
	d.mu.RLock()
	client := d.client
	d.mu.RUnlock()
	connected := false
	loggedIn := false
	if client != nil {
		connected = client.IsConnected()
		loggedIn = client.IsLoggedIn()
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	switch {
	case connected && loggedIn:
		d.state = domainDevice.DeviceStateLoggedIn
	case connected:
		d.state = domainDevice.DeviceStateConnected
	default:
		d.state = domainDevice.DeviceStateDisconnected
	}

	d.refreshIdentityLocked()
	return d.state
}

func (d *DeviceInstance) recordConnectResult(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err != nil {
		d.lastConnectErr = err.Error()
		return
	}
	d.lastConnectErr = ""
}

func (d *DeviceInstance) refreshIdentityLocked() {
	if d.client != nil && d.client.Store != nil && d.client.Store.ID != nil {
		d.jid = d.client.Store.ID.ToNonAD().String()
		d.displayName = d.client.Store.PushName
	}
}

func (d *DeviceInstance) SetOnLoggedOut(callback func(deviceID string)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onLoggedOut = callback
}

func (d *DeviceInstance) TriggerLoggedOut() {
	d.mu.RLock()
	callback := d.onLoggedOut
	deviceID := d.id
	d.mu.RUnlock()

	if callback != nil {
		callback(deviceID)
	}
}
