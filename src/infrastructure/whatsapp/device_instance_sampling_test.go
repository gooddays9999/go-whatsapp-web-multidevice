package whatsapp

import (
	"testing"

	domainDevice "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/device"
	"go.mau.fi/whatsmeow"
)

func TestTryClientStateDoesNotWaitForBusyInstance(t *testing.T) {
	d := NewDeviceInstance("1", nil, nil)
	d.state = domainDevice.DeviceStateLoggedIn
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, known := d.TryClientState(); known {
		t.Fatal("busy instance must be unknown")
	}
	if d.state != domainDevice.DeviceStateLoggedIn {
		t.Fatal("sampling rewrote the cached state")
	}
}

func TestSnapshotDevicesDoesNotLockIndividualDevices(t *testing.T) {
	d := NewDeviceInstance("1", nil, nil)
	m := &DeviceManager{devices: map[string]*DeviceInstance{"1": d}}
	d.mu.Lock()
	defer d.mu.Unlock()
	devices := m.SnapshotDevices()
	if len(devices) != 1 || devices[0] != d {
		t.Fatalf("unexpected registry snapshot: %v", devices)
	}
	delete(m.devices, "1")
	if len(devices) != 1 {
		t.Fatal("snapshot aliases registry")
	}
}

func TestTryClientStateDoesNotReuseStaleOnlineState(t *testing.T) {
	for _, client := range []*whatsmeow.Client{nil, {}} {
		d := NewDeviceInstance("1", client, nil)
		d.SetState(domainDevice.DeviceStateLoggedIn)
		if state, known := d.TryClientState(); !known || state != domainDevice.DeviceStateDisconnected {
			t.Fatalf("expected actual disconnected state, got %q, known=%t", state, known)
		}
		if d.State() != domainDevice.DeviceStateLoggedIn {
			t.Fatal("sampling must not mutate the cached state")
		}
	}
}
