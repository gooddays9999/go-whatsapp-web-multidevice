package whatsapp

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

func TestRunWAVersionRefresherFetchesEveryInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	done := make(chan struct{})

	go func() {
		defer close(done)
		runWAVersionRefresher(ctx, 10*time.Millisecond, func(context.Context) {
			if calls.Add(1) == 3 {
				cancel()
			}
		})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("refresher did not stop after the context was cancelled")
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("fetch calls = %d, want 3", got)
	}
}

func TestRunWAVersionRefresherDoesNotFetchImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	done := make(chan struct{})

	go func() {
		defer close(done)
		runWAVersionRefresher(ctx, time.Hour, func(context.Context) { calls.Add(1) })
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	// The startup fetch already ran in initApp; the refresher only covers later drift.
	if got := calls.Load(); got != 0 {
		t.Fatalf("fetch calls = %d, want 0 before the first interval elapses", got)
	}
}

func TestStartWAVersionRefresherDisabledForNonPositiveInterval(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Minute} {
		if StartWAVersionRefresher(context.Background(), interval, time.Second) {
			t.Fatalf("interval %s: refresher started, want disabled", interval)
		}
	}
}

// A refresh must change what the next handshake announces while leaving
// payloads already built (live sessions) alone. whatsmeow builds the login
// payload from store.BaseClientPayload on every connect via
// Device.GetClientPayload, the same call used here.
func TestApplyLatestWAVersionAffectsNextHandshakePayload(t *testing.T) {
	orig := store.GetWAVersion()
	t.Cleanup(func() { store.SetWAVersion(orig) })
	store.SetWAVersion(store.WAVersionContainer{2, 3000, 1000000001})

	jid := types.NewJID("15550000000", types.DefaultUserServer)
	device := &store.Device{ID: &jid}
	before := device.GetClientPayload()

	ApplyLatestWAVersion(context.Background(), mockHTTPClient(`{"client_revision":1048792267,"x":1}`, 200, nil))

	after := device.GetClientPayload()
	if got := after.GetUserAgent().GetAppVersion().GetTertiary(); got != 1048792267 {
		t.Fatalf("next handshake app version tertiary = %d, want 1048792267", got)
	}
	if got := before.GetUserAgent().GetAppVersion().GetTertiary(); got != 1000000001 {
		t.Fatalf("already-built payload changed to %d; live sessions must not be affected", got)
	}
}
