package whatsapp

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
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
