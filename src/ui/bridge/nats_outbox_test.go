package bridge

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestOutboxCapacityConfigurationAndExpansionPreserveEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "capacity.db")
	t.Setenv("BRIDGE_NATS_OUTBOX_MAX_BYTES", "1048576")
	o, err := openEventOutbox(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.enqueue(context.Background(), "retained", "message.received", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	stats, err := o.stats(context.Background())
	if err != nil || stats.MaxBytes != 1<<20 || stats.UsedBytes <= 0 {
		t.Fatalf("capacity: %+v %v", stats, err)
	}
	if err := o.db.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BRIDGE_NATS_OUTBOX_MAX_BYTES", "8589934592")
	o, err = openEventOutbox(path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.db.Close()
	stats, err = o.stats(context.Background())
	if err != nil || stats.MaxBytes != 8<<30 || stats.Pending != 1 {
		t.Fatalf("expanded: %+v %v", stats, err)
	}
	batch, err := o.batch(context.Background())
	if err != nil || len(batch) != 1 || batch[0].eventID != "retained" {
		t.Fatalf("expansion lost event: %+v %v", batch, err)
	}
}

func TestOutboxRejectsInvalidCapacity(t *testing.T) {
	for _, value := range []string{"-1", "0", "1024", "8GB", "1099511627777", "9223372036854775808"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("BRIDGE_NATS_OUTBOX_MAX_BYTES", value)
			if o, err := openEventOutbox(filepath.Join(t.TempDir(), "invalid.db")); err == nil {
				_ = o.db.Close()
				t.Fatal("invalid capacity accepted")
			}
		})
	}
}

func TestOutboxConcurrentEnqueuesRemainDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "burst.db")
	o, err := openEventOutbox(path)
	if err != nil {
		t.Fatal(err)
	}
	const count = 2000
	start := make(chan struct{})
	errorsCh := make(chan error, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			errorsCh <- o.enqueue(ctx, fmt.Sprint(i), "message.status", []byte(`{"status":"delivered"}`))
		}(i)
	}
	close(start)
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Errorf("burst enqueue: %v", err)
		}
	}
	if err := o.db.Close(); err != nil {
		t.Fatal(err)
	}
	o, err = openEventOutbox(path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.db.Close()
	stats, err := o.stats(context.Background())
	if err != nil || stats.Pending != count {
		t.Fatalf("reopened spool: %+v, error=%v", stats, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := o.enqueue(ctx, "cancelled", "message.status", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled enqueue: %v", err)
	}
}

func TestOutboxProbeDoesNotPublishOrDeleteRealEvents(t *testing.T) {
	o, err := openEventOutbox(filepath.Join(t.TempDir(), "probe.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer o.db.Close()
	if err := o.enqueue(context.Background(), "real-event", "message.received", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := o.probe(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	batch, err := o.batch(context.Background())
	if err != nil || len(batch) != 1 || batch[0].eventID != "real-event" {
		t.Fatalf("probe changed visible events: %+v %v", batch, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := o.probe(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("probe cancellation: %v", err)
	}
	if err := o.db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := o.probe(context.Background()); err == nil {
		t.Fatal("closed spool passed write probe")
	}
}
