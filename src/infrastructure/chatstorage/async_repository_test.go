package chatstorage

import (
	"context"
	"sync"
	"testing"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	"go.mau.fi/whatsmeow/proto/waE2E"
)

// fakeChatStore implements just enough of IChatStorageRepository for these
// tests. The embedded interface is nil, so any method we don't override panics
// if exercised — which keeps the fake honest about what the async layer touches.
type fakeChatStore struct {
	domainChatStorage.IChatStorageRepository

	mu         sync.Mutex
	sentIDs    []string
	sentCtxErr []error
	storeCount int

	gate     chan struct{} // when non-nil, writes block until a value is received
	getByIDResult *domainChatStorage.Message
	getByIDCalls  int
}

func (f *fakeChatStore) StoreSentMessageWithContext(ctx context.Context, messageID, _, _, _ string, _ time.Time, _ *waE2E.Message) error {
	if f.gate != nil {
		<-f.gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sentIDs = append(f.sentIDs, messageID)
	f.sentCtxErr = append(f.sentCtxErr, ctx.Err())
	return nil
}

func (f *fakeChatStore) StoreMessage(*domainChatStorage.Message) error {
	f.mu.Lock()
	f.storeCount++
	f.mu.Unlock()
	return nil
}

func (f *fakeChatStore) GetMessageByID(string) (*domainChatStorage.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getByIDCalls++
	return f.getByIDResult, nil
}

func (f *fakeChatStore) sentSnapshot() ([]string, []error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := append([]string(nil), f.sentIDs...)
	errs := append([]error(nil), f.sentCtxErr...)
	return ids, errs
}

// waitFor polls cond until true or the deadline elapses.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func TestAsyncStoreSentMessageReturnsImmediatelyAndPersists(t *testing.T) {
	fake := &fakeChatStore{}
	a := NewAsyncRepository(fake, AsyncConfig{QueueSize: 100})
	defer a.Close(context.Background())

	start := time.Now()
	if err := a.StoreSentMessageWithContext(context.Background(), "m1", "s", "r", "hi", time.Now(), nil); err != nil {
		t.Fatalf("StoreSentMessage returned err: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("enqueue blocked for %s, expected near-instant", elapsed)
	}

	waitFor(t, time.Second, func() bool {
		ids, _ := fake.sentSnapshot()
		return len(ids) == 1 && ids[0] == "m1"
	})
	if got := a.Stats(); got.Enqueued != 1 || got.Processed != 1 || got.Dropped != 0 {
		t.Fatalf("stats = %+v, want enqueued=1 processed=1 dropped=0", got)
	}
}

// TestAsyncDetachesFromCallerContext is the core of the fix: even when the
// caller's context is already canceled (the RPC has returned), the background
// write must still run under a live context.
func TestAsyncDetachesFromCallerContext(t *testing.T) {
	fake := &fakeChatStore{}
	a := NewAsyncRepository(fake, AsyncConfig{QueueSize: 10})
	defer a.Close(context.Background())

	canceled, cancel := context.WithCancel(context.Background())
	cancel() // dead on arrival

	if err := a.StoreSentMessageWithContext(canceled, "m1", "s", "r", "hi", time.Now(), nil); err != nil {
		t.Fatalf("err: %v", err)
	}

	waitFor(t, time.Second, func() bool {
		ids, _ := fake.sentSnapshot()
		return len(ids) == 1
	})
	_, errs := fake.sentSnapshot()
	if errs[0] != nil {
		t.Fatalf("background write saw ctx err %v, want a live (detached) context", errs[0])
	}
}

func TestAsyncReadsPassThroughSynchronously(t *testing.T) {
	want := &domainChatStorage.Message{}
	fake := &fakeChatStore{getByIDResult: want}
	a := NewAsyncRepository(fake, AsyncConfig{QueueSize: 10})
	defer a.Close(context.Background())

	got, err := a.GetMessageByID("x")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != want {
		t.Fatalf("GetMessageByID did not pass through to base")
	}
	if fake.getByIDCalls != 1 {
		t.Fatalf("base GetMessageByID calls = %d, want 1", fake.getByIDCalls)
	}
}

func TestAsyncDropsWhenQueueFullWithoutBlocking(t *testing.T) {
	fake := &fakeChatStore{gate: make(chan struct{})} // writes block until we release
	a := NewAsyncRepository(fake, AsyncConfig{QueueSize: 1})

	// Worker will pick the first task and block on the gate; the queue (size 1)
	// holds one more; everything after that must be dropped, never blocking.
	start := time.Now()
	for i := 0; i < 20; i++ {
		if err := a.StoreSentMessageWithContext(context.Background(), "m", "s", "r", "c", time.Now(), nil); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("enqueue loop blocked for %s; drop path should be non-blocking", elapsed)
	}
	if got := a.Stats(); got.Dropped == 0 {
		t.Fatalf("expected some drops with a full queue, stats=%+v", got)
	}

	close(fake.gate) // release blocked writes
	_ = a.Close(context.Background())
}

func TestAsyncCloseDrainsBufferedWrites(t *testing.T) {
	fake := &fakeChatStore{}
	a := NewAsyncRepository(fake, AsyncConfig{QueueSize: 1000})

	const n = 200
	for i := 0; i < n; i++ {
		if err := a.StoreMessage(&domainChatStorage.Message{}); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	if err := a.Close(context.Background()); err != nil {
		t.Fatalf("Close err: %v", err)
	}
	fake.mu.Lock()
	got := fake.storeCount
	fake.mu.Unlock()
	if got != n {
		t.Fatalf("after Close, persisted %d of %d writes", got, n)
	}
}
