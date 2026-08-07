package chatstorage

import (
	"context"
	"fmt"
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

	gate          chan struct{} // when non-nil, writes block until a value is received
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

// batchingChatStore implements batchCommitter (RunInTx) so the async layer
// exercises its group-commit path. It records every stored id, counts the
// transactions used, and flags if a solo write ever ran inside a transaction.
type batchingChatStore struct {
	domainChatStorage.IChatStorageRepository

	mu            sync.Mutex
	stored        []string
	batchRuns     int
	inTx          bool
	inTxViolation bool

	gate      chan struct{} // when non-nil, the first StoreMessage blocks on it
	gateOnce  sync.Once
	panicOnID string // StoreMessage panics on this id, to exercise batch panic isolation
}

func (f *batchingChatStore) RunInTx(fn func(repo domainChatStorage.IChatStorageRepository) error) error {
	f.mu.Lock()
	f.batchRuns++
	f.inTx = true
	f.mu.Unlock()
	err := fn(f) // run the batch against ourselves, standing in for the tx-bound repo
	f.mu.Lock()
	f.inTx = false
	f.mu.Unlock()
	return err
}

func (f *batchingChatStore) StoreMessage(m *domainChatStorage.Message) error {
	if f.gate != nil {
		f.gateOnce.Do(func() { <-f.gate }) // hold only the first write so the queue piles up
	}
	if f.panicOnID != "" && m.ID == f.panicOnID {
		panic("boom in StoreMessage")
	}
	f.mu.Lock()
	f.stored = append(f.stored, m.ID)
	f.mu.Unlock()
	return nil
}

func (f *batchingChatStore) StoreMessagesBatch(ms []*domainChatStorage.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.inTx {
		f.inTxViolation = true // a self-transactioning write must never nest in the group tx
	}
	for _, m := range ms {
		f.stored = append(f.stored, m.ID)
	}
	return nil
}

func (f *batchingChatStore) snapshot() (stored []string, runs int, violation bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.stored...), f.batchRuns, f.inTxViolation
}

// TestAsyncGroupCommitsBatchedWrites verifies that a burst of queued writes is
// folded into far fewer transactions than there are writes, while every write
// still persists. The gate holds the first write so the rest pile up in the
// queue and get drained together.
func TestAsyncGroupCommitsBatchedWrites(t *testing.T) {
	fake := &batchingChatStore{gate: make(chan struct{})}
	a := NewAsyncRepository(fake, AsyncConfig{QueueSize: 1000, MaxBatch: 256})

	const n = 200
	for i := 0; i < n; i++ {
		if err := a.StoreMessage(&domainChatStorage.Message{ID: fmt.Sprintf("m%d", i)}); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	close(fake.gate) // release the first write; the pile now drains as a batch
	if err := a.Close(context.Background()); err != nil {
		t.Fatalf("Close err: %v", err)
	}

	stored, runs, violation := fake.snapshot()
	if len(stored) != n {
		t.Fatalf("persisted %d of %d writes", len(stored), n)
	}
	if violation {
		t.Fatalf("a solo write ran inside the group transaction")
	}
	if runs == 0 {
		t.Fatalf("group commit was never used (RunInTx runs=0)")
	}
	if runs >= n {
		t.Fatalf("no batching: %d transactions for %d writes", runs, n)
	}
	if st := a.Stats(); st.Committed == 0 {
		t.Fatalf("stats.Committed=0, expected group commits: %+v", st)
	}
}

// TestAsyncGroupCommitIsolatesPanickingTask verifies that when one write in a
// batched group panics, the panic is contained to that task: every other write
// in the group still persists, the panic is counted as a failure, and the
// worker survives (Close returns).
func TestAsyncGroupCommitIsolatesPanickingTask(t *testing.T) {
	fake := &batchingChatStore{gate: make(chan struct{}), panicOnID: "boom"}
	a := NewAsyncRepository(fake, AsyncConfig{QueueSize: 1000, MaxBatch: 256})

	const n = 50
	const poison = 25
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("m%d", i)
		if i == poison {
			id = "boom"
		}
		if err := a.StoreMessage(&domainChatStorage.Message{ID: id}); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
	close(fake.gate) // release; the pile drains into a group that includes the poison task
	if err := a.Close(context.Background()); err != nil {
		t.Fatalf("Close err (worker did not survive the panic): %v", err)
	}

	stored, _, _ := fake.snapshot()
	if len(stored) != n-1 {
		t.Fatalf("persisted %d writes, want %d (all but the panicking task)", len(stored), n-1)
	}
	for _, id := range stored {
		if id == "boom" {
			t.Fatalf("panicking task unexpectedly persisted")
		}
	}
	if st := a.Stats(); st.Failed == 0 {
		t.Fatalf("panicking task was not counted as failed: %+v", st)
	}
}

// TestAsyncSoloTaskNotFoldedIntoGroupCommit ensures a self-transactioning write
// (StoreMessagesBatch) runs standalone, never nested inside the group tx where
// SQLite's single writer would deadlock — and that ordering around it is kept.
func TestAsyncSoloTaskNotFoldedIntoGroupCommit(t *testing.T) {
	fake := &batchingChatStore{}
	a := NewAsyncRepository(fake, AsyncConfig{QueueSize: 1000, MaxBatch: 256})

	_ = a.StoreMessage(&domainChatStorage.Message{ID: "a"})
	_ = a.StoreMessagesBatch([]*domainChatStorage.Message{{ID: "b"}, {ID: "c"}})
	_ = a.StoreMessage(&domainChatStorage.Message{ID: "d"})
	if err := a.Close(context.Background()); err != nil {
		t.Fatalf("Close err: %v", err)
	}

	stored, _, violation := fake.snapshot()
	if violation {
		t.Fatalf("StoreMessagesBatch ran inside a group transaction (deadlock risk)")
	}
	if len(stored) != 4 {
		t.Fatalf("persisted %v, want 4 rows", stored)
	}
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
