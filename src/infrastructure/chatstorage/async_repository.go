package chatstorage

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	"github.com/sirupsen/logrus"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types/events"
)

// Default tuning for the async write-behind layer.
const (
	defaultAsyncQueueSize    = 20000
	defaultAsyncWriteTimeout = 30 * time.Second
	defaultAsyncMaxBatch     = 256
)

// AsyncConfig tunes the write-behind layer.
type AsyncConfig struct {
	// QueueSize is the number of pending write tasks buffered before new tasks
	// are dropped (best-effort). Each task is a small closure, so this can be
	// generous without meaningful memory cost.
	QueueSize int
	// WriteTimeout bounds each background flush so a pathological SQLite stall
	// cannot wedge the single worker forever.
	WriteTimeout time.Duration
	// MaxBatch is the most writes folded into one group-commit transaction. The
	// worker only ever groups tasks that are already queued (it never waits to
	// fill a batch), so this bounds a burst's transaction size, not latency.
	MaxBatch int
}

func (c AsyncConfig) withDefaults() AsyncConfig {
	if c.QueueSize <= 0 {
		c.QueueSize = defaultAsyncQueueSize
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = defaultAsyncWriteTimeout
	}
	if c.MaxBatch <= 0 {
		c.MaxBatch = defaultAsyncMaxBatch
	}
	return c
}

// AsyncStats is a snapshot of the write-behind counters.
type AsyncStats struct {
	Enqueued  int64
	Dropped   int64
	Processed int64
	Failed    int64
	Committed int64 // group-commit transactions executed
	QueueLen  int64
}

// asyncTask is one queued write. apply runs the write against repo, which is the
// base repository normally and a transaction-bound clone inside a group commit.
type asyncTask struct {
	name string
	// solo forces the task to run in its own transaction — never folded into a
	// group commit. Used for writes that already open their own transaction
	// (StoreMessagesBatch), which would deadlock SQLite's single writer if
	// nested inside the batch transaction.
	solo  bool
	apply func(ctx context.Context, repo domainChatStorage.IChatStorageRepository) error
}

// batchCommitter is implemented by the concrete SQLite repository. When the
// wrapped base supports it, the worker folds consecutive batchable writes into a
// single transaction (group commit) so a burst costs one commit, not N.
type batchCommitter interface {
	RunInTx(fn func(repo domainChatStorage.IChatStorageRepository) error) error
}

// AsyncRepository wraps a chat storage repository and moves the high-volume
// message/chat writes onto a single background worker so callers — above all
// the send path — never block on SQLite write-lock contention. A single serial
// writer also removes the lock thrash of many concurrent writers; group commit
// (below) then keeps that one writer efficient under load. Everything not
// overridden here (reads, device registry, webhook config, chatwoot, stats,
// schema) is inherited from the embedded base and runs synchronously.
//
// Writes are best-effort: the message has already been sent or received and
// WhatsApp remains the source of truth, so when the queue is full the task is
// dropped and counted rather than blocking the caller. Crucially, the worker
// runs each write under a fresh background context, so a write no longer dies
// with "context canceled" when the originating RPC returns.
type AsyncRepository struct {
	domainChatStorage.IChatStorageRepository // embedded base; non-overridden methods pass through

	cfg     AsyncConfig
	queue   chan asyncTask
	stop    chan struct{}
	closeMu sync.Once
	wg      sync.WaitGroup
	batcher batchCommitter // non-nil when the base supports group commit

	enqueued  atomic.Int64
	dropped   atomic.Int64
	processed atomic.Int64
	failed    atomic.Int64
	committed atomic.Int64
}

// NewAsyncRepository wraps base with a write-behind worker and starts it.
func NewAsyncRepository(base domainChatStorage.IChatStorageRepository, cfg AsyncConfig) *AsyncRepository {
	cfg = cfg.withDefaults()
	a := &AsyncRepository{
		IChatStorageRepository: base,
		cfg:                    cfg,
		queue:                  make(chan asyncTask, cfg.QueueSize),
		stop:                   make(chan struct{}),
	}
	if bc, ok := base.(batchCommitter); ok {
		a.batcher = bc
	}
	a.wg.Add(1)
	go a.worker()
	return a
}

func (a *AsyncRepository) worker() {
	defer a.wg.Done()
	for {
		select {
		case task := <-a.queue:
			a.processFrom(task)
		case <-a.stop:
			// Drain whatever is already buffered, then exit.
			for {
				select {
				case task := <-a.queue:
					a.processFrom(task)
				default:
					return
				}
			}
		}
	}
}

// processFrom greedily collects the tasks already sitting in the queue (it never
// waits for more) and flushes them together. Because it only drains what is
// already buffered, group commit adds throughput without adding latency.
func (a *AsyncRepository) processFrom(first asyncTask) {
	defer func() {
		if rec := recover(); rec != nil {
			a.failed.Add(1)
			logrus.WithField("panic", rec).Error("async chat storage batch panicked")
		}
	}()
	batch := a.drain(first)
	a.flush(batch)
}

// drain returns first plus any tasks immediately available in the queue, up to
// MaxBatch. It never blocks: the moment the queue is momentarily empty it stops.
func (a *AsyncRepository) drain(first asyncTask) []asyncTask {
	batch := make([]asyncTask, 0, a.cfg.MaxBatch)
	batch = append(batch, first)
	for len(batch) < a.cfg.MaxBatch {
		select {
		case task := <-a.queue:
			batch = append(batch, task)
		default:
			return batch
		}
	}
	return batch
}

// flush runs a drained batch. Batchable tasks fold into one group-commit
// transaction; solo tasks (and everything, when the base has no batch support)
// run one at a time. Relative order is preserved so a solo task flushes the
// group accumulated before it.
func (a *AsyncRepository) flush(batch []asyncTask) {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.WriteTimeout)
	defer cancel()

	if a.batcher == nil {
		for _, t := range batch {
			a.runOne(ctx, a.IChatStorageRepository, t)
		}
		return
	}

	group := make([]asyncTask, 0, len(batch))
	for _, t := range batch {
		if t.solo {
			a.commitGroup(ctx, group)
			group = group[:0]
			a.runOne(ctx, a.IChatStorageRepository, t)
			continue
		}
		group = append(group, t)
	}
	a.commitGroup(ctx, group)
}

// commitGroup persists group in a single transaction. A single-task group skips
// the transaction overhead. If the whole transaction fails (e.g. a commit or
// serialization error), each task is retried individually against the pool so
// one poison row cannot drop the entire batch — the writes are idempotent
// upserts, so a retry after a rollback is safe.
func (a *AsyncRepository) commitGroup(ctx context.Context, group []asyncTask) {
	switch len(group) {
	case 0:
		return
	case 1:
		a.runOne(ctx, a.IChatStorageRepository, group[0])
		return
	}

	err := a.batcher.RunInTx(func(repo domainChatStorage.IChatStorageRepository) error {
		for _, t := range group {
			// applyTask isolates a per-task error AND panic, so one bad write
			// neither aborts the unrelated writes in this transaction nor unwinds
			// past the individual-retry fallback below (a panic would otherwise
			// escape RunInTx entirely and silently drop the whole group). If a
			// failure poisons the SQLite transaction, Commit fails and the
			// fallback retries each task.
			a.applyTask(ctx, repo, t)
		}
		return nil
	})
	if err != nil {
		logrus.WithError(err).WithField("batch", len(group)).Warn("async group commit failed; retrying individually")
		for _, t := range group {
			a.runOne(ctx, a.IChatStorageRepository, t)
		}
		return
	}
	a.committed.Add(1)
	a.processed.Add(int64(len(group)))
}

// applyTask runs one task's write against repo, isolating both a returned error
// and a panic so a single bad write never aborts the surrounding batch nor kills
// the worker. It does not advance the processed counter — callers do.
func (a *AsyncRepository) applyTask(ctx context.Context, repo domainChatStorage.IChatStorageRepository, t asyncTask) {
	defer func() {
		if rec := recover(); rec != nil {
			a.failed.Add(1)
			logrus.WithField("panic", rec).WithField("op", t.name).Error("async chat storage task panicked")
		}
	}()
	a.logFail(t.name, t.apply(ctx, repo))
}

// runOne executes a single task against repo (solo tasks and the individual
// retry fallback), isolating panics so one bad task never kills the worker.
func (a *AsyncRepository) runOne(ctx context.Context, repo domainChatStorage.IChatStorageRepository, t asyncTask) {
	a.applyTask(ctx, repo, t)
	a.processed.Add(1)
}

// enqueue submits a write task without blocking the caller. When the queue is
// full the task is dropped and counted. The channel is never closed, so a task
// submitted concurrently with Close is harmless (it is simply left undrained).
func (a *AsyncRepository) enqueue(task asyncTask) {
	select {
	case <-a.stop:
		return
	default:
	}
	select {
	case a.queue <- task:
		a.enqueued.Add(1)
	default:
		a.dropped.Add(1)
	}
}

// Stats returns a snapshot of the write-behind counters.
func (a *AsyncRepository) Stats() AsyncStats {
	return AsyncStats{
		Enqueued:  a.enqueued.Load(),
		Dropped:   a.dropped.Load(),
		Processed: a.processed.Load(),
		Failed:    a.failed.Load(),
		Committed: a.committed.Load(),
		QueueLen:  int64(len(a.queue)),
	}
}

// Close stops accepting new writes, drains the buffered ones, and waits for the
// worker to finish (bounded by ctx). Safe to call more than once.
func (a *AsyncRepository) Close(ctx context.Context) error {
	a.closeMu.Do(func() { close(a.stop) })
	done := make(chan struct{})
	go func() { a.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *AsyncRepository) logFail(op string, err error) {
	if err != nil {
		a.failed.Add(1)
		logrus.WithError(err).WithField("op", op).Debug("async chat storage write failed")
	}
}

// --- overridden hot-path writes (enqueue + return immediately) ---

func (a *AsyncRepository) StoreSentMessageWithContext(_ context.Context, messageID, senderJID, recipientJID, content string, timestamp time.Time, msg *waE2E.Message) error {
	a.enqueue(asyncTask{name: "StoreSentMessage", apply: func(ctx context.Context, repo domainChatStorage.IChatStorageRepository) error {
		return repo.StoreSentMessageWithContext(ctx, messageID, senderJID, recipientJID, content, timestamp, msg)
	}})
	return nil
}

func (a *AsyncRepository) StoreMessage(message *domainChatStorage.Message) error {
	a.enqueue(asyncTask{name: "StoreMessage", apply: func(_ context.Context, repo domainChatStorage.IChatStorageRepository) error {
		return repo.StoreMessage(message)
	}})
	return nil
}

// StoreMessagesBatch already writes in its own transaction, so it runs solo:
// nesting it inside the group-commit transaction would deadlock SQLite's single
// writer.
func (a *AsyncRepository) StoreMessagesBatch(messages []*domainChatStorage.Message) error {
	a.enqueue(asyncTask{name: "StoreMessagesBatch", solo: true, apply: func(_ context.Context, repo domainChatStorage.IChatStorageRepository) error {
		return repo.StoreMessagesBatch(messages)
	}})
	return nil
}

func (a *AsyncRepository) StoreMessageEdit(edit *domainChatStorage.MessageEdit) error {
	a.enqueue(asyncTask{name: "StoreMessageEdit", apply: func(_ context.Context, repo domainChatStorage.IChatStorageRepository) error {
		return repo.StoreMessageEdit(edit)
	}})
	return nil
}

func (a *AsyncRepository) StoreChat(chat *domainChatStorage.Chat) error {
	a.enqueue(asyncTask{name: "StoreChat", apply: func(_ context.Context, repo domainChatStorage.IChatStorageRepository) error {
		return repo.StoreChat(chat)
	}})
	return nil
}

func (a *AsyncRepository) CreateMessage(_ context.Context, evt *events.Message) error {
	a.enqueue(asyncTask{name: "CreateMessage", apply: func(ctx context.Context, repo domainChatStorage.IChatStorageRepository) error {
		return repo.CreateMessage(ctx, evt)
	}})
	return nil
}

func (a *AsyncRepository) CreateReaction(_ context.Context, evt *events.Message) error {
	a.enqueue(asyncTask{name: "CreateReaction", apply: func(ctx context.Context, repo domainChatStorage.IChatStorageRepository) error {
		return repo.CreateReaction(ctx, evt)
	}})
	return nil
}

func (a *AsyncRepository) CreateIncomingCallRecord(_ context.Context, evt *events.CallOffer, autoRejected bool) error {
	a.enqueue(asyncTask{name: "CreateIncomingCallRecord", apply: func(ctx context.Context, repo domainChatStorage.IChatStorageRepository) error {
		return repo.CreateIncomingCallRecord(ctx, evt, autoRejected)
	}})
	return nil
}
