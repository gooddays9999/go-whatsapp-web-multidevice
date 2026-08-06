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
)

// AsyncConfig tunes the write-behind layer.
type AsyncConfig struct {
	// QueueSize is the number of pending write tasks buffered before new tasks
	// are dropped (best-effort). Each task is a small closure, so this can be
	// generous without meaningful memory cost.
	QueueSize int
	// WriteTimeout bounds each background write so a pathological SQLite stall
	// cannot wedge the single worker forever.
	WriteTimeout time.Duration
}

func (c AsyncConfig) withDefaults() AsyncConfig {
	if c.QueueSize <= 0 {
		c.QueueSize = defaultAsyncQueueSize
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = defaultAsyncWriteTimeout
	}
	return c
}

// AsyncStats is a snapshot of the write-behind counters.
type AsyncStats struct {
	Enqueued  int64
	Dropped   int64
	Processed int64
	Failed    int64
	QueueLen  int64
}

// AsyncRepository wraps a chat storage repository and moves the high-volume
// message/chat writes onto a single background worker so callers — above all
// the send path — never block on SQLite write-lock contention. Everything not
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
	queue   chan func(context.Context)
	stop    chan struct{}
	closeMu sync.Once
	wg      sync.WaitGroup

	enqueued  atomic.Int64
	dropped   atomic.Int64
	processed atomic.Int64
	failed    atomic.Int64
}

// NewAsyncRepository wraps base with a write-behind worker and starts it.
func NewAsyncRepository(base domainChatStorage.IChatStorageRepository, cfg AsyncConfig) *AsyncRepository {
	cfg = cfg.withDefaults()
	a := &AsyncRepository{
		IChatStorageRepository: base,
		cfg:                    cfg,
		queue:                  make(chan func(context.Context), cfg.QueueSize),
		stop:                   make(chan struct{}),
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
			a.run(task)
		case <-a.stop:
			// Drain whatever is already buffered, then exit.
			for {
				select {
				case task := <-a.queue:
					a.run(task)
				default:
					return
				}
			}
		}
	}
}

func (a *AsyncRepository) run(task func(context.Context)) {
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.WriteTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			a.failed.Add(1)
			logrus.WithField("panic", r).Error("async chat storage task panicked")
		}
	}()
	task(ctx)
	a.processed.Add(1)
}

// enqueue submits a write task without blocking the caller. When the queue is
// full the task is dropped and counted. The channel is never closed, so a task
// submitted concurrently with Close is harmless (it is simply left undrained).
func (a *AsyncRepository) enqueue(task func(context.Context)) {
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
	a.enqueue(func(ctx context.Context) {
		a.logFail("StoreSentMessage", a.IChatStorageRepository.StoreSentMessageWithContext(ctx, messageID, senderJID, recipientJID, content, timestamp, msg))
	})
	return nil
}

func (a *AsyncRepository) StoreMessage(message *domainChatStorage.Message) error {
	a.enqueue(func(context.Context) {
		a.logFail("StoreMessage", a.IChatStorageRepository.StoreMessage(message))
	})
	return nil
}

func (a *AsyncRepository) StoreMessagesBatch(messages []*domainChatStorage.Message) error {
	a.enqueue(func(context.Context) {
		a.logFail("StoreMessagesBatch", a.IChatStorageRepository.StoreMessagesBatch(messages))
	})
	return nil
}

func (a *AsyncRepository) StoreMessageEdit(edit *domainChatStorage.MessageEdit) error {
	a.enqueue(func(context.Context) {
		a.logFail("StoreMessageEdit", a.IChatStorageRepository.StoreMessageEdit(edit))
	})
	return nil
}

func (a *AsyncRepository) StoreChat(chat *domainChatStorage.Chat) error {
	a.enqueue(func(context.Context) {
		a.logFail("StoreChat", a.IChatStorageRepository.StoreChat(chat))
	})
	return nil
}

func (a *AsyncRepository) CreateMessage(_ context.Context, evt *events.Message) error {
	a.enqueue(func(ctx context.Context) {
		a.logFail("CreateMessage", a.IChatStorageRepository.CreateMessage(ctx, evt))
	})
	return nil
}

func (a *AsyncRepository) CreateReaction(_ context.Context, evt *events.Message) error {
	a.enqueue(func(ctx context.Context) {
		a.logFail("CreateReaction", a.IChatStorageRepository.CreateReaction(ctx, evt))
	})
	return nil
}

func (a *AsyncRepository) CreateIncomingCallRecord(_ context.Context, evt *events.CallOffer, autoRejected bool) error {
	a.enqueue(func(ctx context.Context) {
		a.logFail("CreateIncomingCallRecord", a.IChatStorageRepository.CreateIncomingCallRecord(ctx, evt, autoRejected))
	})
	return nil
}
