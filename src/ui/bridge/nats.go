package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"github.com/sirupsen/logrus"
)

type NATSPublisher struct {
	mu              sync.RWMutex
	conn            *nats.Conn
	closed          bool
	url             string
	outbox          *eventOutbox
	cancel          context.CancelFunc
	done            chan struct{}
	wake            chan struct{}
	closeOnce       sync.Once
	published       atomic.Uint64
	rebuilds        atomic.Uint64
	failures        atomic.Uint64
	enqueueFailures atomic.Uint64
	lastEnqueueLog  atomic.Int64
	lastDial        time.Time
	nextDial        time.Time
	dialBackoff     time.Duration
}

func NewNATSPublisher(url, outboxPath string) (*NATSPublisher, error) {
	p := &NATSPublisher{url: url}
	if url == "" {
		return p, nil
	}
	outbox, err := openEventOutbox(outboxPath)
	if err != nil {
		return nil, err
	}
	p.outbox = outbox
	p.done = make(chan struct{})
	p.wake = make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	go p.run(ctx)
	return p, nil
}

func (p *NATSPublisher) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *NATSPublisher) connection() *nats.Conn {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.conn
}

// Only the sender worker replaces connections. Library callbacks never dial or
// close a connection, avoiding callback deadlocks and reconnect storms.
func (p *NATSPublisher) ensureConnection() error {
	if conn := p.connection(); conn != nil && !conn.IsClosed() {
		if conn.IsConnected() && time.Since(p.lastDial) >= 30*time.Second {
			p.dialBackoff = time.Second
		}
		return nil
	}
	if time.Now().Before(p.nextDial) {
		return errors.New("NATS connection recreation backing off")
	}
	if p.dialBackoff == 0 {
		p.dialBackoff = time.Second
	}
	p.lastDial = time.Now()
	p.nextDial = p.lastDial.Add(p.dialBackoff)
	p.dialBackoff = min(2*p.dialBackoff, 30*time.Second)
	conn, err := nats.Connect(p.url,
		nats.Name("go-whatsapp-web-ims-bridge"),
		nats.Timeout(2*time.Second),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logrus.WithError(err).Warn("bridge NATS disconnected; events retained in outbox")
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			logrus.Info("bridge NATS reconnected")
			p.signal()
		}),
		nats.ClosedHandler(func(conn *nats.Conn) {
			logrus.WithError(conn.LastError()).Warn("bridge NATS connection closed")
			p.signal()
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			logrus.WithError(err).Error("bridge NATS asynchronous error")
		}),
	)
	if err != nil {
		return err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		conn.Close()
		return nats.ErrConnectionClosed
	}
	p.conn = conn
	p.mu.Unlock()
	p.rebuilds.Add(1)
	logrus.Info("bridge NATS connection created")
	return nil
}

func (p *NATSPublisher) run(ctx context.Context) {
	defer close(p.done)
	backoff := time.Second
	for ctx.Err() == nil {
		err := p.ensureConnection()
		if err == nil && p.IsConnected() {
			err = p.deliver(ctx)
		}
		wait := 500 * time.Millisecond
		if err != nil {
			p.failures.Add(1)
			logrus.WithError(err).Warn("bridge NATS delivery paused; retrying retained events")
			wait = backoff
			backoff = min(backoff*2, 30*time.Second)
		} else {
			backoff = time.Second
		}
		timer := time.NewTimer(wait)
		// On failure, incoming events must not defeat the retry backoff.
		if err != nil {
			select {
			case <-ctx.Done():
			case <-timer.C:
			}
		} else {
			select {
			case <-ctx.Done():
			case <-timer.C:
			case <-p.wake:
			}
		}
		timer.Stop()
	}
}

func (p *NATSPublisher) deliver(ctx context.Context) error {
	events, err := p.outbox.batch(ctx)
	if err != nil || len(events) == 0 {
		return err
	}
	conn := p.connection()
	ids := make([]int64, 0, len(events))
	for _, event := range events {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Heartbeats describe a moment in time, not durable business events.
		if event.subject == "account.heartbeat_batch" && time.Since(time.UnixMilli(event.created)) > 90*time.Second {
			if err := p.outbox.ack(ctx, []int64{event.id}); err != nil {
				return err
			}
			continue
		}
		msg := &nats.Msg{Subject: event.subject, Data: event.payload, Header: nats.Header{}}
		msg.Header.Set("Nats-Msg-Id", event.eventID)
		if err := conn.PublishMsg(msg); err != nil {
			if errors.Is(err, nats.ErrMaxPayload) || errors.Is(err, nats.ErrBadSubject) {
				// Preserve poison events for inspection without blocking every account.
				if err := p.outbox.quarantine(ctx, event.id, err.Error()); err != nil {
					return err
				}
				logrus.WithFields(logrus.Fields{"event_id": event.eventID, "subject": event.subject, "bytes": len(event.payload)}).Error("bridge NATS event quarantined")
				continue
			}
			return err
		}
		ids = append(ids, event.id)
	}
	if len(ids) == 0 {
		return nil
	}
	// Core NATS PONG confirms server receipt, not consumer processing. A crash
	// between this flush and the delete replays the same stable event IDs.
	if err := conn.FlushTimeout(2 * time.Second); err != nil {
		if errors.Is(err, nats.ErrTimeout) {
			// Reset only the event-bus transport; WhatsApp clients stay connected.
			_ = conn.ForceReconnect()
		}
		return err
	}
	if err := p.outbox.ack(ctx, ids); err != nil {
		return err
	}
	p.published.Add(uint64(len(ids)))
	if len(events) == outboxBatchSize {
		p.signal()
	}
	return nil
}

func (p *NATSPublisher) Close() {
	if p == nil {
		return
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		conn := p.conn
		p.mu.Unlock()
		if p.cancel != nil {
			p.cancel()
			<-p.done
		}
		if conn != nil {
			conn.Close()
		}
		if p.outbox != nil {
			_ = p.outbox.db.Close()
		}
	})
}

func (p *NATSPublisher) IsConnected() bool {
	if p == nil {
		return false
	}
	conn := p.connection()
	return conn != nil && conn.IsConnected()
}

func (p *NATSPublisher) Count() uint64 {
	if p == nil {
		return 0
	}
	return p.published.Load()
}

func (p *NATSPublisher) Publish(ctx context.Context, eventType string, payload map[string]any) {
	if err := p.enqueue(ctx, eventType, payload); err != nil {
		p.failures.Add(1)
		p.enqueueFailures.Add(1)
		now := time.Now().UnixNano()
		previous := p.lastEnqueueLog.Load()
		if (previous == 0 || now-previous >= int64(30*time.Second)) && p.lastEnqueueLog.CompareAndSwap(previous, now) {
			logrus.WithError(err).WithFields(logrus.Fields{"subject": eventType, "failures": p.enqueueFailures.Load()}).Error("bridge event could not be persisted; readiness disabled")
		}
	}
}

func (p *NATSPublisher) enqueue(ctx context.Context, eventType string, payload map[string]any) error {
	if p == nil || p.url == "" || eventType == "" {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nats.ErrConnectionClosed
	}
	// Copy the caller's map; event callbacks may retain or reuse it.
	event := make(map[string]any, len(payload)+3)
	for key, value := range payload {
		event[key] = value
	}
	eventID := nuid.Next()
	event["eventId"] = eventID
	if _, ok := event["type"]; !ok {
		event["type"] = eventType
	}
	if _, ok := event["timestamp"]; !ok {
		event["timestamp"] = time.Now().UnixMilli()
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := p.outbox.enqueue(ctx, eventID, eventType, data); err != nil {
		return err
	}
	p.signal()
	return nil
}
