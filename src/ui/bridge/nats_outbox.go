package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/sqlite"
	"github.com/nats-io/nuid"
	"github.com/sirupsen/logrus"
)

const outboxBatchSize = 256

type eventOutbox struct {
	db       *sql.DB
	mu       sync.Mutex
	pending  []*outboxInsert
	flushing bool
}

type outboxInsert struct {
	ctx              context.Context
	eventID, subject string
	payload          []byte
	created          int64
	done             chan error
}

type outboxEvent struct {
	id      int64
	eventID string
	subject string
	payload []byte
	created int64
}

func openEventOutbox(path string) (*eventOutbox, error) {
	maxBytes := int64(1 << 30)
	if raw := strings.TrimSpace(os.Getenv("BRIDGE_NATS_OUTBOX_MAX_BYTES")); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 1<<20 || value > 1<<40 {
			return nil, fmt.Errorf("BRIDGE_NATS_OUTBOX_MAX_BYTES must be an integer between 1048576 and 1099511627776")
		}
		maxBytes = value
	}
	if path == "" {
		path = "storages/bridge-events.db"
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, fmt.Errorf("create bridge outbox directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0600); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := file.Close(); err != nil {
		return nil, err
	}
	db, err := sql.Open(sqlite.DriverName, path)
	if err != nil {
		return nil, err
	}
	// Isolated from the chat DB pool. One connection keeps these PRAGMAs active
	// for every write; FULL makes a committed enqueue survive a power loss.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, statement := range []string{
		"PRAGMA busy_timeout=2000",
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		`CREATE TABLE IF NOT EXISTS bridge_event_outbox (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			event_id TEXT NOT NULL UNIQUE,
			subject TEXT NOT NULL,
			payload BLOB NOT NULL,
			created_ms INTEGER NOT NULL,
			failure TEXT NOT NULL DEFAULT ''
		)`,
		"CREATE INDEX IF NOT EXISTS bridge_event_pending ON bridge_event_outbox(failure, id)",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("initialize bridge outbox: %w", err)
		}
	}
	// Bound the spool (excluding WAL). Hitting this limit is an
	// explicit enqueue failure and readiness failure, never silent eviction.
	var pageSize int64
	if err := db.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err != nil {
		_ = db.Close()
		return nil, err
	}
	var maxPages int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("PRAGMA max_page_count=%d", maxBytes/pageSize)).Scan(&maxPages); err != nil {
		_ = db.Close()
		return nil, err
	}
	// SQLite cannot shrink below an existing file's page count. Never delete
	// retained events to satisfy a smaller configuration on a later restart.
	logrus.WithFields(logrus.Fields{"configured_bytes": maxBytes, "effective_bytes": maxPages * pageSize}).Info("bridge outbox capacity configured")
	return &eventOutbox{db: db}, nil
}

func (o *eventOutbox) enqueue(ctx context.Context, eventID, subject string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	insert := &outboxInsert{ctx: ctx, eventID: eventID, subject: subject, payload: payload, created: time.Now().UnixMilli(), done: make(chan error, 1)}
	o.mu.Lock()
	o.pending = append(o.pending, insert)
	if !o.flushing {
		o.flushing = true
		go o.flushInserts()
	}
	o.mu.Unlock()
	select {
	case err := <-insert.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Share one FULL-durability commit across simultaneous account callbacks;
// acknowledging an enqueue still waits for its transaction to commit.
func (o *eventOutbox) flushInserts() {
	for {
		time.Sleep(2 * time.Millisecond)
		o.mu.Lock()
		if len(o.pending) == 0 {
			o.flushing = false
			o.mu.Unlock()
			return
		}
		n := min(len(o.pending), outboxBatchSize)
		batch := o.pending[:n:n]
		o.pending = o.pending[n:]
		o.mu.Unlock()
		active := make([]*outboxInsert, 0, len(batch))
		for _, insert := range batch {
			if err := insert.ctx.Err(); err != nil {
				insert.done <- err
			} else {
				active = append(active, insert)
			}
		}
		if len(active) == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := o.commitInserts(ctx, active)
		cancel()
		for _, insert := range active {
			insert.done <- err
		}
	}
}

func (o *eventOutbox) commitInserts(ctx context.Context, batch []*outboxInsert) error {
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO bridge_event_outbox(event_id, subject, payload, created_ms) VALUES(?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, insert := range batch {
		if _, err := stmt.ExecContext(ctx, insert.eventID, insert.subject, insert.payload, insert.created); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Insert and delete inside one committed transaction: exercise durable writes
// without ever making a probe visible to the NATS sender.
func (o *eventOutbox) probe(ctx context.Context) error {
	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	id := "probe-" + nuid.Next()
	if _, err = tx.ExecContext(ctx, `INSERT INTO bridge_event_outbox(event_id,subject,payload,created_ms) VALUES(?,?,?,?)`, id, "internal.probe", []byte("{}"), time.Now().UnixMilli()); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM bridge_event_outbox WHERE event_id=?", id); err != nil {
		return err
	}
	return tx.Commit()
}

func (o *eventOutbox) batch(ctx context.Context) ([]outboxEvent, error) {
	rows, err := o.db.QueryContext(ctx, `SELECT id,event_id,subject,payload,created_ms FROM bridge_event_outbox WHERE failure='' ORDER BY id LIMIT ?`, outboxBatchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []outboxEvent
	for rows.Next() {
		var event outboxEvent
		if err := rows.Scan(&event.id, &event.eventID, &event.subject, &event.payload, &event.created); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (o *eventOutbox) ack(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	_, err := o.db.ExecContext(ctx, "DELETE FROM bridge_event_outbox WHERE id IN ("+strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")+")", args...)
	return err
}

func (o *eventOutbox) quarantine(ctx context.Context, id int64, failure string) error {
	_, err := o.db.ExecContext(ctx, "UPDATE bridge_event_outbox SET failure=? WHERE id=?", failure, id)
	return err
}

type outboxStats struct {
	Pending     int64
	Quarantined int64
	Oldest      int64
	UsedBytes   int64
	MaxBytes    int64
}

func (o *eventOutbox) stats(ctx context.Context) (outboxStats, error) {
	var stats outboxStats
	err := o.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(failure=''),0), COALESCE(SUM(failure<>''),0), COALESCE(MIN(CASE WHEN failure='' THEN created_ms END),0) FROM bridge_event_outbox`).Scan(&stats.Pending, &stats.Quarantined, &stats.Oldest)
	if err != nil {
		return stats, err
	}
	var pages, freePages, pageSize, maxPages int64
	for _, query := range []struct {
		sql  string
		dest *int64
	}{
		{"PRAGMA page_count", &pages},
		{"PRAGMA freelist_count", &freePages},
		{"PRAGMA page_size", &pageSize},
		{"PRAGMA max_page_count", &maxPages},
	} {
		if err := o.db.QueryRowContext(ctx, query.sql).Scan(query.dest); err != nil {
			return stats, err
		}
	}
	stats.UsedBytes, stats.MaxBytes = (pages-freePages)*pageSize, maxPages*pageSize
	return stats, err
}
