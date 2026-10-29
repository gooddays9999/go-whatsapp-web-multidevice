package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/sqlite"
)

const outboxBatchSize = 256

type eventOutbox struct{ db *sql.DB }

type outboxEvent struct {
	id      int64
	eventID string
	subject string
	payload []byte
	created int64
}

func openEventOutbox(path string) (*eventOutbox, error) {
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
	// Bound the spool to 1 GiB (plus a small WAL). Hitting this limit is an
	// explicit enqueue failure and readiness failure, never silent eviction.
	var pageSize int64
	if err := db.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("PRAGMA max_page_count=%d", (1<<30)/pageSize)); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &eventOutbox{db: db}, nil
}

func (o *eventOutbox) enqueue(ctx context.Context, eventID, subject string, payload []byte) error {
	_, err := o.db.ExecContext(ctx, `INSERT INTO bridge_event_outbox(event_id, subject, payload, created_ms) VALUES(?,?,?,?)`, eventID, subject, payload, time.Now().UnixMilli())
	return err
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
}

func (o *eventOutbox) stats(ctx context.Context) (outboxStats, error) {
	var stats outboxStats
	err := o.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(failure=''),0), COALESCE(SUM(failure<>''),0), COALESCE(MIN(CASE WHEN failure='' THEN created_ms END),0) FROM bridge_event_outbox`).Scan(&stats.Pending, &stats.Quarantined, &stats.Oldest)
	return stats, err
}
