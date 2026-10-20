package bridge

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/sqlite"
)

func historyRecoveryTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(sqlite.DriverName, filepath.Join(t.TempDir(), "history.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, err = db.Exec(`CREATE TABLE messages (
		id TEXT NOT NULL,chat_jid TEXT NOT NULL,device_id TEXT NOT NULL,
		sender TEXT,content TEXT,timestamp TIMESTAMP NOT NULL,is_from_me BOOLEAN,media_type TEXT,
		PRIMARY KEY(id,chat_jid,device_id));
		CREATE INDEX idx_messages_device ON messages(device_id);
		CREATE INDEX idx_messages_chat_jid ON messages(chat_jid);`)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestHistoryRecoveryReadScopesAndOriginalIDs(t *testing.T) {
	db := historyRecoveryTestDB(t)
	r := validRecoveryRequest()
	r.Since = r.Since.In(time.FixedZone("CST", 8*3600))
	r.Until = r.Until.In(time.FixedZone("CST", 8*3600))
	r.MessageIDs = []string{"real", "real", "outside", "outgoing"}
	for _, row := range []struct {
		id, chat, device string
		at               time.Time
		fromMe           bool
	}{
		{"real", r.ChatJID, "legacy", r.Since.Add(time.Minute), false},
		{"real", r.ChatJID, "other-device", r.Since.Add(time.Minute), false},
		{"real", "other@s.whatsapp.net", "legacy", r.Since.Add(time.Minute), false},
		{"not-requested", r.ChatJID, "legacy", r.Since.Add(time.Minute), false},
		{"outside", r.ChatJID, "legacy", r.Until, false},
		{"outgoing", r.ChatJID, "legacy", r.Since.Add(time.Minute), true},
	} {
		if _, err := db.Exec(`INSERT INTO messages VALUES (?,?,?,?,?,?,?,NULL)`, row.id, row.chat, row.device, row.chat, "original reply", row.at.UTC(), row.fromMe); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := readHistoryRecoveryMessages(context.Background(), db, r, "legacy")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != "real" || rows[0].DeviceID != "legacy" || rows[0].Content != "original reply" || !rows[0].Timestamp.Equal(r.Since.Add(time.Minute)) {
		t.Fatalf("incorrect scoped recovery: %+v", rows)
	}
}

func TestHistoryRecoveryReadUsesSelectiveIndexes(t *testing.T) {
	db := historyRecoveryTestDB(t)
	for _, withIDs := range []bool{false, true} {
		r := validRecoveryRequest()
		want := "idx_messages_chat_jid"
		if withIDs {
			r.MessageIDs = []string{"real"}
			want = "sqlite_autoindex_messages_1"
		}
		query, args := historyRecoverySQL(r, "legacy", true)
		rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
		if err != nil {
			t.Fatal(err)
		}
		var plan strings.Builder
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				t.Fatal(err)
			}
			plan.WriteString(detail)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if !strings.Contains(plan.String(), want) || strings.Contains(plan.String(), "idx_messages_device") {
			t.Fatalf("query chose an unselective index: %s", plan.String())
		}
	}
}

func TestHistoryRecoveryReadHonorsCancellation(t *testing.T) {
	db := historyRecoveryTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := readHistoryRecoveryMessages(ctx, db, validRecoveryRequest(), "legacy")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}
