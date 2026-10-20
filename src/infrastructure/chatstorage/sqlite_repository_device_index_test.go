package chatstorage

import (
	"strings"
	"testing"
)

// GetDeviceRecordByJID runs for every forwarded event (per-device webhook
// lookup). Without an index on devices.jid it scanned the whole table — 57% of
// a production bridge's CPU at 22k devices.
func TestGetDeviceRecordByJIDUsesJIDIndex(t *testing.T) {
	repo := newTestSQLiteRepository(t)

	rows, err := repo.db.Query(`EXPLAIN QUERY PLAN SELECT device_id FROM devices WHERE jid = ? LIMIT 1`, "x@s.whatsapp.net")
	if err != nil {
		t.Fatalf("explain query plan: %v", err)
	}
	defer rows.Close()

	var plan []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan plan: %v", err)
		}
		plan = append(plan, detail)
	}
	joined := strings.Join(plan, "; ")
	if !strings.Contains(joined, "USING INDEX idx_devices_jid") {
		t.Fatalf("device lookup by jid does not use idx_devices_jid: %s", joined)
	}
}
