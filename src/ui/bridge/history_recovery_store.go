package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
)

// Recovery needs only the original payload, not reactions, edits or media keys.
// Keeping this read separate also lets a cancelled HTTP request interrupt SQL.
func readHistoryRecoveryMessages(ctx context.Context, db *sql.DB, req historyRecoveryRequest, deviceID string) ([]*domainChatStorage.Message, error) {
	query, args := historyRecoverySQL(req, deviceID, strings.Contains(strings.ToLower(fmt.Sprintf("%T", db.Driver())), "sqlite"))
	queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, err := db.QueryContext(queryCtx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*domainChatStorage.Message
	for rows.Next() {
		m := &domainChatStorage.Message{}
		if err := rows.Scan(&m.ID, &m.ChatJID, &m.DeviceID, &m.Sender, &m.Content, &m.Timestamp, &m.IsFromMe, &m.MediaType); err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

func historyRecoverySQL(req historyRecoveryRequest, deviceID string, isSQLite bool) (string, []any) {
	filter := historyRecoveryFilter(req, deviceID)
	index := ""
	// Old default-device caches contain millions of rows. SQLite otherwise
	// sometimes chooses idx_messages_device and scans that entire device.
	if isSQLite && len(req.MessageIDs) == 0 {
		index = " INDEXED BY idx_messages_chat_jid"
	}
	query := `SELECT id,chat_jid,device_id,COALESCE(sender,''),COALESCE(content,''),timestamp,is_from_me,COALESCE(media_type,'') FROM messages` + index +
		` WHERE chat_jid=? AND device_id=? AND timestamp>=? AND timestamp<? AND is_from_me=0`
	args := []any{req.ChatJID, deviceID, *filter.StartTime, *filter.EndTime}
	if len(req.MessageIDs) > 0 {
		ids := make(map[string]bool)
		var placeholders []string
		for _, id := range req.MessageIDs {
			if !ids[id] {
				ids[id] = true
				placeholders = append(placeholders, "?")
				args = append(args, id)
			}
		}
		query += " AND id IN (" + strings.Join(placeholders, ",") + ")"
	}
	query += " ORDER BY timestamp DESC,id DESC LIMIT 501"
	return query, args
}
