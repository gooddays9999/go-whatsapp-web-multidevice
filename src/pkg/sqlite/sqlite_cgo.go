//go:build !purego

package sqlite

import (
	"net/url"

	_ "github.com/mattn/go-sqlite3"
)

const DriverName = "sqlite3"

// FormatChatStorageURI formats the URI for chat storage using standard SQLite syntax
func FormatChatStorageURI(baseURI string, enableWAL bool, enableFK bool) string {
	u, err := url.Parse(baseURI)
	if err != nil {
		return baseURI
	}

	q := u.Query()
	if enableWAL {
		q.Set("_journal_mode", "WAL")
		q.Set("_busy_timeout", "30000")
		// In WAL mode NORMAL is crash-safe (no corruption; only the last
		// transaction can be lost on OS/power failure) and avoids a full fsync
		// on every commit, which is a major throughput win under heavy
		// concurrent writes. FULL (the SQLite default) fsyncs each commit.
		q.Set("_synchronous", "NORMAL")
	}
	if enableFK {
		q.Set("_foreign_keys", "on")
	}
	u.RawQuery = q.Encode()
	return u.String()
}
