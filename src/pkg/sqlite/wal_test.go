package sqlite

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestCheckpointWALTruncatesFile proves the core of the WAL-bloat fix: a
// TRUNCATE checkpoint reclaims a WAL that was allowed to grow (the exact failure
// that filled the disk and crash-looped the bridge).
func TestCheckpointWALTruncatesFile(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "chat.db")
	walPath := dbPath + "-wal"

	uri := FormatChatStorageURI("file:"+dbPath, true, false)
	db, err := sql.Open(DriverName, uri)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1) // deterministic: one connection, no competing reader

	// Disable autocheckpoint so the WAL grows without being reclaimed, mirroring
	// the production condition where checkpoints could not truncate it.
	if _, err := db.Exec("PRAGMA wal_autocheckpoint=0"); err != nil {
		t.Fatalf("disable autocheckpoint: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	blob := strings.Repeat("x", 400)
	for i := range 6000 {
		if _, err := db.Exec("INSERT INTO t (data) VALUES (?)", blob); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	before := walSize(t, walPath)
	if before == 0 {
		t.Fatal("expected WAL to have grown before checkpoint, got 0 bytes")
	}

	busy, logFrames, checkpointed, err := CheckpointWAL(db)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	after := walSize(t, walPath)
	t.Logf("checkpoint busy=%d log=%d checkpointed=%d; wal bytes before=%d after=%d",
		busy, logFrames, checkpointed, before, after)

	if busy != 0 {
		t.Fatalf("checkpoint unexpectedly blocked (busy=%d) on a single connection", busy)
	}
	// The essential guarantee: the -wal file is reclaimed instead of growing
	// unbounded.
	if after >= before {
		t.Fatalf("WAL not truncated: before=%d after=%d", before, after)
	}
}

// TestStartWALCheckpointerStops verifies lifecycle: the checkpointer runs and
// its stop function returns cleanly without leaking or panicking.
func TestStartWALCheckpointerStops(t *testing.T) {
	dir := t.TempDir()
	uri := FormatChatStorageURI("file:"+filepath.Join(dir, "c.db"), true, false)
	db, err := sql.Open(DriverName, uri)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	stop := StartWALCheckpointer(db, 10*time.Millisecond)
	time.Sleep(50 * time.Millisecond) // let it tick a few times
	stop()

	// A nil db / non-positive interval must be a safe no-op.
	StartWALCheckpointer(nil, time.Second)()
	StartWALCheckpointer(db, 0)()
}

func walSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}
