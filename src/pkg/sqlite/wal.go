package sqlite

import (
	"database/sql"
	"time"

	"github.com/sirupsen/logrus"
)

// CheckpointWAL runs a TRUNCATE checkpoint on a WAL-mode SQLite database. It
// writes committed WAL frames back into the main database file and then
// truncates the -wal file to zero. It returns the raw PRAGMA result:
//   - busy: 1 if an open reader prevented full truncation (the checkpoint still
//     runs, but the -wal file cannot be shrunk past the reader's snapshot).
//   - logFrames: number of frames in the WAL at checkpoint time.
//   - checkpointed: number of frames written back to the main database.
func CheckpointWAL(db *sql.DB) (busy, logFrames, checkpointed int, err error) {
	if db == nil {
		return 0, 0, 0, nil
	}
	err = db.QueryRow("PRAGMA wal_checkpoint(TRUNCATE)").Scan(&busy, &logFrames, &checkpointed)
	return busy, logFrames, checkpointed, err
}

// StartWALCheckpointer periodically truncates the chat-storage WAL so it cannot
// grow unbounded and fill the disk.
//
// Background: in WAL mode a lingering read snapshot pins the WAL and prevents
// checkpoints from reclaiming space. Under heavy concurrent writes (e.g. a mass
// history sync across tens of thousands of accounts) this let the -wal file grow
// to hundreds of GB, fill the disk, and crash the process. Recycling pool
// connections (SetConnMaxLifetime) releases those snapshots; this ticker then
// truncates the file back down. Returns a stop function for graceful shutdown.
func StartWALCheckpointer(db *sql.DB, interval time.Duration) (stop func()) {
	if db == nil || interval <= 0 {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				busy, logFrames, checkpointed, err := CheckpointWAL(db)
				if err != nil {
					logrus.WithError(err).Warn("chatstorage WAL checkpoint failed")
					continue
				}
				if busy != 0 {
					logrus.WithFields(logrus.Fields{
						"wal_frames":          logFrames,
						"checkpointed_frames": checkpointed,
					}).Warn("chatstorage WAL checkpoint blocked by an open reader; WAL not fully truncated (will retry)")
				}
			}
		}
	}()
	return func() { close(done) }
}
