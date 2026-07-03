package sqlite

import (
	"strings"
	"testing"
)

// FormatChatStorageURI must enable WAL, a busy timeout, and synchronous=NORMAL
// so heavy concurrent writers don't fsync on every commit or fail instantly on
// lock contention. The exact query syntax differs between the cgo and purego
// builds, so assert on case-insensitive substrings that hold for both.
func TestFormatChatStorageURIEnablesWALTuning(t *testing.T) {
	got := strings.ToLower(FormatChatStorageURI("file:storages/whatsapp.db", true, true))

	for _, want := range []string{"wal", "busy_timeout", "synchronous", "30000"} {
		if !strings.Contains(got, want) {
			t.Fatalf("FormatChatStorageURI missing %q: %s", want, got)
		}
	}
	// NORMAL (or its numeric form 1) — not the FULL default.
	if !strings.Contains(got, "normal") && !strings.Contains(got, "synchronous(1)") && !strings.Contains(got, "synchronous=1") {
		t.Fatalf("expected synchronous NORMAL, got: %s", got)
	}
}

func TestFormatChatStorageURINoWALLeavesBaseWhenDisabled(t *testing.T) {
	got := FormatChatStorageURI("file:storages/whatsapp.db", false, false)
	if strings.Contains(strings.ToLower(got), "synchronous") {
		t.Fatalf("synchronous must only be set with WAL enabled, got: %s", got)
	}
}
