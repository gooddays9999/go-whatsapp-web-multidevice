package whatsmeow

import (
	"testing"

	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
)

func TestGetTypeFromMessageTreatsPollCreationV3AsPoll(t *testing.T) {
	msg := &waE2E.Message{PollCreationMessageV3: &waE2E.PollCreationMessage{}}

	if got := getTypeFromMessage(msg); got != "poll" {
		t.Fatalf("getTypeFromMessage(PollCreationMessageV3) = %q, want poll", got)
	}
}
