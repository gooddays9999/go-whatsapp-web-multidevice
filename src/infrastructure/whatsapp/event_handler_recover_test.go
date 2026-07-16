package whatsapp

import (
	"context"
	"testing"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	"go.mau.fi/whatsmeow/types/events"
)

// panicChatStorage embeds the interface (all methods present as nil) and overrides
// only GetMessageByID to panic, simulating a chatstorage-layer panic during event
// processing (the class of crash that took down the whole multi-account bridge).
type panicChatStorage struct {
	domainChatStorage.IChatStorageRepository
}

func (panicChatStorage) GetMessageByID(string) (*domainChatStorage.Message, error) {
	panic("simulated chatstorage panic during event processing")
}

// TestHandlerRecoversPanicFromEventProcessing guards fault isolation: a panic while
// processing one account's event must NOT propagate out of handler (which runs in
// whatsmeow's shared event goroutine) and crash the whole process.
func TestHandlerRecoversPanicFromEventProcessing(t *testing.T) {
	inst := NewDeviceInstance("test-device", nil, panicChatStorage{})

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("handler propagated panic instead of recovering: %v", r)
		}
	}()

	// *events.DeleteForMe routes to handleDeleteForMe -> GetMessageByID -> panic.
	handler(context.Background(), inst, &events.DeleteForMe{MessageID: "msg-1"})
}
