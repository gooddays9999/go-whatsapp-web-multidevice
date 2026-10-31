package whatsmeow

import "testing"

func TestTryIsConnectedDoesNotWaitForBusySocket(t *testing.T) {
	cli := &Client{}
	cli.socketLock.Lock()
	defer cli.socketLock.Unlock()
	if connected, known := cli.TryIsConnected(); connected || known {
		t.Fatalf("busy socket must be unknown, got connected=%t known=%t", connected, known)
	}
}

func TestTryIsConnectedNilAndDisconnected(t *testing.T) {
	for _, cli := range []*Client{nil, {}} {
		if connected, known := cli.TryIsConnected(); connected || !known {
			t.Fatalf("missing socket must be disconnected, got connected=%t known=%t", connected, known)
		}
	}
}
