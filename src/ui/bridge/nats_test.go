package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func eventuallyNATS(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("NATS condition not reached before timeout")
}

// Set NATS_SERVER_BIN to run failure-injection tests with a real broker.
func startTestNATS(t *testing.T, port int, maxPayload int) (string, func()) {
	t.Helper()
	bin := os.Getenv("NATS_SERVER_BIN")
	if bin == "" {
		var err error
		bin, err = exec.LookPath("nats-server")
		if err != nil {
			t.Skip("real-broker tests require nats-server or NATS_SERVER_BIN")
		}
	}
	if port == 0 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port = listener.Addr().(*net.TCPAddr).Port
		_ = listener.Close()
	}
	config := filepath.Join(t.TempDir(), "nats.conf")
	if err := os.WriteFile(config, []byte(fmt.Sprintf("host: 127.0.0.1\nport: %d\nmax_payload: %d\n", port, maxPayload)), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-c", config)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }) }
	t.Cleanup(stop)
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	eventuallyNATS(t, func() bool {
		conn, err := nats.Connect(url, nats.Timeout(100*time.Millisecond), nats.NoReconnect())
		if err != nil {
			return false
		}
		conn.Close()
		return true
	})
	return url, stop
}

func testPublisher(t *testing.T, url, path string) *NATSPublisher {
	t.Helper()
	p, err := NewNATSPublisher(url, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func testSubscriber(t *testing.T, url string) *nats.Subscription {
	t.Helper()
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	sub, err := conn.SubscribeSync(">")
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	return sub
}

func TestNATSRebuildsTerminalConnectionRepeatedly(t *testing.T) {
	url, _ := startTestNATS(t, 0, 1024*1024)
	sub := testSubscriber(t, url)
	p := testPublisher(t, url, filepath.Join(t.TempDir(), "events.db"))
	eventuallyNATS(t, p.IsConnected)
	for i := 0; i < 3; i++ {
		previous := p.connection()
		previous.Close()
		p.signal()
		if err := p.enqueue(context.Background(), "message.received", map[string]any{"sequence": i}); err != nil {
			t.Fatal(err)
		}
		eventuallyNATS(t, func() bool { return p.IsConnected() && p.connection() != previous })
		msg, err := sub.NextMsg(5 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if msg.Subject != "message.received" || msg.Header.Get("Nats-Msg-Id") == "" {
			t.Fatalf("unexpected replay: %+v", msg)
		}
	}
	p.Close()
	rebuilds := p.rebuilds.Load()
	time.Sleep(600 * time.Millisecond)
	if p.IsConnected() || p.rebuilds.Load() != rebuilds {
		t.Fatal("intentional shutdown recreated a connection")
	}
	if err := p.enqueue(context.Background(), "message.received", nil); err != nats.ErrConnectionClosed {
		t.Fatalf("publish after close: %v", err)
	}
}

func TestNATSOutboxSurvivesPublisherRestart(t *testing.T) {
	url, stop := startTestNATS(t, 0, 1024*1024)
	stop()
	path := filepath.Join(t.TempDir(), "events.db")
	p := testPublisher(t, url, path)
	for i := 0; i < 3; i++ {
		if err := p.enqueue(context.Background(), "message.ack", map[string]any{"sequence": i}); err != nil {
			t.Fatal(err)
		}
	}
	before, err := p.outbox.batch(context.Background())
	if err != nil || len(before) != 3 {
		t.Fatalf("expected durable backlog, got %d: %v", len(before), err)
	}
	p.Close()
	address := strings.TrimPrefix(url, "nats://")
	_, portString, _ := net.SplitHostPort(address)
	var port int
	_, _ = fmt.Sscan(portString, &port)
	startTestNATS(t, port, 1024*1024)
	sub := testSubscriber(t, url)
	p = testPublisher(t, url, path)
	for i := 0; i < 3; i++ {
		msg, err := sub.NextMsg(5 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]any
		if err := json.Unmarshal(msg.Data, &body); err != nil {
			t.Fatal(err)
		}
		if body["sequence"] != float64(i) || body["eventId"] != before[i].eventID || msg.Header.Get("Nats-Msg-Id") != before[i].eventID {
			t.Fatalf("event identity/order changed across restart: %s", msg.Data)
		}
	}
	eventuallyNATS(t, func() bool {
		stats, err := p.outbox.stats(context.Background())
		return err == nil && stats.Pending == 0 && p.Count() == 3
	})
}

func TestNATSRecoversBrokerRestart(t *testing.T) {
	url, stop := startTestNATS(t, 0, 1024*1024)
	p := testPublisher(t, url, filepath.Join(t.TempDir(), "events.db"))
	eventuallyNATS(t, p.IsConnected)
	original := p.connection()
	stop()
	eventuallyNATS(t, func() bool { return !p.IsConnected() })
	if err := p.enqueue(context.Background(), "message.received", map[string]any{"content": "during outage"}); err != nil {
		t.Fatal(err)
	}
	stats, err := p.outbox.stats(context.Background())
	if err != nil || stats.Pending != 1 {
		t.Fatalf("outage event not retained: %+v %v", stats, err)
	}
	var port int
	_, _ = fmt.Sscanf(url, "nats://127.0.0.1:%d", &port)
	startTestNATS(t, port, 1024*1024)
	eventuallyNATS(t, p.IsConnected)
	eventuallyNATS(t, func() bool {
		stats, err := p.outbox.stats(context.Background())
		return err == nil && stats.Pending == 0 && p.Count() == 1
	})
	if original != p.connection() {
		t.Fatal("ordinary reconnect unnecessarily replaced the connection")
	}
}

func TestNATSExpiredHeartbeatNotReplayedAfterRestart(t *testing.T) {
	// Expiry is covered with a real broker above; also ensure the durable spool
	// itself preserves the expiry timestamp rather than resetting it on reopen.
	path := filepath.Join(t.TempDir(), "events.db")
	o, err := openEventOutbox(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.enqueue(context.Background(), "stable-id", "account.heartbeat_batch", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if _, err := o.db.Exec("UPDATE bridge_event_outbox SET created_ms=1"); err != nil {
		t.Fatal(err)
	}
	_ = o.db.Close()
	o, err = openEventOutbox(path)
	if err != nil {
		t.Fatal(err)
	}
	defer o.db.Close()
	batch, err := o.batch(context.Background())
	if err != nil || len(batch) != 1 || batch[0].created != 1 || batch[0].eventID != "stable-id" {
		t.Fatalf("persisted identity/expiry changed: %+v %v", batch, err)
	}
}

func TestNATSQuarantinesOversizedEventAndSkipsExpiredHeartbeat(t *testing.T) {
	url, stop := startTestNATS(t, 0, 1024)
	stop()
	p := testPublisher(t, url, filepath.Join(t.TempDir(), "events.db"))
	for _, event := range []struct{ subject, content string }{
		{"account.heartbeat_batch", "stale"},
		{"message.received", strings.Repeat("x", 2048)},
		{"message.ack", "valid"},
	} {
		if err := p.enqueue(context.Background(), event.subject, map[string]any{"content": event.content}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := p.outbox.db.Exec("UPDATE bridge_event_outbox SET created_ms=0 WHERE subject='account.heartbeat_batch'"); err != nil {
		t.Fatal(err)
	}
	var port int
	_, _ = fmt.Sscanf(url, "nats://127.0.0.1:%d", &port)
	startTestNATS(t, port, 1024)
	sub := testSubscriber(t, url)
	msg, err := sub.NextMsg(8 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Subject != "message.ack" {
		t.Fatalf("unexpected event delivered: %s", msg.Subject)
	}
	eventuallyNATS(t, func() bool {
		stats, err := p.outbox.stats(context.Background())
		return err == nil && stats.Pending == 0 && stats.Quarantined == 1
	})
}

func TestNATSOutboxValidationAndConcurrentEnqueue(t *testing.T) {
	// An unreachable local port exercises startup failure without a broker.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	url := "nats://" + listener.Addr().String()
	_ = listener.Close()
	p := testPublisher(t, url, filepath.Join(t.TempDir(), "events.db"))
	payload := map[string]any{"accountId": "27"}
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.enqueue(context.Background(), "message.received", payload); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if len(payload) != 1 {
		t.Fatal("publisher mutated caller payload")
	}
	stats, err := p.outbox.stats(context.Background())
	if err != nil || stats.Pending != 40 {
		t.Fatalf("unexpected backlog: %+v, %v", stats, err)
	}
	p.Publish(context.Background(), "invalid", map[string]any{"bad": make(chan int)})
	if p.enqueueFailures.Load() != 1 {
		t.Fatal("serialization failure was not surfaced")
	}
}

func TestNATSOutboxStartupFailureIsFatal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if p, err := NewNATSPublisher("nats://127.0.0.1:1", filepath.Join(path, "events.db")); err == nil {
		p.Close()
		t.Fatal("outbox failure silently disabled durability")
	}
}

func TestNATSDeleteFailureReplaysSameEventID(t *testing.T) {
	url, _ := startTestNATS(t, 0, 1024*1024)
	sub := testSubscriber(t, url)
	conn, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	o, err := openEventOutbox(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	// Drive the sender synchronously to inject a failure after broker flush.
	p := &NATSPublisher{url: url, conn: conn, outbox: o}
	defer p.Close()
	if err := p.enqueue(context.Background(), "message.ack", map[string]any{"ack": 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := o.db.Exec(`CREATE TRIGGER reject_ack BEFORE DELETE ON bridge_event_outbox BEGIN SELECT RAISE(FAIL, 'injected delete failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := p.deliver(context.Background()); err == nil {
		t.Fatal("injected delete failure was ignored")
	}
	first, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := o.db.Exec("DROP TRIGGER reject_ack"); err != nil {
		t.Fatal(err)
	}
	if err := p.deliver(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if first.Header.Get("Nats-Msg-Id") == "" || first.Header.Get("Nats-Msg-Id") != second.Header.Get("Nats-Msg-Id") || string(first.Data) != string(second.Data) {
		t.Fatal("retry changed the event identity or payload")
	}
}
