package bridge

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetricsUsesSnapshotWithoutDeviceOrDatabaseAccess(t *testing.T) {
	s := &Service{workerID: "test"}
	s.metricsSnapshot.Store(&bridgeMetricsSnapshot{sampledAt: time.Now(), accounts: 100, online: 81, outbox: outboxStats{Pending: 12}})
	w := httptest.NewRecorder()
	s.handleMetrics(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `bridge_active_connections{worker_id="test"} 81`) || !strings.Contains(w.Body.String(), "bridge_nats_outbox_pending 12") {
		t.Fatalf("unexpected metrics: %d %s", w.Code, w.Body.String())
	}
}

func TestMetricsDoesNotAdvertiseStaleOnlineCount(t *testing.T) {
	for _, snapshot := range []*bridgeMetricsSnapshot{nil, {sampledAt: time.Now().Add(-2 * time.Minute), online: 100}} {
		s := &Service{}
		s.metricsSnapshot.Store(snapshot)
		w := httptest.NewRecorder()
		s.handleMetrics(w, httptest.NewRequest("GET", "/metrics", nil))
		if w.Code != 503 || strings.Contains(w.Body.String(), "bridge_active_connections") {
			t.Fatalf("stale count advertised: %d %s", w.Code, w.Body.String())
		}
	}
}

func TestReadinessIsNonblockingAndDistinguishesOutboxFailure(t *testing.T) {
	p := &NATSPublisher{}
	s := &Service{publisher: p}
	s.metricsSnapshot.Store(&bridgeMetricsSnapshot{sampledAt: time.Now(), managerReady: true, outboxHealthy: true})
	p.enqueueFailures.Add(1)
	w := httptest.NewRecorder()
	s.handleReady(w, httptest.NewRequest("GET", "/ready", nil))
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if w.Code != 503 || body["outbox"] != false || body["nats"] != false {
		t.Fatalf("unexpected readiness: %d %s", w.Code, w.Body.String())
	}
}
