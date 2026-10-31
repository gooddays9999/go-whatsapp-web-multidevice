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
	s.metricsSnapshot.Store(&bridgeMetricsSnapshot{sampledAt: time.Now(), accounts: 100, online: 81, unknown: 2, outbox: outboxStats{Pending: 12}})
	w := httptest.NewRecorder()
	s.handleMetrics(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `bridge_active_connections{worker_id="test"} 81`) || !strings.Contains(w.Body.String(), "bridge_nats_outbox_pending 12") || !strings.Contains(w.Body.String(), "bridge_accounts_state_unknown 2") {
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
	p.recordEnqueueFailure(time.Now())
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

func TestOutboxHealthRecoveryPreservesCounterAndRejectsConcurrentFailure(t *testing.T) {
	p := &NATSPublisher{}
	now := time.Now()
	p.recordEnqueueFailure(now)
	p.recoverEnqueueHealth(1, now.Add(29*time.Second))
	if p.enqueueHealthy() {
		t.Fatal("recovered before quiet period")
	}
	p.recoverEnqueueHealth(1, now.Add(31*time.Second))
	if !p.enqueueHealthy() || p.enqueueFailures.Load() != 1 {
		t.Fatal("historical failure did not recover without clearing counter")
	}
	p.recordEnqueueFailure(now.Add(32 * time.Second))
	p.recoverEnqueueHealth(1, now.Add(time.Minute))
	if p.enqueueHealthy() {
		t.Fatal("old probe hid a newer failure")
	}
	p.recoverEnqueueHealth(2, now.Add(63*time.Second))
	if !p.enqueueHealthy() || p.enqueueFailures.Load() != 2 {
		t.Fatal("new failure did not recover")
	}
	s := &Service{publisher: p}
	s.metricsSnapshot.Store(&bridgeMetricsSnapshot{sampledAt: time.Now(), managerReady: true, outboxHealthy: true})
	w := httptest.NewRecorder()
	s.handleReady(w, httptest.NewRequest("GET", "/ready", nil))
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["outbox"] != true || body["nats"] != false {
		t.Fatalf("wrong dependency recovery: %s", w.Body.String())
	}
	s.metricsSnapshot.Store(&bridgeMetricsSnapshot{sampledAt: time.Now(), managerReady: true, outboxHealthy: false})
	w = httptest.NewRecorder()
	s.handleReady(w, httptest.NewRequest("GET", "/ready", nil))
	if !strings.Contains(w.Body.String(), `"outbox":false`) {
		t.Fatal("failed probe reported healthy")
	}
}
