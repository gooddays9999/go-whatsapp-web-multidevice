package main

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func adaptiveMetrics(pending, age float64) map[string]float64 {
	m := healthyMetrics()
	m["bridge_nats_enqueue_failures_total"] = 0
	m["bridge_nats_outbox_pending"] = pending
	m["bridge_nats_outbox_oldest_age_seconds"] = age
	m["bridge_nats_outbox_used_bytes"] = 10 << 20
	m["bridge_nats_outbox_max_bytes"] = 8 << 30
	m["bridge_events_published_total"] = 1000
	return m
}

func TestAdaptiveGatePacesInsteadOfStoppingOnSmallBacklog(t *testing.T) {
	for _, tc := range []struct{ pending, age, rate float64 }{{100, 2, 6}, {12000, 13, 3}, {40000, 40, 1}, {100001, 10, 0}, {100, 61, 0}} {
		h := &recoveryHealth{adaptive: true, maxRate: 6}
		ok := h.evaluate(time.Now(), 200, true, true, adaptiveMetrics(tc.pending, tc.age))
		if ok != (tc.rate > 0) || h.rate != tc.rate {
			t.Fatalf("%+v: ok=%t rate=%g", tc, ok, h.rate)
		}
	}
	h := &recoveryHealth{adaptive: true, maxRate: 4}
	if !h.evaluate(time.Now(), 200, true, true, adaptiveMetrics(100, 2)) || h.rate != 4 {
		t.Fatal("ignored caller rate cap")
	}
}

func TestAdaptiveGateKeepsHardSafetyChecks(t *testing.T) {
	for key, value := range map[string]float64{
		"bridge_nats_connected": 0, "bridge_metrics_snapshot_age_seconds": 31,
		"bridge_nats_enqueue_healthy": 0, "bridge_nats_outbox_storage_healthy": 0,
		"bridge_nats_outbox_quarantined": 1, "bridge_nats_outbox_used_bytes": 8 << 30,
		"bridge_nats_outbox_max_bytes": 0, "bridge_events_published_total": math.NaN(),
		"bridge_nats_enqueue_failures_total": math.NaN(),
	} {
		h := &recoveryHealth{adaptive: true, maxRate: 6}
		m := adaptiveMetrics(100, 2)
		m[key] = value
		if h.evaluate(time.Now(), 200, true, true, m) || h.rate != 0 {
			t.Fatalf("accepted unsafe %s", key)
		}
	}
	h := &recoveryHealth{adaptive: true, maxRate: 6}
	m := adaptiveMetrics(100, 2)
	delete(m, "bridge_nats_outbox_oldest_age_seconds")
	if h.evaluate(time.Now(), 200, true, true, m) {
		t.Fatal("accepted missing queue age")
	}
	if h.evaluate(time.Now(), 503, true, true, adaptiveMetrics(100, 2)) {
		t.Fatal("bypassed readiness")
	}
}

func TestAdaptiveGateSlowsGrowthAndPausesStalledSender(t *testing.T) {
	now := time.Now()
	h := &recoveryHealth{adaptive: true, maxRate: 6}
	if !h.evaluate(now, 200, true, true, adaptiveMetrics(100, 2)) {
		t.Fatal("initial healthy sample rejected")
	}
	m := adaptiveMetrics(4000, 5)
	m["bridge_events_published_total"] = 2000
	if !h.evaluate(now.Add(5*time.Second), 200, true, true, m) || h.rate != 1 {
		t.Fatal("did not slow rapid growth")
	}
	if h.evaluate(now.Add(10*time.Second), 200, true, true, m) {
		t.Fatal("ignored stalled sender")
	}
	m["bridge_events_published_total"] = 3000
	if !h.evaluate(now.Add(15*time.Second), 200, true, true, m) || h.rate != 3 {
		t.Fatal("did not resume after drain progress")
	}
}

func TestRecoveryPermitsPauseAndDoNotAccumulate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var interval atomic.Int64
	permits := recoveryPermits(ctx, &interval)
	if cap(permits) != 0 {
		t.Fatal("permits can accumulate")
	}
	select {
	case <-permits:
		t.Fatal("permit issued while paused")
	case <-time.After(30 * time.Millisecond):
	}
	interval.Store(int64(10 * time.Millisecond))
	select {
	case <-permits:
	case <-time.After(time.Second):
		t.Fatal("did not resume")
	}
	interval.Store(0)
	select {
	case <-permits:
		t.Fatal("permit issued after pause")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case _, ok := <-permits:
		if ok {
			t.Fatal("issued permit after shutdown")
		}
	case <-time.After(time.Second):
		t.Fatal("pacer leaked")
	}
}

func TestResumeIncludesUnprocessedCandidates(t *testing.T) {
	latest := map[string]string{"online": "recovered_online", "retry": "not_yet_online", "manual": "explicit_offline_skipped"}
	if !resumeAccount(latest, "unprocessed") || !resumeAccount(latest, "retry") || resumeAccount(latest, "online") || resumeAccount(latest, "manual") {
		t.Fatal("incorrect checkpoint resume selection")
	}
}

func TestTwoWayBroadcastPriorityUsesPurposeAndDirection(t *testing.T) {
	for _, tc := range []struct {
		usage, direction string
		want             bool
	}{
		{"broadcast", "two_way", true}, {"active, broadcast", "", true}, {" BROADCAST ", " TWO_WAY ", true},
		{"broadcast", "one_way", false}, {"active", "two_way", false}, {"rental", "two_way", false},
		{"broadcasting", "two_way", false}, {"broadcast", "invalid", false},
	} {
		if got := isTwoWayBroadcast(candidate{Usage: tc.usage, BroadcastDirection: tc.direction}); got != tc.want {
			t.Fatalf("%+v got %t", tc, got)
		}
	}
}

func TestTwoWayBroadcastOutranksPreviouslyOnlineOtherPurposes(t *testing.T) {
	candidates := []candidate{
		{ID: "rental", Usage: "rental"}, {ID: "broadcast-new", Usage: "broadcast", BroadcastDirection: "two_way"},
		{ID: "single", Usage: "broadcast", BroadcastDirection: "one_way"}, {ID: "broadcast-known", Usage: "broadcast"},
		{ID: "active", Usage: "active"},
	}
	orderRecoveryCandidates(candidates, map[string]string{"rental": "recovered_online", "broadcast-known": "recovered_online"})
	want := []string{"broadcast-known", "broadcast-new", "rental", "single", "active"}
	for i, c := range candidates {
		if c.ID != want[i] {
			t.Fatalf("position %d: got %s want %s", i, c.ID, want[i])
		}
	}
}

func healthyMetrics() map[string]float64 {
	return map[string]float64{
		"bridge_nats_enqueue_failures_total": 1335, "bridge_metrics_snapshot_fresh": 1, "bridge_metrics_snapshot_age_seconds": 2,
		"bridge_nats_connected": 1, "bridge_nats_enqueue_healthy": 1, "bridge_nats_outbox_storage_healthy": 1, "bridge_nats_outbox_pending": 8, "bridge_nats_outbox_quarantined": 0, "bridge_active_connections": 2600,
	}
}

func TestGateRequiresQuietPeriodAndNeverBypassesReadiness(t *testing.T) {
	now := time.Now()
	h := &recoveryHealth{}
	m := healthyMetrics()
	if h.evaluate(now, 503, true, false, m) {
		t.Fatal("accepted historical failures without observation")
	}
	if h.evaluate(now.Add(46*time.Second), 503, true, false, m) {
		t.Fatal("bypassed real readiness")
	}
	if !h.evaluate(now.Add(46*time.Second), 200, true, true, m) {
		t.Fatal("did not resume after healthy readiness")
	}
	m["bridge_nats_enqueue_failures_total"]++
	if h.evaluate(now.Add(47*time.Second), 200, true, true, m) {
		t.Fatal("new failure did not pause")
	}
	m["bridge_nats_enqueue_healthy"] = 0
	if h.evaluate(now.Add(95*time.Second), 200, true, true, m) {
		t.Fatal("unhealthy enqueue state accepted")
	}
}

func TestGateRejectsStaleDisconnectedAndBackloggedMetrics(t *testing.T) {
	for key, value := range map[string]float64{"bridge_metrics_snapshot_age_seconds": 31, "bridge_metrics_snapshot_fresh": 0, "bridge_nats_connected": 0, "bridge_nats_enqueue_healthy": 0, "bridge_nats_outbox_storage_healthy": 0, "bridge_nats_outbox_pending": 257, "bridge_nats_outbox_quarantined": 1, "bridge_nats_outbox_oldest_age_seconds": 16} {
		t.Run(key, func(t *testing.T) {
			h := &recoveryHealth{}
			m := healthyMetrics()
			m["bridge_nats_enqueue_failures_total"] = 0
			m[key] = value
			if h.evaluate(time.Now(), 200, true, true, m) {
				t.Fatal("unhealthy metrics passed")
			}
		})
	}
	h := &recoveryHealth{}
	m := healthyMetrics()
	delete(m, "bridge_nats_outbox_storage_healthy")
	if h.evaluate(time.Now(), 200, true, true, m) {
		t.Fatal("missing metric accepted")
	}
}

func TestWaitReadyWaitsInsteadOfDiscardingCandidate(t *testing.T) {
	var ready atomic.Bool
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- waitReady(ctx, &ready, time.Second) }()
	select {
	case <-done:
		t.Fatal("candidate discarded while not ready")
	case <-time.After(20 * time.Millisecond):
	}
	ready.Store(true)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ready.Store(false)
	if err := waitReady(context.Background(), &ready, time.Millisecond); err == nil {
		t.Fatal("outage deadline ignored")
	}
}

func TestResumeOnlyRetriesUnfinishedResults(t *testing.T) {
	for _, s := range []string{"deferred_bridge_unavailable", "status_timeout_deferred", "not_yet_online", "status_timeout_after_connect", "connect_rpc_failed"} {
		if !retryResult(s) {
			t.Fatal(s)
		}
	}
	for _, s := range []string{"already_online", "recovered_online", "login_required", "explicit_offline_skipped", "proxy_blocked", "proxy_auth_failed", ""} {
		if retryResult(s) {
			t.Fatal(s)
		}
	}
}

func TestParseMetricsAcceptsLabels(t *testing.T) {
	m := parseMetrics("# comment\nbridge_active_connections{worker_id=\"api02\"} 2600\nbridge_nats_connected 1\n")
	if m["bridge_active_connections"] != 2600 || m["bridge_nats_connected"] != 1 {
		t.Fatal(m)
	}
}

func TestResumeLatestResultsWin(t *testing.T) {
	a := filepath.Join(t.TempDir(), "first.jsonl")
	b := filepath.Join(t.TempDir(), "second.jsonl")
	if err := os.WriteFile(a, []byte("{\"account_id\":\"1\",\"result\":\"deferred_bridge_unavailable\"}\n{\"account_id\":\"2\",\"result\":\"deferred_bridge_unavailable\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b, []byte("{\"account_id\":\"1\",\"result\":\"recovered_online\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	latest, err := readResults(a + "," + b)
	if err != nil || latest["1"] != "recovered_online" || !retryResult(latest["2"]) {
		t.Fatalf("lost checkpoint: %+v %v", latest, err)
	}
}
