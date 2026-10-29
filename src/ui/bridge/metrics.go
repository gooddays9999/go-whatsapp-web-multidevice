package bridge

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

func (s *Service) startHTTP(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, `{"status":"ok"}`)
	})
	mux.HandleFunc("/health/ready", s.handleReady)
	mux.HandleFunc("/ready", s.handleReady)
	mux.HandleFunc("/health/detail", s.handleHealthDetail)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/internal/history-sync", s.handleHistoryRecovery)
	if s.cfg.EnablePprof {
		// Opt-in live profiling: capture a goroutine/CPU/block profile during a
		// send-latency incident to locate serialization, e.g.
		//   curl :<metrics>/debug/pprof/goroutine?debug=2
		//   curl -o cpu.prof :<metrics>/debug/pprof/profile?seconds=30
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		logrus.Warn("bridge pprof debug endpoints enabled on metrics port (/debug/pprof/) — disable when done")
	}
	addr := fmt.Sprintf("0.0.0.0:%d", s.cfg.MetricsPort)
	s.httpServer = &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(shutdownCtx)
	}()
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// gRPC server startup will still surface fatal errors; metrics failure is logged only.
			fmt.Printf("bridge metrics server failed: %v\n", err)
		}
	}()
	return nil
}

func (s *Service) handleReady(w http.ResponseWriter, _ *http.Request) {
	snapshot := s.metricsSnapshot.Load()
	ready := snapshot != nil && time.Since(snapshot.sampledAt) <= 90*time.Second && snapshot.managerReady
	natsOK := s.publisher == nil || s.publisher.IsConnected()
	outboxOK := snapshot != nil && snapshot.outboxHealthy && (s.publisher == nil || s.publisher.enqueueFailures.Load() == 0)
	status := http.StatusOK
	state := "ready"
	if !ready || !natsOK || !outboxOK {
		status = http.StatusServiceUnavailable
		state = "not_ready"
	}
	writeJSON(w, status, fmt.Sprintf(`{"status":%q,"redis":true,"nats":%t,"outbox":%t,"workers":1}`, state, natsOK, outboxOK))
}

func (s *Service) handleHealthDetail(w http.ResponseWriter, _ *http.Request) {
	accounts := s.accountIDs()
	body := fmt.Sprintf(
		`{"status":"ok","workers":[{"id":%q,"pid":%d,"status":"ready","accounts":%s,"accountCount":%d,"startedAt":%d,"lastHeartbeat":%d,"uptimeMs":%d}],"totalWorkers":1,"totalAccounts":%d}`,
		s.workerID,
		os.Getpid(),
		jsonStringArray(accounts),
		len(accounts),
		s.startedAt.UnixMilli(),
		time.Now().UnixMilli(),
		time.Since(s.startedAt).Milliseconds(),
		len(accounts),
	)
	writeJSON(w, http.StatusOK, body)
}

func (s *Service) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	snapshot := s.metricsSnapshot.Load()
	// Never walk device locks or query SQL in the scrape path. An incomplete or
	// stale sample must not masquerade as a current, healthy online count.
	if snapshot == nil || time.Since(snapshot.sampledAt) > 90*time.Second {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, "bridge_metrics_snapshot_fresh 0")
		return
	}
	fmt.Fprintln(w, "bridge_metrics_snapshot_fresh 1")
	fmt.Fprintf(w, "bridge_metrics_snapshot_age_seconds %g\n", time.Since(snapshot.sampledAt).Seconds())
	fmt.Fprintf(w, "# HELP bridge_active_connections Number of active WhatsApp connections\n")
	fmt.Fprintf(w, "# TYPE bridge_active_connections gauge\n")
	fmt.Fprintf(w, "bridge_active_connections{worker_id=%q} %d\n", s.workerID, snapshot.online)
	fmt.Fprintf(w, "# HELP bridge_active_workers Number of active workers\n")
	fmt.Fprintf(w, "# TYPE bridge_active_workers gauge\nbridge_active_workers 1\n")
	fmt.Fprintf(w, "# HELP bridge_workers_ready Number of ready workers\n")
	natsReady := s.publisher == nil || (s.publisher.IsConnected() && s.publisher.enqueueFailures.Load() == 0)
	ready := 0
	if natsReady && snapshot.managerReady && snapshot.outboxHealthy {
		ready = 1
	}
	fmt.Fprintf(w, "# TYPE bridge_workers_ready gauge\nbridge_workers_ready %d\n", ready)
	fmt.Fprintf(w, "# HELP bridge_worker_account_count Number of accounts per worker\n")
	fmt.Fprintf(w, "# TYPE bridge_worker_account_count gauge\n")
	fmt.Fprintf(w, "bridge_worker_account_count{worker_id=%q} %d\n", s.workerID, snapshot.accounts)
	fmt.Fprintf(w, "# HELP bridge_events_published_total Total number of events published to NATS\n")
	fmt.Fprintf(w, "# TYPE bridge_events_published_total counter\nbridge_events_published_total %d\n", s.publisher.Count())
	if p := s.publisher; p != nil {
		connected := 0
		if p.IsConnected() {
			connected = 1
		}
		fmt.Fprintf(w, "bridge_nats_connected %d\nbridge_nats_connections_created_total %d\nbridge_nats_failures_total %d\n", connected, p.rebuilds.Load(), p.failures.Load())
		fmt.Fprintf(w, "bridge_nats_enqueue_failures_total %d\n", p.enqueueFailures.Load())
	}
	fmt.Fprintf(w, "bridge_nats_outbox_pending %d\nbridge_nats_outbox_quarantined %d\n", snapshot.outbox.Pending, snapshot.outbox.Quarantined)
	outboxHealthy := 0
	if snapshot.outboxHealthy {
		outboxHealthy = 1
	}
	fmt.Fprintf(w, "bridge_nats_outbox_storage_healthy %d\n", outboxHealthy)
	if snapshot.outbox.Oldest != 0 {
		fmt.Fprintf(w, "bridge_nats_outbox_oldest_age_seconds %g\n", time.Since(time.UnixMilli(snapshot.outbox.Oldest)).Seconds())
	}
}

type bridgeMetricsSnapshot struct {
	sampledAt     time.Time
	managerReady  bool
	outboxHealthy bool
	accounts      int
	online        int
	outbox        outboxStats
}

func (s *Service) metricsSnapshotLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		snapshot := &bridgeMetricsSnapshot{sampledAt: time.Now(), outboxHealthy: true}
		if s.publisher != nil && s.publisher.outbox != nil {
			statsCtx, cancel := context.WithTimeout(ctx, time.Second)
			stats, err := s.publisher.outbox.stats(statsCtx)
			cancel()
			if err != nil {
				snapshot.outboxHealthy = false
				logrus.WithError(err).Warn("bridge outbox metrics unavailable")
			} else {
				snapshot.outbox = stats
			}
		}
		snapshot.managerReady = s.deps.DeviceManager != nil && s.deps.DeviceManager.IsHealthy()
		snapshot.accounts = len(s.accountIDs())
		snapshot.online = len(s.usableAccountIDs())
		s.metricsSnapshot.Store(snapshot)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) accountIDs() []string {
	devices := s.deps.DeviceManager.ListDevices()
	ids := make([]string, 0, len(devices))
	for _, inst := range devices {
		ids = append(ids, inst.ID())
	}
	return ids
}

func (s *Service) usableAccountIDs() []string {
	devices := s.deps.DeviceManager.ListDevices()
	ids := make([]string, 0, len(devices))
	for _, inst := range devices {
		if s.isExplicitOffline(inst.ID()) {
			continue
		}
		if cachedLoggedIn(inst.UpdateStateFromClient()) {
			ids = append(ids, inst.ID())
		}
	}
	return ids
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}

func jsonStringArray(values []string) string {
	if len(values) == 0 {
		return "[]"
	}
	var b strings.Builder
	b.WriteString("[")
	for i, value := range values {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf("%q", value))
	}
	b.WriteString("]")
	return b.String()
}
