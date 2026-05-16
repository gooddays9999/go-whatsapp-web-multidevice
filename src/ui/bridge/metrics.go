package bridge

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
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
	ready := s.deps.DeviceManager != nil && s.deps.DeviceManager.IsHealthy()
	natsOK := s.publisher == nil || s.publisher.IsConnected()
	status := http.StatusOK
	state := "ready"
	if !ready || !natsOK {
		status = http.StatusServiceUnavailable
		state = "not_ready"
	}
	writeJSON(w, status, fmt.Sprintf(`{"status":%q,"redis":true,"nats":%t,"workers":1}`, state, natsOK))
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
	accounts := s.accountIDs()
	usable := s.usableAccountIDs()
	fmt.Fprintf(w, "# HELP bridge_active_connections Number of active WhatsApp connections\n")
	fmt.Fprintf(w, "# TYPE bridge_active_connections gauge\n")
	fmt.Fprintf(w, "bridge_active_connections{worker_id=%q} %d\n", s.workerID, len(usable))
	fmt.Fprintf(w, "# HELP bridge_active_workers Number of active workers\n")
	fmt.Fprintf(w, "# TYPE bridge_active_workers gauge\nbridge_active_workers 1\n")
	fmt.Fprintf(w, "# HELP bridge_workers_ready Number of ready workers\n")
	fmt.Fprintf(w, "# TYPE bridge_workers_ready gauge\nbridge_workers_ready 1\n")
	fmt.Fprintf(w, "# HELP bridge_worker_account_count Number of accounts per worker\n")
	fmt.Fprintf(w, "# TYPE bridge_worker_account_count gauge\n")
	fmt.Fprintf(w, "bridge_worker_account_count{worker_id=%q} %d\n", s.workerID, len(accounts))
	fmt.Fprintf(w, "# HELP bridge_events_published_total Total number of events published to NATS\n")
	fmt.Fprintf(w, "# TYPE bridge_events_published_total counter\nbridge_events_published_total %d\n", s.publisher.Count())
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
		if cachedLoggedIn(inst.Snapshot().State) {
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
