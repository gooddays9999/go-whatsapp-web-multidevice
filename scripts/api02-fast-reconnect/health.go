package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type recoveryHealth struct {
	observed      bool
	failures      float64
	changedAt     time.Time
	adaptive      bool
	maxRate       float64
	rate          float64
	lastAt        time.Time
	lastPending   float64
	lastPublished float64
}

func parseMetrics(body string) map[string]float64 {
	values := map[string]float64{}
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) != 2 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		v, e := strconv.ParseFloat(fields[1], 64)
		if e == nil {
			values[strings.SplitN(fields[0], "{", 2)[0]] = v
		}
	}
	return values
}

func (h *recoveryHealth) evaluate(now time.Time, code int, natsOK, outboxOK bool, m map[string]float64) bool {
	h.rate = 0
	failures, present := m["bridge_nats_enqueue_failures_total"]
	if !present || math.IsNaN(failures) || math.IsInf(failures, 0) || failures < 0 {
		return false
	}
	if !h.observed || h.failures != failures {
		h.observed = true
		h.failures = failures
		h.changedAt = now
	}
	for _, key := range []string{"bridge_metrics_snapshot_fresh", "bridge_metrics_snapshot_age_seconds", "bridge_nats_connected", "bridge_nats_enqueue_healthy", "bridge_nats_outbox_storage_healthy", "bridge_nats_outbox_pending", "bridge_nats_outbox_quarantined"} {
		if value, ok := m[key]; !ok || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return false
		}
	}
	if m["bridge_metrics_snapshot_fresh"] != 1 || m["bridge_metrics_snapshot_age_seconds"] > 30 || m["bridge_nats_connected"] != 1 || m["bridge_nats_enqueue_healthy"] != 1 || m["bridge_nats_outbox_storage_healthy"] != 1 || m["bridge_nats_outbox_quarantined"] > 0 {
		return false
	}
	if failures > 0 && now.Sub(h.changedAt) < 45*time.Second {
		return false
	}
	if code != http.StatusOK || !natsOK || !outboxOK {
		return false
	}
	if !h.adaptive {
		return m["bridge_nats_outbox_pending"] <= 256 && m["bridge_nats_outbox_oldest_age_seconds"] <= 15
	}
	return h.adaptiveRate(now, m)
}

func (h *recoveryHealth) adaptiveRate(now time.Time, m map[string]float64) bool {
	for _, key := range []string{"bridge_nats_outbox_used_bytes", "bridge_nats_outbox_max_bytes", "bridge_events_published_total"} {
		v, ok := m[key]
		if !ok || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			return false
		}
	}
	pending := m["bridge_nats_outbox_pending"]
	age, present := m["bridge_nats_outbox_oldest_age_seconds"]
	if pending > 0 && (!present || math.IsNaN(age) || math.IsInf(age, 0) || age < 0) {
		return false
	}
	capacity := m["bridge_nats_outbox_max_bytes"]
	if capacity <= 0 {
		return false
	}
	utilization := m["bridge_nats_outbox_used_bytes"] / capacity
	published := m["bridge_events_published_total"]
	elapsed := now.Sub(h.lastAt).Seconds()
	growing := !h.lastAt.IsZero() && elapsed > 0 && pending-h.lastPending > 500*elapsed
	stalled := !h.lastAt.IsZero() && pending > 2000 && published <= h.lastPublished && elapsed >= 4
	h.lastAt, h.lastPending, h.lastPublished = now, pending, published
	if pending > 100000 || age > 60 || utilization >= 0.9 || stalled {
		return false
	}
	h.rate = 1
	if pending <= 20000 && age <= 30 && utilization < 0.7 && !growing {
		h.rate = 3
	}
	if pending <= 2000 && age <= 10 && utilization < 0.5 && !growing {
		h.rate = 6
	}
	if h.maxRate > 0 {
		h.rate = min(h.rate, h.maxRate)
	}
	return true
}

// Unbuffered permits prevent a burst of saved tokens after a health pause.
func recoveryPermits(ctx context.Context, interval *atomic.Int64) <-chan struct{} {
	permits := make(chan struct{})
	go func() {
		defer close(permits)
		for ctx.Err() == nil {
			delay := time.Duration(interval.Load())
			if delay <= 0 {
				delay = 100 * time.Millisecond
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if interval.Load() > 0 {
				select {
				case permits <- struct{}{}:
				default:
				}
			}
		}
	}()
	return permits
}

func (h *recoveryHealth) check(client *http.Client) bool {
	h.rate = 0
	r, e := client.Get("http://127.0.0.1:29192/ready")
	if e != nil {
		return false
	}
	code := r.StatusCode
	var readiness struct {
		NATS   bool `json:"nats"`
		Outbox bool `json:"outbox"`
	}
	e = json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&readiness)
	r.Body.Close()
	if e != nil {
		return false
	}
	r, e = client.Get("http://127.0.0.1:29192/metrics")
	if e != nil {
		return false
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return false
	}
	b, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if e != nil {
		return false
	}
	return h.evaluate(time.Now(), code, readiness.NATS, readiness.Outbox, parseMetrics(string(b)))
}

func resumeAccount(latest map[string]string, id string) bool {
	result, seen := latest[id]
	return !seen || retryResult(result)
}

func waitReady(ctx context.Context, ready *atomic.Bool, maxWait time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for !ready.Load() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return context.DeadlineExceeded
		case <-tick.C:
		}
	}
	return nil
}

func retryResult(result string) bool {
	switch result {
	case "deferred_bridge_unavailable", "status_timeout_deferred", "not_yet_online", "status_timeout_after_connect", "connect_rpc_failed":
		return true
	default:
		return false
	}
}

func readResults(paths string) (map[string]string, error) {
	latest := map[string]string{}
	for _, path := range strings.Split(paths, ",") {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(f)
		for {
			var r struct {
				ID     string `json:"account_id"`
				Result string `json:"result"`
			}
			err = decoder.Decode(&r)
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return nil, err
			}
			latest[r.ID] = r.Result
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	return latest, nil
}
