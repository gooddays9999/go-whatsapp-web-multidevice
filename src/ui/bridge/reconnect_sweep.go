package bridge

import (
	"context"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/sirupsen/logrus"
)

// reconnectSweepInterval is how often the bridge scans its held device instances for accounts
// that dropped their socket without emitting an events.Disconnected the bridge could hook
// (silent drops: proxy blips, device-level batch drops, zombie sockets). It is a package var
// so tests can shorten it. Set to 0 to disable the sweep.
var reconnectSweepInterval = 60 * time.Second

// reconnectSweepLoop periodically reconnects accounts that are held by the bridge but are no
// longer connected. Unlike the events.Disconnected fallback (which only fires when whatsmeow
// reports a disconnect), the sweep catches silent drops that never reach the bridge as an
// event — the case where an account stays offline until a manual re-login.
func (s *Service) reconnectSweepLoop(ctx context.Context) {
	if s == nil {
		return
	}
	interval := reconnectSweepInterval
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweepReconnect(ctx)
		}
	}
}

// sweepReconnect scans every held device instance once and schedules a reconnect for any that
// has a persisted session but is not currently connected, unless it was explicitly taken
// offline or is already being reconnected. The actual reconnect reuses scheduleReconnect's
// connect-slot limiter, so a large batch of stuck accounts drains at a bounded rate.
func (s *Service) sweepReconnect(ctx context.Context) {
	if s == nil || s.deps.DeviceManager == nil {
		return
	}
	// Skip while the startup restore is still connecting accounts: the restore owns that wave,
	// and sweeping mid-restore would duplicate its reconnects.
	if s.isRestoring() {
		return
	}
	for _, inst := range s.deps.DeviceManager.ListDevices() {
		if inst == nil || inst.IsConnected() {
			continue
		}
		accountID := s.eventAccountID(inst)
		if accountID == "" {
			continue
		}
		if !shouldSweepReconnect(instanceHasSession(inst), false, s.isExplicitOffline(accountID), s.isReconnecting(accountID)) {
			continue
		}
		if !s.canScheduleReconnect(ctx, accountID, inst) {
			continue
		}
		logrus.WithField("account_id", accountID).Warn("reconnect sweep: account disconnected without event, scheduling reconnect")
		s.scheduleReconnect(accountID, "reconnect sweep")
	}
}

// shouldSweepReconnect reports whether the periodic sweep should reconnect an account. It
// reconnects only accounts that still hold a WhatsApp session (reconnect can succeed), whose
// socket is down, that were not explicitly taken offline, and that are not already queued for
// reconnect (dedup with the event-driven paths).
func shouldSweepReconnect(hasSession, connected, explicitOffline, reconnecting bool) bool {
	return hasSession && !connected && !explicitOffline && !reconnecting
}

// isReconnecting reports whether a reconnect is already in flight or armed for the account,
// covering both the scheduleReconnect path and the events.Disconnected fallback.
func (s *Service) isReconnecting(accountID string) bool {
	if s == nil || accountID == "" {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.reconnecting[accountID]; ok {
		return true
	}
	_, ok := s.reconnectFallback[accountID]
	return ok
}

// instanceHasSession reports whether the instance's client still holds a persisted WhatsApp
// session (device store ID). It is a cheap, socket-independent check so logged-out accounts
// are skipped by the sweep.
func instanceHasSession(inst *whatsapp.DeviceInstance) bool {
	if inst == nil {
		return false
	}
	client := inst.GetClient()
	return client != nil && client.Store != nil && client.Store.ID != nil
}
