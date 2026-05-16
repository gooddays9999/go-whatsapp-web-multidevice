package bridge

import (
	"context"
	"sync"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/sirupsen/logrus"
)

const (
	startupRestoreDelay       = 5 * time.Second
	startupRestoreConcurrency = 3
)

func (s *Service) restorePersistedAccounts(ctx context.Context) {
	if s == nil || s.envStore == nil || s.deps.DeviceManager == nil {
		return
	}
	timer := time.NewTimer(startupRestoreDelay)
	select {
	case <-timer.C:
	case <-ctx.Done():
		timer.Stop()
		return
	}

	envs, err := s.envStore.List(ctx)
	if err != nil {
		logrus.WithError(err).Warn("failed to list bridge environments for startup restore")
		return
	}
	if len(envs) == 0 {
		return
	}
	logrus.WithField("accounts", len(envs)).Info("starting bridge environment restore")

	sem := make(chan struct{}, startupRestoreConcurrency)
	var wg sync.WaitGroup
	for _, env := range envs {
		if env == nil || env.AccountID == "" {
			continue
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		wg.Add(1)
		go func(env *BridgeEnvironment) {
			defer wg.Done()
			defer func() { <-sem }()
			s.restorePersistedAccount(ctx, env)
		}(env)
	}
	wg.Wait()
	logrus.Info("bridge environment restore finished")
}

func (s *Service) restorePersistedAccount(parent context.Context, env *BridgeEnvironment) {
	proxyURL, err := env.ProxyURL()
	if err != nil {
		logrus.WithError(err).WithField("account_id", env.AccountID).Warn("skipping bridge environment restore with invalid proxy")
		return
	}
	inst, err := s.deps.DeviceManager.EnsureClientWithEnvironment(parent, env.AccountID, whatsapp.ClientEnvironment{
		ProxyAddress:    proxyURL,
		ProxyConfigured: true,
		UserAgent:       env.UserAgent,
		BrowserFamily:   env.BrowserFamily,
		OSName:          env.OSName,
	})
	if err != nil {
		logrus.WithError(err).WithField("account_id", env.AccountID).Warn("failed to create client for bridge environment restore")
		return
	}
	client := inst.GetClient()
	if client == nil || client.Store == nil || client.Store.ID == nil {
		return
	}
	if cachedLoggedIn(inst.UpdateStateFromClient()) {
		s.markConnected(env.AccountID)
		return
	}

	timeout := s.connectTimeout()
	ctx, cancel := context.WithTimeout(parent, timeout+5*time.Second)
	defer cancel()
	if err := inst.ConnectWithTimeout(ctx, timeout, "bridge startup restore"); err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"account_id": env.AccountID,
			"proxy":      env.ProxySummary(),
		}).Warn("bridge environment restore failed")
		return
	}
	if cachedLoggedIn(inst.UpdateStateFromClient()) {
		s.markConnected(env.AccountID)
		s.publish("account.connected", env.AccountID, map[string]any{
			"phoneNumber": inst.PhoneNumber(),
			"workerId":    s.workerID,
			"connectedAt": time.Now().UnixMilli(),
			"verified":    true,
		})
	}
}
