package bridge

import (
	"context"
	"sync"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/sirupsen/logrus"
)

const (
	startupRestoreDelay              = 5 * time.Second
	defaultStartupRestoreConcurrency = 20
	accountWebOnlineOnline           = 1
)

func (s *Service) restorePersistedAccounts(ctx context.Context) {
	if s == nil || s.envStore == nil || s.deps.DeviceManager == nil {
		return
	}
	s.setRestoring(true)
	defer s.setRestoring(false)

	timer := time.NewTimer(startupRestoreDelay)
	select {
	case <-timer.C:
	case <-ctx.Done():
		timer.Stop()
		return
	}

	envs, restorableAccounts, err := s.restoreCandidateEnvironments(ctx)
	if err != nil {
		logrus.WithError(err).Warn("failed to list bridge environments for startup restore")
		return
	}
	if len(envs) == 0 {
		if restorableAccounts > 0 {
			logrus.WithField("restorable_accounts", restorableAccounts).Info("no persisted bridge environments found for startup restore")
		}
		return
	}
	concurrency := s.cfg.StartupRestoreConcurrency
	if concurrency <= 0 {
		concurrency = defaultStartupRestoreConcurrency
	}
	if concurrency > len(envs) {
		concurrency = len(envs)
	}
	fields := logrus.Fields{
		"accounts":    len(envs),
		"concurrency": concurrency,
	}
	if restorableAccounts > 0 {
		fields["restorable_accounts"] = restorableAccounts
		fields["missing_environments"] = restorableAccounts - len(envs)
		fields["source"] = "account_database"
	}
	logrus.WithFields(fields).Info("starting bridge environment restore")

	sem := make(chan struct{}, concurrency)
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

func (s *Service) restoreCandidateEnvironments(ctx context.Context) ([]*BridgeEnvironment, int, error) {
	if s.accountProxyStore == nil {
		envs, err := s.envStore.List(ctx)
		return envs, 0, err
	}

	accountIDs, err := s.accountProxyStore.RestorableAccountIDs(ctx)
	if err != nil {
		return nil, 0, err
	}
	if len(accountIDs) == 0 {
		logrus.Info("no account database records are eligible for bridge startup restore")
		return nil, 0, nil
	}

	envs, err := s.envStore.ListByAccountIDs(ctx, accountIDs)
	if err != nil {
		return nil, len(accountIDs), err
	}
	return envs, len(accountIDs), nil
}

func (s *Service) setRestoring(restoring bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.restoring = restoring
	s.mu.Unlock()
}

func (s *Service) isRestoring() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.restoring
}

func (s *Service) restorePersistedAccount(parent context.Context, env *BridgeEnvironment) {
	if s.accountProxyStore != nil {
		webOnline, found, err := s.accountProxyStore.WebOnlineForAccount(parent, env.AccountID)
		if err != nil {
			logrus.WithError(err).WithField("account_id", env.AccountID).Warn("failed to read account web_online for bridge environment restore")
			return
		}
		if !found {
			logrus.WithField("account_id", env.AccountID).Info("skipping bridge environment restore for missing or deleted account")
			return
		}
		if webOnline != accountWebOnlineOnline {
			logrus.WithFields(logrus.Fields{
				"account_id": env.AccountID,
				"web_online": webOnline,
			}).Info("skipping bridge environment restore for offline account")
			return
		}
	}
	resolvedEnv, _, err := s.environmentForAccount(parent, env.AccountID, env.TenantID, nil, false)
	if err != nil {
		logrus.WithError(err).WithField("account_id", env.AccountID).Warn("skipping bridge environment restore without current account proxy")
		return
	}
	env = resolvedEnv
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
