package bridge

import (
	"context"
	"fmt"

	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
)

func (s *Service) environmentForAccount(ctx context.Context, accountID, tenantID string, requestProxy *bridgepb.ProxyConfig, allowRequestProxy bool) (*BridgeEnvironment, bool, error) {
	if s == nil || s.envStore == nil {
		return nil, false, fmt.Errorf("bridge environment store is not ready")
	}

	effectiveProxy := requestProxy
	effectiveAllowRequestProxy := allowRequestProxy
	if s.accountProxyStore != nil {
		accountProxy, found, err := s.accountProxyStore.ProxyForAccount(ctx, accountID)
		if err != nil {
			return nil, false, fmt.Errorf("resolve account database proxy: %w", err)
		}
		if !found || accountProxy.IsEmpty() {
			return nil, false, fmt.Errorf("account %s proxy is required in account database", accountID)
		}
		effectiveProxy = proxyConfigFromSpec(accountProxy)
		effectiveAllowRequestProxy = true
	}

	return s.envStore.GetOrCreate(ctx, accountID, tenantID, effectiveProxy, effectiveAllowRequestProxy)
}

func proxyConfigFromSpec(proxy ProxySpec) *bridgepb.ProxyConfig {
	return &bridgepb.ProxyConfig{
		Type:     proxy.Type,
		Host:     proxy.Host,
		Port:     proxy.Port,
		Username: proxy.Username,
		Password: proxy.Password,
	}
}
