package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
)

type BridgeEnvironment struct {
	AccountID     string
	TenantID      string
	ProxyType     string
	ProxyHost     string
	ProxyPort     int32
	ProxyUsername string
	ProxyPassword string
	UserAgent     string
	BrowserFamily string
	OSName        string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type EnvironmentStore struct {
	db     *sql.DB
	uaPool *UAPool
	cfg    Config
}

func NewEnvironmentStore(db *sql.DB, uaPool *UAPool, cfg Config) *EnvironmentStore {
	return &EnvironmentStore{db: db, uaPool: uaPool, cfg: cfg}
}

func (s *EnvironmentStore) Init(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS bridge_environments (
			account_id VARCHAR(255) PRIMARY KEY,
			tenant_id VARCHAR(255) DEFAULT '',
			proxy_type VARCHAR(20) DEFAULT '',
			proxy_host VARCHAR(255) DEFAULT '',
			proxy_port INTEGER DEFAULT 0,
			proxy_username VARCHAR(255) DEFAULT '',
			proxy_password VARCHAR(255) DEFAULT '',
			user_agent TEXT DEFAULT '',
			browser_family VARCHAR(50) DEFAULT '',
			os_name VARCHAR(50) DEFAULT '',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	return err
}

func (s *EnvironmentStore) Get(ctx context.Context, accountID string) (*BridgeEnvironment, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT account_id, tenant_id, proxy_type, proxy_host, proxy_port, proxy_username, proxy_password,
		       user_agent, browser_family, os_name, created_at, updated_at
		FROM bridge_environments
		WHERE account_id = ?
		LIMIT 1
	`, accountID)
	var env BridgeEnvironment
	err := row.Scan(
		&env.AccountID,
		&env.TenantID,
		&env.ProxyType,
		&env.ProxyHost,
		&env.ProxyPort,
		&env.ProxyUsername,
		&env.ProxyPassword,
		&env.UserAgent,
		&env.BrowserFamily,
		&env.OSName,
		&env.CreatedAt,
		&env.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &env, nil
}

func (s *EnvironmentStore) GetOrCreate(ctx context.Context, accountID, tenantID string, requestProxy *bridgepb.ProxyConfig, allowRequestProxy bool) (*BridgeEnvironment, bool, error) {
	if accountID == "" {
		return nil, false, fmt.Errorf("account_id is required")
	}
	existing, err := s.Get(ctx, accountID)
	if err != nil || existing != nil {
		return existing, false, err
	}

	proxy := s.cfg.DefaultProxy
	if allowRequestProxy && requestProxy != nil && (requestProxy.GetType() != "" || requestProxy.GetHost() != "") {
		proxy = ProxySpec{
			Type:     requestProxy.GetType(),
			Host:     requestProxy.GetHost(),
			Port:     requestProxy.GetPort(),
			Username: requestProxy.GetUsername(),
			Password: requestProxy.GetPassword(),
		}
	}
	if _, err := proxy.URL(); err != nil {
		return nil, false, err
	}

	ua := s.uaPool.Select(accountID)
	now := time.Now()
	env := &BridgeEnvironment{
		AccountID:     accountID,
		TenantID:      tenantID,
		ProxyType:     strings.ToLower(strings.TrimSpace(proxy.Type)),
		ProxyHost:     strings.TrimSpace(proxy.Host),
		ProxyPort:     proxy.Port,
		ProxyUsername: proxy.Username,
		ProxyPassword: proxy.Password,
		UserAgent:     ua.UserAgent,
		BrowserFamily: ua.BrowserFamily,
		OSName:        ua.OSName,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO bridge_environments (
			account_id, tenant_id, proxy_type, proxy_host, proxy_port, proxy_username, proxy_password,
			user_agent, browser_family, os_name, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, env.AccountID, env.TenantID, env.ProxyType, env.ProxyHost, env.ProxyPort, env.ProxyUsername, env.ProxyPassword,
		env.UserAgent, env.BrowserFamily, env.OSName, env.CreatedAt, env.UpdatedAt)
	if err != nil {
		return nil, false, err
	}
	return env, true, nil
}

func (s *EnvironmentStore) Delete(ctx context.Context, accountID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM bridge_environments WHERE account_id = ?`, accountID)
	return err
}

func (e BridgeEnvironment) ProxyURL() (string, error) {
	return ProxySpec{
		Type:     e.ProxyType,
		Host:     e.ProxyHost,
		Port:     e.ProxyPort,
		Username: e.ProxyUsername,
		Password: e.ProxyPassword,
	}.URL()
}

func (p ProxySpec) URL() (string, error) {
	proxyType := strings.ToLower(strings.TrimSpace(p.Type))
	host := strings.TrimSpace(p.Host)
	if proxyType == "" && host == "" {
		return "", nil
	}
	if proxyType != "socks5" && proxyType != "http" && proxyType != "https" {
		return "", fmt.Errorf("unsupported proxy type %q", p.Type)
	}
	if host == "" {
		return "", fmt.Errorf("proxy host is required")
	}
	if p.Port <= 0 || p.Port > 65535 {
		return "", fmt.Errorf("valid proxy port is required")
	}
	u := &url.URL{
		Scheme: proxyType,
		Host:   net.JoinHostPort(host, fmt.Sprintf("%d", p.Port)),
	}
	if p.Username != "" {
		u.User = url.UserPassword(p.Username, p.Password)
	}
	return u.String(), nil
}
