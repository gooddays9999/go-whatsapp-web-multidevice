package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"hash/fnv"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
)

// environmentCacheTTL bounds how long a cached bridge_environments row (used by
// the hot per-event tenant lookup) may be stale after an out-of-band change.
// Writes that go through this store (GetOrCreate/Delete) invalidate the affected
// account_id immediately, so this TTL only guards against edits made outside the
// store (e.g. an admin restoring the registry). It is long because the data is
// effectively static config: a short TTL made sparsely-active accounts miss and
// re-query the SQLite pool on nearly every event. Set to 0 to disable caching.
var environmentCacheTTL = 1 * time.Hour

// environmentCacheEntry caches a resolved bridge environment. A nil env is cached
// too, so negative lookups (account not yet provisioned) are also served from
// memory instead of re-querying the SQLite pool on every inbound event.
type environmentCacheEntry struct {
	env       *BridgeEnvironment
	expiresAt time.Time
}

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
	// cache maps account_id -> environmentCacheEntry for the hot per-event
	// tenant lookup (see GetCached). Invalidated per account_id on write.
	cache sync.Map
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

// GetCached is a read-only, memory-cached variant of Get for the hot per-event
// path (tenant resolution on every published event). The returned pointer must
// be treated as read-only and never mutated, since it is shared across callers.
// Writes through this store invalidate the cached account_id, and environmentCacheTTL
// bounds staleness from any out-of-band edits.
func (s *EnvironmentStore) GetCached(ctx context.Context, accountID string) (*BridgeEnvironment, error) {
	if accountID == "" {
		return nil, nil
	}
	if environmentCacheTTL > 0 {
		if v, ok := s.cache.Load(accountID); ok {
			if entry := v.(environmentCacheEntry); time.Now().Before(entry.expiresAt) {
				return entry.env, nil
			}
		}
	}

	env, err := s.Get(ctx, accountID)
	if err != nil {
		// Do not cache failures; the next event retries.
		return nil, err
	}

	if environmentCacheTTL > 0 {
		s.cache.Store(accountID, environmentCacheEntry{
			env:       env,
			expiresAt: s.cacheExpiry(accountID),
		})
	}
	return env, nil
}

// cacheExpiry returns the expiry for an account's cache entry, jittered by a
// deterministic per-account offset (up to TTL/4). This spreads out expirations
// so entries warmed together (e.g. by WarmCache at startup) do not all lapse at
// the same instant and stampede the SQLite pool with simultaneous reloads.
func (s *EnvironmentStore) cacheExpiry(accountID string) time.Time {
	now := time.Now()
	if environmentCacheTTL <= 0 {
		return now
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(accountID))
	jitter := time.Duration(h.Sum32()) % (environmentCacheTTL/4 + 1)
	return now.Add(environmentCacheTTL + jitter)
}

// WarmCache preloads every stored environment into the in-memory cache so the
// hot per-event tenant lookup (GetCached) starts fully warm and does not fault
// through to the SQLite pool on the first event of each account. Intended to be
// called once at startup after Init. A failure is non-fatal: lookups just fall
// back to on-demand caching.
func (s *EnvironmentStore) WarmCache(ctx context.Context) error {
	if environmentCacheTTL <= 0 {
		return nil
	}
	envs, err := s.List(ctx)
	if err != nil {
		return err
	}
	for _, env := range envs {
		if env == nil || env.AccountID == "" {
			continue
		}
		s.cache.Store(env.AccountID, environmentCacheEntry{
			env:       env,
			expiresAt: s.cacheExpiry(env.AccountID),
		})
	}
	return nil
}

// invalidateCache drops the cached environment for a single account_id. Called
// after any write (GetOrCreate/Delete) so the next GetCached reloads from SQLite.
func (s *EnvironmentStore) invalidateCache(accountID string) {
	if accountID != "" {
		s.cache.Delete(accountID)
	}
}

func (s *EnvironmentStore) List(ctx context.Context) ([]*BridgeEnvironment, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT account_id, tenant_id, proxy_type, proxy_host, proxy_port, proxy_username, proxy_password,
		       user_agent, browser_family, os_name, created_at, updated_at
		FROM bridge_environments
		ORDER BY account_id
	`)
	if err != nil {
		return nil, err
	}
	return scanBridgeEnvironments(rows)
}

func (s *EnvironmentStore) ListByAccountIDs(ctx context.Context, accountIDs []string) ([]*BridgeEnvironment, error) {
	if len(accountIDs) == 0 {
		return nil, nil
	}

	const batchSize = 500
	envs := make([]*BridgeEnvironment, 0, len(accountIDs))
	for start := 0; start < len(accountIDs); start += batchSize {
		end := start + batchSize
		if end > len(accountIDs) {
			end = len(accountIDs)
		}
		batch := accountIDs[start:end]
		placeholders := strings.TrimRight(strings.Repeat("?,", len(batch)), ",")
		args := make([]any, 0, len(batch))
		for _, accountID := range batch {
			args = append(args, accountID)
		}
		rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`
			SELECT account_id, tenant_id, proxy_type, proxy_host, proxy_port, proxy_username, proxy_password,
			       user_agent, browser_family, os_name, created_at, updated_at
			FROM bridge_environments
			WHERE account_id IN (%s)
			ORDER BY account_id
		`, placeholders), args...)
		if err != nil {
			return nil, err
		}
		batchEnvs, err := scanBridgeEnvironments(rows)
		if err != nil {
			return nil, err
		}
		envs = append(envs, batchEnvs...)
	}
	return envs, nil
}

func scanBridgeEnvironments(rows *sql.Rows) ([]*BridgeEnvironment, error) {
	defer rows.Close()

	envs := make([]*BridgeEnvironment, 0)
	for rows.Next() {
		var env BridgeEnvironment
		if err := rows.Scan(
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
		); err != nil {
			return nil, err
		}
		envs = append(envs, &env)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return envs, nil
}

func (s *EnvironmentStore) GetOrCreate(ctx context.Context, accountID, tenantID string, requestProxy *bridgepb.ProxyConfig, allowRequestProxy bool) (*BridgeEnvironment, bool, error) {
	if accountID == "" {
		return nil, false, fmt.Errorf("account_id is required")
	}
	existing, err := s.Get(ctx, accountID)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		updated := false
		if tenantID != "" && existing.TenantID != tenantID {
			existing.TenantID = tenantID
			updated = true
		}
		if allowRequestProxy {
			proxy := normalizeProxySpec(proxySpecFromRequest(requestProxy))
			if proxy.IsEmpty() {
				if !existing.proxyConfigured() {
					return nil, false, fmt.Errorf("proxy is required for account environment")
				}
			} else {
				if _, err := proxy.URL(); err != nil {
					return nil, false, err
				}
				if !existing.hasProxy(proxy) {
					existing.applyProxy(proxy)
					updated = true
				}
			}
		}
		if !existing.proxyConfigured() {
			return nil, false, fmt.Errorf("proxy is required for account environment")
		}
		if updated {
			now := time.Now()
			if _, err := s.db.ExecContext(ctx, `
				UPDATE bridge_environments
				SET tenant_id = ?, proxy_type = ?, proxy_host = ?, proxy_port = ?,
				    proxy_username = ?, proxy_password = ?, updated_at = ?
				WHERE account_id = ?
			`, existing.TenantID, existing.ProxyType, existing.ProxyHost, existing.ProxyPort,
				existing.ProxyUsername, existing.ProxyPassword, now, accountID); err != nil {
				return nil, false, err
			}
			existing.UpdatedAt = now
			s.invalidateCache(accountID)
		}
		return existing, false, err
	}

	proxy := s.cfg.DefaultProxy
	if allowRequestProxy {
		proxy = proxySpecFromRequest(requestProxy)
	}
	proxy = normalizeProxySpec(proxy)
	if proxy.IsEmpty() {
		return nil, false, fmt.Errorf("proxy is required for account environment")
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
	s.invalidateCache(accountID)
	return env, true, nil
}

func (s *EnvironmentStore) Delete(ctx context.Context, accountID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM bridge_environments WHERE account_id = ?`, accountID)
	if err == nil {
		s.invalidateCache(accountID)
	}
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

func (e BridgeEnvironment) ProxySummary() string {
	if strings.TrimSpace(e.ProxyType) == "" && strings.TrimSpace(e.ProxyHost) == "" {
		return ""
	}
	auth := ""
	if e.ProxyUsername != "" {
		auth = e.ProxyUsername + "@"
	}
	return fmt.Sprintf("%s://%s%s:%d", e.ProxyType, auth, e.ProxyHost, e.ProxyPort)
}

func (e *BridgeEnvironment) hasProxy(proxy ProxySpec) bool {
	return e.ProxyType == proxy.Type &&
		e.ProxyHost == proxy.Host &&
		e.ProxyPort == proxy.Port &&
		e.ProxyUsername == proxy.Username &&
		e.ProxyPassword == proxy.Password
}

func (e *BridgeEnvironment) proxyConfigured() bool {
	return strings.TrimSpace(e.ProxyType) != "" &&
		strings.TrimSpace(e.ProxyHost) != "" &&
		e.ProxyPort > 0
}

func (e *BridgeEnvironment) applyProxy(proxy ProxySpec) {
	e.ProxyType = proxy.Type
	e.ProxyHost = proxy.Host
	e.ProxyPort = proxy.Port
	e.ProxyUsername = proxy.Username
	e.ProxyPassword = proxy.Password
}

func proxySpecFromRequest(req *bridgepb.ProxyConfig) ProxySpec {
	if req == nil {
		return ProxySpec{}
	}
	return ProxySpec{
		Type:     req.GetType(),
		Host:     req.GetHost(),
		Port:     req.GetPort(),
		Username: req.GetUsername(),
		Password: req.GetPassword(),
	}
}

func normalizeProxySpec(proxy ProxySpec) ProxySpec {
	return ProxySpec{
		Type:     strings.ToLower(strings.TrimSpace(proxy.Type)),
		Host:     strings.TrimSpace(proxy.Host),
		Port:     proxy.Port,
		Username: proxy.Username,
		Password: proxy.Password,
	}
}

func (p ProxySpec) IsEmpty() bool {
	return strings.TrimSpace(p.Type) == "" &&
		strings.TrimSpace(p.Host) == "" &&
		p.Port == 0 &&
		p.Username == "" &&
		p.Password == ""
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
