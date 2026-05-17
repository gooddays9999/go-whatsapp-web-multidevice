package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type AccountProxyStore struct {
	db *sql.DB
}

func NewAccountProxyStore(dsn string) (*AccountProxyStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping account database: %w", err)
	}
	return &AccountProxyStore{db: db}, nil
}

func (s *AccountProxyStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *AccountProxyStore) ProxyForAccount(ctx context.Context, accountID string) (ProxySpec, bool, error) {
	if s == nil || s.db == nil {
		return ProxySpec{}, false, nil
	}
	if accountID == "" {
		return ProxySpec{}, false, fmt.Errorf("account_id is required")
	}

	row := s.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(p.type, ''),
			COALESCE(p.host, ''),
			COALESCE(p.port, 0),
			COALESCE(p.username, ''),
			COALESCE(p.password, '')
		FROM accounts a
		LEFT JOIN proxies p ON p.id = a.proxy_id
		WHERE a.deleted_at IS NULL
		  AND (CAST(a.id AS CHAR) = ? OR a.phone = ?)
		LIMIT 1
	`, accountID, accountID)

	var proxy ProxySpec
	if err := row.Scan(&proxy.Type, &proxy.Host, &proxy.Port, &proxy.Username, &proxy.Password); err != nil {
		if err == sql.ErrNoRows {
			return ProxySpec{}, false, nil
		}
		return ProxySpec{}, false, err
	}
	return normalizeProxySpec(proxy), true, nil
}
