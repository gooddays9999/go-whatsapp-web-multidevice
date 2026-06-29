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

// AccountProxyLookup is the outcome of resolving an account's proxy, with
// enough detail to tell apart the distinct failure modes (account missing vs
// no proxy assigned vs proxy assigned but unresolvable).
type AccountProxyLookup struct {
	Proxy      ProxySpec
	Found      bool // the (non-deleted) account row exists
	HasProxyID bool // account.proxy_id is assigned (non-null and > 0)
}

func (s *AccountProxyStore) ProxyForAccount(ctx context.Context, accountID string) (AccountProxyLookup, error) {
	if s == nil || s.db == nil {
		return AccountProxyLookup{}, nil
	}
	if accountID == "" {
		return AccountProxyLookup{}, fmt.Errorf("account_id is required")
	}

	row := s.db.QueryRowContext(ctx, `
		SELECT
			a.proxy_id,
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

	var proxyID sql.NullInt64
	var proxy ProxySpec
	if err := row.Scan(&proxyID, &proxy.Type, &proxy.Host, &proxy.Port, &proxy.Username, &proxy.Password); err != nil {
		if err == sql.ErrNoRows {
			return AccountProxyLookup{}, nil
		}
		return AccountProxyLookup{}, err
	}
	return AccountProxyLookup{
		Proxy:      normalizeProxySpec(proxy),
		Found:      true,
		HasProxyID: proxyID.Valid && proxyID.Int64 > 0,
	}, nil
}

func (s *AccountProxyStore) WebOnlineForAccount(ctx context.Context, accountID string) (int, bool, error) {
	if s == nil || s.db == nil {
		return 0, false, nil
	}
	if accountID == "" {
		return 0, false, fmt.Errorf("account_id is required")
	}

	row := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(a.web_online, 0)
		FROM accounts a
		WHERE a.deleted_at IS NULL
		  AND (CAST(a.id AS CHAR) = ? OR a.phone = ?)
		LIMIT 1
	`, accountID, accountID)

	var webOnline int
	if err := row.Scan(&webOnline); err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return 0, false, err
	}
	return webOnline, true, nil
}

func (s *AccountProxyStore) RestorableAccountIDs(ctx context.Context) ([]string, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT CAST(a.id AS CHAR)
		FROM accounts a
		INNER JOIN proxies p ON p.id = a.proxy_id
		WHERE a.deleted_at IS NULL
		  AND COALESCE(a.web_online, 0) = ?
		  AND LOWER(TRIM(COALESCE(p.type, ''))) IN ('socks5', 'http', 'https')
		  AND TRIM(COALESCE(p.host, '')) <> ''
		  AND COALESCE(p.port, 0) BETWEEN 1 AND 65535
		ORDER BY a.id
	`, accountWebOnlineOnline)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	accountIDs := make([]string, 0)
	for rows.Next() {
		var accountID string
		if err := rows.Scan(&accountID); err != nil {
			return nil, err
		}
		accountIDs = append(accountIDs, accountID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return accountIDs, nil
}
