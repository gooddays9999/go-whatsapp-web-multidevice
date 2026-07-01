package bridge

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
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

// canonicalAccountID reports whether s is the canonical decimal form of an
// account primary key — i.e. exactly what CAST(id AS CHAR) would produce.
// Non-canonical strings (leading zeros, signs, non-digits, overflow) can never
// equal the cast of an id, so callers safely fall back to phone-only matching.
// bitSize 63 keeps the value inside the signed-BIGINT id domain (larger values
// cannot exist as ids) and inside what every sql driver binds losslessly.
func canonicalAccountID(s string) (uint64, bool) {
	id, err := strconv.ParseUint(s, 10, 63)
	if err != nil || strconv.FormatUint(id, 10) != s {
		return 0, false
	}
	return id, true
}

// accountIDOrPhoneWhere returns the WHERE tail and args matching an account by
// canonical id or phone. The previous form `(CAST(a.id AS CHAR) = ? OR a.phone = ?)`
// defeated every index (full scan of ~25k rows per call, 5-6 always-on concurrent
// executions ≈ 5 CPU cores on prod MySQL); `(a.id = ? OR a.phone = ?)` uses
// index_merge(PRIMARY, idx_accounts_phone) and examines ~2 rows instead.
func accountIDOrPhoneWhere(accountID string) (string, []any) {
	if id, ok := canonicalAccountID(accountID); ok {
		return "(a.id = ? OR a.phone = ?)", []any{id, accountID}
	}
	return "a.phone = ?", []any{accountID}
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

	where, args := accountIDOrPhoneWhere(accountID)
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
		  AND `+where+`
		LIMIT 1
	`, args...)

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

	where, args := accountIDOrPhoneWhere(accountID)
	row := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(a.web_online, 0)
		FROM accounts a
		WHERE a.deleted_at IS NULL
		  AND `+where+`
		LIMIT 1
	`, args...)

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
