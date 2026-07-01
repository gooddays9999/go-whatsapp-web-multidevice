package bridge

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	_ "github.com/mattn/go-sqlite3"
)

func TestAccountProxyStoreProxyForAccount(t *testing.T) {
	ctx := context.Background()
	db := newAccountProxyTestDB(t)
	store := &AccountProxyStore{db: db}

	got, err := store.ProxyForAccount(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Found {
		t.Fatal("expected account proxy to be found")
	}
	if !got.HasProxyID {
		t.Fatal("expected account 1 to have a proxy id")
	}
	proxy := got.Proxy
	if proxy.Type != "socks5" || proxy.Host != "127.0.0.1" || proxy.Port != 1080 || proxy.Username != "user" || proxy.Password != "pass" {
		t.Fatalf("unexpected proxy: %#v", proxy)
	}

	phone, err := store.ProxyForAccount(ctx, "15510000001")
	if err != nil {
		t.Fatal(err)
	}
	if !phone.Found || phone.Proxy.Host != proxy.Host {
		t.Fatalf("expected phone lookup to return same proxy, lookup=%#v", phone)
	}

	noProxy, err := store.ProxyForAccount(ctx, "2")
	if err != nil {
		t.Fatal(err)
	}
	if !noProxy.Found {
		t.Fatal("expected account without proxy row to still be found")
	}
	if noProxy.HasProxyID {
		t.Fatal("expected account 2 to have no proxy id")
	}
	if !noProxy.Proxy.IsEmpty() {
		t.Fatalf("expected empty proxy for account without proxy, got %#v", noProxy.Proxy)
	}

	deletedProxy, err := store.ProxyForAccount(ctx, "9")
	if err != nil {
		t.Fatal(err)
	}
	if !deletedProxy.Found || !deletedProxy.HasProxyID {
		t.Fatalf("expected account 9 to be found with a proxy id, got %#v", deletedProxy)
	}
	if !deletedProxy.Proxy.IsEmpty() {
		t.Fatalf("expected proxy_id pointing to a missing proxy to resolve empty, got %#v", deletedProxy.Proxy)
	}

	missing, err := store.ProxyForAccount(ctx, "3")
	if err != nil {
		t.Fatal(err)
	}
	if missing.Found {
		t.Fatal("expected deleted account to not be found")
	}

	// 非规范数字串(前导零)不可能等于 CAST(id AS CHAR),必须走 phone-only 回退分支。
	leadingZero, err := store.ProxyForAccount(ctx, "015510000011")
	if err != nil {
		t.Fatal(err)
	}
	if !leadingZero.Found || leadingZero.Proxy.Host != "127.0.0.1" {
		t.Fatalf("expected leading-zero phone to resolve via phone-only fallback, got %#v", leadingZero)
	}
}

func TestCanonicalAccountID(t *testing.T) {
	for _, tc := range []struct {
		in string
		id uint64
		ok bool
	}{
		{"44190", 44190, true},
		{"0", 0, true},
		{"044190", 0, false},       // 前导零:CAST(id AS CHAR) 不可能产生
		{"+15510000001", 0, false}, // 符号前缀
		{"-5", 0, false},           // 负号
		{"abc", 0, false},          // 非数字
		{"", 0, false},             // 空串(调用方已前置拦截,双保险)
		{"9223372036854775807", 9223372036854775807, true}, // signed BIGINT 上界
		{"9223372036854775808", 0, false},                  // 超出 signed BIGINT 域
		{"18446744073709551616", 0, false},                 // uint64 溢出
	} {
		id, ok := canonicalAccountID(tc.in)
		if id != tc.id || ok != tc.ok {
			t.Fatalf("canonicalAccountID(%q) = (%d,%v), want (%d,%v)", tc.in, id, ok, tc.id, tc.ok)
		}
	}
}

func TestAccountProxyStoreWebOnlineForAccount(t *testing.T) {
	ctx := context.Background()
	db := newAccountProxyTestDB(t)
	store := &AccountProxyStore{db: db}

	webOnline, found, err := store.WebOnlineForAccount(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}
	if !found || webOnline != 1 {
		t.Fatalf("expected online account, found=%v web_online=%d", found, webOnline)
	}

	webOnline, found, err = store.WebOnlineForAccount(ctx, "15510000002")
	if err != nil {
		t.Fatal(err)
	}
	if !found || webOnline != 2 {
		t.Fatalf("expected offline account by phone, found=%v web_online=%d", found, webOnline)
	}

	_, found, err = store.WebOnlineForAccount(ctx, "999")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected missing account to not be found")
	}

	_, found, err = store.WebOnlineForAccount(ctx, "3")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected deleted account to not be found")
	}
}

func TestAccountProxyStoreRestorableAccountIDs(t *testing.T) {
	ctx := context.Background()
	db := newAccountProxyTestDB(t)
	store := &AccountProxyStore{db: db}

	accountIDs, err := store.RestorableAccountIDs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"1", "8"}
	if !reflect.DeepEqual(accountIDs, want) {
		t.Fatalf("RestorableAccountIDs() = %#v, want %#v", accountIDs, want)
	}
}

func TestEnvironmentForAccountPrefersAccountDatabaseProxy(t *testing.T) {
	ctx := context.Background()
	envDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer envDB.Close()

	envStore := NewEnvironmentStore(envDB, newTestUAPool(), Config{})
	if err := envStore.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := envStore.GetOrCreate(ctx, "1", "tenant", &bridgepb.ProxyConfig{
		Type: "socks5", Host: "old.proxy", Port: 1000, Username: "old", Password: "oldpass",
	}, true); err != nil {
		t.Fatal(err)
	}

	accountDB := newAccountProxyTestDB(t)
	service := &Service{
		envStore:          envStore,
		accountProxyStore: &AccountProxyStore{db: accountDB},
	}

	env, created, err := service.environmentForAccount(ctx, "1", "tenant", &bridgepb.ProxyConfig{
		Type: "socks5", Host: "request.proxy", Port: 1001,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("expected existing environment to be updated")
	}
	proxyURL, err := env.ProxyURL()
	if err != nil {
		t.Fatal(err)
	}
	if proxyURL != "socks5://user:pass@127.0.0.1:1080" {
		t.Fatalf("expected account database proxy, got %q", proxyURL)
	}

	if _, _, err := service.environmentForAccount(ctx, "2", "tenant", nil, false); err == nil {
		t.Fatal("expected account without proxy to fail")
	}
}

func TestEnvironmentForAccountProxyErrorTyping(t *testing.T) {
	ctx := context.Background()
	accountDB := newAccountProxyTestDB(t)
	envDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer envDB.Close()
	envStore := NewEnvironmentStore(envDB, newTestUAPool(), Config{})
	if err := envStore.Init(ctx); err != nil {
		t.Fatal(err)
	}
	svc := &Service{envStore: envStore, accountProxyStore: &AccountProxyStore{db: accountDB}}

	cases := []struct {
		name       string
		account    string
		wantSubstr string
	}{
		{"account without proxy_id", "2", "has no proxy configured"},
		{"proxy_id set but deleted/invalid", "9", "resolves to empty"},
		{"account not found", "404", "not found in account database"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := svc.environmentForAccount(ctx, tc.account, "tenant", nil, false)
			if err == nil {
				t.Fatalf("account %s: expected error, got nil", tc.account)
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("account %s error = %q, want substring %q", tc.account, err.Error(), tc.wantSubstr)
			}
		})
	}
}

func newAccountProxyTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, stmt := range []string{
		`CREATE TABLE accounts (id INTEGER PRIMARY KEY, phone TEXT, proxy_id INTEGER, web_online INTEGER DEFAULT 0, deleted_at TIMESTAMP NULL)`,
		`CREATE TABLE proxies (id INTEGER PRIMARY KEY, type TEXT, host TEXT, port INTEGER, username TEXT, password TEXT)`,
		`INSERT INTO proxies (id, type, host, port, username, password) VALUES (10, 'SOCKS5', '127.0.0.1', 1080, 'user', 'pass')`,
		`INSERT INTO proxies (id, type, host, port, username, password) VALUES (11, 'HTTP', '10.0.0.1', 8080, '', '')`,
		`INSERT INTO proxies (id, type, host, port, username, password) VALUES (12, 'SOCKS5', '', 1080, '', '')`,
		`INSERT INTO accounts (id, phone, proxy_id, web_online, deleted_at) VALUES (1, '15510000001', 10, 1, NULL)`,
		`INSERT INTO accounts (id, phone, proxy_id, web_online, deleted_at) VALUES (2, '15510000002', NULL, 2, NULL)`,
		`INSERT INTO accounts (id, phone, proxy_id, web_online, deleted_at) VALUES (3, '15510000003', 10, 1, CURRENT_TIMESTAMP)`,
		`INSERT INTO accounts (id, phone, proxy_id, web_online, deleted_at) VALUES (4, '15510000004', 10, 0, NULL)`,
		`INSERT INTO accounts (id, phone, proxy_id, web_online, deleted_at) VALUES (5, '15510000005', 10, 3, NULL)`,
		`INSERT INTO accounts (id, phone, proxy_id, web_online, deleted_at) VALUES (6, '15510000006', 10, 2, NULL)`,
		`INSERT INTO accounts (id, phone, proxy_id, web_online, deleted_at) VALUES (7, '15510000007', 12, 1, NULL)`,
		`INSERT INTO accounts (id, phone, proxy_id, web_online, deleted_at) VALUES (8, '15510000008', 11, 1, NULL)`,
		`INSERT INTO accounts (id, phone, proxy_id, web_online, deleted_at) VALUES (9, '15510000009', 999, 3, NULL)`,
		`INSERT INTO accounts (id, phone, proxy_id, web_online, deleted_at) VALUES (11, '015510000011', 10, 0, NULL)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func newTestUAPool() *UAPool {
	return &UAPool{items: []UAMetadata{{
		UserAgent:     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
		BrowserFamily: "Chrome",
		OSName:        "Windows",
	}}}
}
