package bridge

import (
	"context"
	"database/sql"
	"reflect"
	"testing"

	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	_ "github.com/mattn/go-sqlite3"
)

func TestAccountProxyStoreProxyForAccount(t *testing.T) {
	ctx := context.Background()
	db := newAccountProxyTestDB(t)
	store := &AccountProxyStore{db: db}

	proxy, found, err := store.ProxyForAccount(ctx, "1")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected account proxy to be found")
	}
	if proxy.Type != "socks5" || proxy.Host != "127.0.0.1" || proxy.Port != 1080 || proxy.Username != "user" || proxy.Password != "pass" {
		t.Fatalf("unexpected proxy: %#v", proxy)
	}

	phoneProxy, found, err := store.ProxyForAccount(ctx, "15510000001")
	if err != nil {
		t.Fatal(err)
	}
	if !found || phoneProxy.Host != proxy.Host {
		t.Fatalf("expected phone lookup to return same proxy, found=%v proxy=%#v", found, phoneProxy)
	}

	missingProxy, found, err := store.ProxyForAccount(ctx, "2")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected account without proxy row to still be found")
	}
	if !missingProxy.IsEmpty() {
		t.Fatalf("expected empty proxy for account without proxy, got %#v", missingProxy)
	}

	_, found, err = store.ProxyForAccount(ctx, "3")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("expected deleted account to not be found")
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
