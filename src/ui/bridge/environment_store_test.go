package bridge

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	bridgepb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	_ "github.com/mattn/go-sqlite3"
)

func TestEnvironmentStoreUsesLatestConnectProxyAndKeepsUA(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	uaPath := filepath.Join(t.TempDir(), "ua.txt")
	if err := os.WriteFile(uaPath, []byte("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := Config{UAFilePath: uaPath}
	pool := LoadUAPool(uaPath)
	store := NewEnvironmentStore(db, pool, cfg)
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}

	first, created, err := store.GetOrCreate(ctx, "acc-1", "tenant", &bridgepb.ProxyConfig{
		Type: "socks5", Host: "127.0.0.1", Port: 1080, Username: "user", Password: "pass",
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatalf("expected first environment to be created")
	}
	proxyURL, err := first.ProxyURL()
	if err != nil {
		t.Fatal(err)
	}
	if proxyURL != "socks5://user:pass@127.0.0.1:1080" {
		t.Fatalf("unexpected proxy URL %q", proxyURL)
	}

	second, created, err := store.GetOrCreate(ctx, "acc-1", "tenant", &bridgepb.ProxyConfig{
		Type: "http", Host: "10.0.0.1", Port: 8080,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatalf("expected existing environment")
	}
	secondURL, err := second.ProxyURL()
	if err != nil {
		t.Fatal(err)
	}
	if secondURL != "http://10.0.0.1:8080" {
		t.Fatalf("proxy was not updated from latest Connect: got %q", secondURL)
	}
	if second.UserAgent != first.UserAgent {
		t.Fatalf("UA changed after second Connect")
	}
	if second.TenantID != "tenant" {
		t.Fatalf("tenant changed unexpectedly to %q", second.TenantID)
	}

	secondUpdated, created, err := store.GetOrCreate(ctx, "acc-1", "tenant-2", &bridgepb.ProxyConfig{
		Type: "http", Host: "10.0.0.2", Port: 8081,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatalf("expected existing environment when only tenant changes")
	}
	if secondUpdated.TenantID != "tenant-2" {
		t.Fatalf("tenant not updated: got %q, want tenant-2", secondUpdated.TenantID)
	}
	secondUpdatedURL, err := secondUpdated.ProxyURL()
	if err != nil {
		t.Fatal(err)
	}
	if secondUpdatedURL != "http://10.0.0.2:8081" {
		t.Fatalf("proxy was not updated with tenant update: got %q", secondUpdatedURL)
	}
	if secondUpdated.UserAgent != first.UserAgent {
		t.Fatalf("UA changed after tenant update")
	}

	preserved, created, err := store.GetOrCreate(ctx, "acc-1", "tenant-2", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatalf("expected existing environment when Connect omits proxy")
	}
	preservedURL, err := preserved.ProxyURL()
	if err != nil {
		t.Fatal(err)
	}
	if preservedURL != "http://10.0.0.2:8081" {
		t.Fatalf("expected existing proxy to be preserved when Connect has no proxy, got %q", preservedURL)
	}
	if preserved.UserAgent != first.UserAgent {
		t.Fatalf("UA changed after proxy preserve")
	}

	if err := store.Delete(ctx, "acc-1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.GetOrCreate(ctx, "acc-1", "tenant", nil, true); err == nil {
		t.Fatalf("expected new environment without proxy to fail")
	}
	third, created, err := store.GetOrCreate(ctx, "acc-1", "tenant", &bridgepb.ProxyConfig{
		Type: "http", Host: "10.0.0.1", Port: 8080,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatalf("expected environment after delete to be recreated")
	}
	thirdURL, err := third.ProxyURL()
	if err != nil {
		t.Fatal(err)
	}
	if thirdURL != "http://10.0.0.1:8080" {
		t.Fatalf("unexpected recreated proxy URL %q", thirdURL)
	}
}

func TestEnvironmentStoreGetCachedServesFromMemoryAndInvalidates(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewEnvironmentStore(db, newTestUAPool(), Config{})
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.GetOrCreate(ctx, "acc-1", "t1", &bridgepb.ProxyConfig{
		Type: "socks5", Host: "127.0.0.1", Port: 1080,
	}, true); err != nil {
		t.Fatal(err)
	}

	// Prime the cache.
	env, err := store.GetCached(ctx, "acc-1")
	if err != nil || env == nil || env.TenantID != "t1" {
		t.Fatalf("GetCached miss: env=%+v err=%v", env, err)
	}

	// Mutate the row directly (bypassing the store, so the cache is NOT
	// invalidated). GetCached must still serve the stale-but-cached value,
	// proving it does not hit the DB on every call.
	if _, err := db.ExecContext(ctx, `UPDATE bridge_environments SET tenant_id = 'OUTOFBAND' WHERE account_id = 'acc-1'`); err != nil {
		t.Fatal(err)
	}
	if env, err := store.GetCached(ctx, "acc-1"); err != nil || env == nil || env.TenantID != "t1" {
		t.Fatalf("expected cached t1, got env=%+v err=%v", env, err)
	}

	// A write through the store (tenant change) must invalidate the cache.
	if _, _, err := store.GetOrCreate(ctx, "acc-1", "t2", nil, true); err != nil {
		t.Fatal(err)
	}
	if env, err := store.GetCached(ctx, "acc-1"); err != nil || env == nil || env.TenantID != "t2" {
		t.Fatalf("expected invalidated t2 after write, got env=%+v err=%v", env, err)
	}

	// Delete must invalidate too: subsequent GetCached returns nil.
	if err := store.Delete(ctx, "acc-1"); err != nil {
		t.Fatal(err)
	}
	if env, err := store.GetCached(ctx, "acc-1"); err != nil || env != nil {
		t.Fatalf("expected nil after delete, got env=%+v err=%v", env, err)
	}
}

func TestEnvironmentStoreWarmCachePreloadsAllAccounts(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewEnvironmentStore(db, newTestUAPool(), Config{})
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"acc-1", "acc-2", "acc-3"} {
		if _, _, err := store.GetOrCreate(ctx, id, "t-"+id, &bridgepb.ProxyConfig{
			Type: "socks5", Host: "127.0.0.1", Port: 1080,
		}, true); err != nil {
			t.Fatal(err)
		}
	}

	if err := store.WarmCache(ctx); err != nil {
		t.Fatal(err)
	}

	// Delete every row directly (bypassing the store, so the cache is NOT
	// invalidated). If WarmCache preloaded the cache, GetCached still serves the
	// warmed values without touching the DB.
	if _, err := db.ExecContext(ctx, `DELETE FROM bridge_environments`); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"acc-1", "acc-2", "acc-3"} {
		env, err := store.GetCached(ctx, id)
		if err != nil || env == nil || env.TenantID != "t-"+id {
			t.Fatalf("account %s not served from warmed cache: env=%+v err=%v", id, env, err)
		}
	}
}

func TestEnvironmentStoreGetCachedTTLZeroDisablesCache(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewEnvironmentStore(db, newTestUAPool(), Config{})
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.GetOrCreate(ctx, "acc-1", "t1", &bridgepb.ProxyConfig{
		Type: "socks5", Host: "127.0.0.1", Port: 1080,
	}, true); err != nil {
		t.Fatal(err)
	}

	orig := environmentCacheTTL
	environmentCacheTTL = 0
	defer func() { environmentCacheTTL = orig }()

	if _, err := store.GetCached(ctx, "acc-1"); err != nil {
		t.Fatal(err)
	}
	// With caching disabled, an out-of-band change is observed immediately.
	if _, err := db.ExecContext(ctx, `UPDATE bridge_environments SET tenant_id = 'fresh' WHERE account_id = 'acc-1'`); err != nil {
		t.Fatal(err)
	}
	env, err := store.GetCached(ctx, "acc-1")
	if err != nil || env == nil || env.TenantID != "fresh" {
		t.Fatalf("expected fresh read with cache disabled, got env=%+v err=%v", env, err)
	}
}

func TestProxySpecURLValidation(t *testing.T) {
	if _, err := (ProxySpec{Type: "socks4", Host: "127.0.0.1", Port: 1080}).URL(); err == nil {
		t.Fatalf("expected unsupported proxy type to fail")
	}
	if _, err := (ProxySpec{Type: "socks5", Host: "", Port: 1080}).URL(); err == nil {
		t.Fatalf("expected missing host to fail")
	}
	if _, err := (ProxySpec{Type: "socks5", Host: "127.0.0.1", Port: 0}).URL(); err == nil {
		t.Fatalf("expected invalid port to fail")
	}
}

func TestEnvironmentStoreListByAccountIDs(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	store := NewEnvironmentStore(db, newTestUAPool(), Config{})
	if err := store.Init(ctx); err != nil {
		t.Fatal(err)
	}
	for _, accountID := range []string{"acc-1", "acc-2", "acc-3"} {
		if _, _, err := store.GetOrCreate(ctx, accountID, "tenant", &bridgepb.ProxyConfig{
			Type: "socks5", Host: "127.0.0.1", Port: 1080,
		}, true); err != nil {
			t.Fatal(err)
		}
	}

	envs, err := store.ListByAccountIDs(ctx, []string{"acc-3", "missing", "acc-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(envs) != 2 {
		t.Fatalf("expected 2 environments, got %d", len(envs))
	}
	if envs[0].AccountID != "acc-1" || envs[1].AccountID != "acc-3" {
		t.Fatalf("unexpected environments: %s, %s", envs[0].AccountID, envs[1].AccountID)
	}

	empty, err := store.ListByAccountIDs(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected empty result for empty account list, got %d", len(empty))
	}
}
