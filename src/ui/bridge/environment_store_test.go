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

	cleared, created, err := store.GetOrCreate(ctx, "acc-1", "tenant-2", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatalf("expected existing environment when clearing proxy")
	}
	clearedURL, err := cleared.ProxyURL()
	if err != nil {
		t.Fatal(err)
	}
	if clearedURL != "" {
		t.Fatalf("expected proxy to be cleared when Connect has no proxy, got %q", clearedURL)
	}
	if cleared.UserAgent != first.UserAgent {
		t.Fatalf("UA changed after proxy clear")
	}

	if err := store.Delete(ctx, "acc-1"); err != nil {
		t.Fatal(err)
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
