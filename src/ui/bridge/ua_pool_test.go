package bridge

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadUAPoolFiltersAndSelectsStable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ua.txt")
	content := `
Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36
bad
Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36
Mozilla/5.0 (Macintosh; Intel Mac OS X 15_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15
`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	pool := LoadUAPool(path)
	if pool.Len() != 2 {
		t.Fatalf("expected 2 valid unique UAs, got %d", pool.Len())
	}
	first := pool.Select("account-1")
	second := pool.Select("account-1")
	if first != second {
		t.Fatalf("expected stable UA selection")
	}
	if first.BrowserFamily == "" || first.OSName == "" {
		t.Fatalf("expected parsed browser and OS, got %#v", first)
	}
}

func TestParseUA(t *testing.T) {
	tests := []struct {
		ua      string
		browser string
		os      string
	}{
		{"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36 Edg/144.0.3179.85", "Edge", "Windows"},
		{"Mozilla/5.0 (Macintosh; Intel Mac OS X 15_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15", "Safari", "macOS"},
		{"Mozilla/5.0 (X11; Linux x86_64; rv:134.0) Gecko/20100101 Firefox/134.0", "Firefox", "Linux"},
		{"Mozilla/5.0 (X11; CrOS x86_64 15823.42.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.7559.42 Safari/537.36", "Chrome", "ChromeOS"},
	}
	for _, tt := range tests {
		got := ParseUA(tt.ua)
		if got.BrowserFamily != tt.browser || got.OSName != tt.os {
			t.Fatalf("ParseUA(%q) = %s/%s, want %s/%s", tt.ua, got.BrowserFamily, got.OSName, tt.browser, tt.os)
		}
	}
}
