package bridge

import (
	"fmt"
	"strings"

	"go.mau.fi/whatsmeow"
)

func pairClientInfo(env *BridgeEnvironment) (whatsmeow.PairClientType, string) {
	browser := "Chrome"
	osName := "Linux"
	if env != nil {
		browserFamily := env.BrowserFamily
		parsedOS := env.OSName
		if (strings.TrimSpace(browserFamily) == "" || strings.TrimSpace(parsedOS) == "") && strings.TrimSpace(env.UserAgent) != "" {
			meta := ParseUA(env.UserAgent)
			if strings.TrimSpace(browserFamily) == "" {
				browserFamily = meta.BrowserFamily
			}
			if strings.TrimSpace(parsedOS) == "" {
				parsedOS = meta.OSName
			}
		}
		if normalized := normalizePairBrowser(browserFamily); normalized != "" {
			browser = normalized
		}
		if normalized := normalizePairOS(parsedOS); normalized != "" {
			osName = normalized
		}
	}
	return pairClientType(browser), fmt.Sprintf("%s (%s)", browser, osName)
}

func pairClientType(browser string) whatsmeow.PairClientType {
	switch strings.ToLower(strings.TrimSpace(browser)) {
	case "edge":
		return whatsmeow.PairClientEdge
	case "firefox":
		return whatsmeow.PairClientFirefox
	case "safari":
		return whatsmeow.PairClientSafari
	case "chrome", "chromium":
		return whatsmeow.PairClientChrome
	default:
		return whatsmeow.PairClientChrome
	}
}

func normalizePairBrowser(browser string) string {
	switch strings.ToLower(strings.TrimSpace(browser)) {
	case "edge", "microsoft edge":
		return "Edge"
	case "firefox", "mozilla firefox":
		return "Firefox"
	case "safari":
		return "Safari"
	case "chrome", "chromium", "google chrome":
		return "Chrome"
	default:
		return ""
	}
}

func normalizePairOS(osName string) string {
	switch strings.ToLower(strings.TrimSpace(osName)) {
	case "windows", "win":
		return "Windows"
	case "macos", "mac os", "mac os x", "darwin":
		return "macOS"
	case "linux":
		return "Linux"
	case "chromeos", "chrome os":
		return "ChromeOS"
	default:
		return ""
	}
}
