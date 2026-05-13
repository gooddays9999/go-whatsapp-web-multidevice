package bridge

import (
	"bufio"
	"hash/fnv"
	"os"
	"regexp"
	"strings"
)

type UAMetadata struct {
	UserAgent     string
	BrowserFamily string
	OSName        string
}

type UAPool struct {
	items []UAMetadata
}

func LoadUAPool(path string) *UAPool {
	file, err := os.Open(path)
	if err != nil {
		return &UAPool{}
	}
	defer file.Close()

	seen := make(map[string]bool)
	items := make([]UAMetadata, 0, 128)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		ua := strings.TrimSpace(scanner.Text())
		if ua == "" || seen[ua] || !looksLikeUA(ua) {
			continue
		}
		seen[ua] = true
		items = append(items, ParseUA(ua))
	}
	return &UAPool{items: items}
}

func (p *UAPool) Select(accountID string) UAMetadata {
	if p == nil || len(p.items) == 0 {
		return UAMetadata{}
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(accountID))
	return p.items[int(hash.Sum32())%len(p.items)]
}

func (p *UAPool) Len() int {
	if p == nil {
		return 0
	}
	return len(p.items)
}

var uaPrefixPattern = regexp.MustCompile(`^Mozilla/\d+\.\d+ \(.+\) .+`)

func looksLikeUA(ua string) bool {
	return uaPrefixPattern.MatchString(ua)
}

func ParseUA(ua string) UAMetadata {
	meta := UAMetadata{
		UserAgent:     ua,
		BrowserFamily: parseBrowser(ua),
		OSName:        parseOS(ua),
	}
	return meta
}

func parseBrowser(ua string) string {
	switch {
	case strings.Contains(ua, "Edg/"):
		return "Edge"
	case strings.Contains(ua, "Firefox/"):
		return "Firefox"
	case strings.Contains(ua, "Safari/") && strings.Contains(ua, "Version/") && !strings.Contains(ua, "Chrome/"):
		return "Safari"
	case strings.Contains(ua, "Chrome/") || strings.Contains(ua, "Chromium/"):
		return "Chrome"
	default:
		return ""
	}
}

func parseOS(ua string) string {
	switch {
	case strings.Contains(ua, "Windows NT"):
		return "Windows"
	case strings.Contains(ua, "Macintosh") || strings.Contains(ua, "Mac OS X"):
		return "macOS"
	case strings.Contains(ua, "CrOS"):
		return "ChromeOS"
	case strings.Contains(ua, "Linux") || strings.Contains(ua, "X11"):
		return "Linux"
	default:
		return ""
	}
}
