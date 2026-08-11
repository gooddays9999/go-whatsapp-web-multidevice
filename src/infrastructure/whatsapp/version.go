package whatsapp

import (
	"context"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
)

// defaultWAVersionFetchTimeout bounds the one-shot startup fetch so an
// unreachable web.whatsapp.com cannot stall boot.
const defaultWAVersionFetchTimeout = 15 * time.Second

// FetchAndApplyLatestWAVersion is the startup entry point: it builds a
// timeout-bounded HTTP client and applies the newest WhatsApp Web version,
// best-effort. Failures never block startup — the compiled-in version stays in
// place and a slightly stale version still connects.
func FetchAndApplyLatestWAVersion(ctx context.Context, timeout time.Duration) {
	if timeout <= 0 {
		timeout = defaultWAVersionFetchTimeout
	}
	fetchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ApplyLatestWAVersion(fetchCtx, &http.Client{Timeout: timeout})
}

// ApplyLatestWAVersion fetches the current WhatsApp Web client version from
// web.whatsapp.com and applies it process-wide via store.SetWAVersion, so every
// client announces a fresh version instead of the one frozen into the vendored
// whatsmeow. WhatsApp deprecates old web versions over time; a bridge pinned to a
// stale version eventually gets rejected at connect.
//
// The fetch is best-effort: on any error (or an empty result) it logs and leaves
// the compiled-in version in place — a slightly stale version still connects, so
// a transient failure must never block startup. httpClient is injected so the
// behaviour is testable without reaching the network.
func ApplyLatestWAVersion(ctx context.Context, httpClient *http.Client) {
	current := store.GetWAVersion()
	ver, err := whatsmeow.GetLatestVersion(ctx, httpClient)
	if err != nil {
		logrus.WithError(err).WithField("current", current.String()).
			Warn("could not fetch latest WhatsApp web version; keeping compiled-in version")
		return
	}
	if ver == nil || ver.IsZero() {
		logrus.WithField("current", current.String()).
			Warn("fetched WhatsApp web version was empty; keeping compiled-in version")
		return
	}
	if *ver == current {
		logrus.Infof("WhatsApp web version already current at %s", current.String())
		return
	}
	store.SetWAVersion(*ver)
	logrus.Infof("WhatsApp web version updated %s -> %s (fetched from web.whatsapp.com)", current.String(), ver.String())
}
