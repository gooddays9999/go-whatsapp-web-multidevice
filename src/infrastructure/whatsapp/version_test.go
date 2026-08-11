package whatsapp

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/store"
)

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func mockHTTPClient(body string, status int, err error) *http.Client {
	return &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		if err != nil {
			return nil, err
		}
		return &http.Response{
			StatusCode: status,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
			Request:    r,
		}, nil
	})}
}

func TestApplyLatestWAVersionUpdatesOnSuccess(t *testing.T) {
	orig := store.GetWAVersion()
	t.Cleanup(func() { store.SetWAVersion(orig) })

	// web.whatsapp.com embeds "client_revision":<n>, — the trailing comma matters.
	client := mockHTTPClient(`var x={"client_revision":1044929606,"foo":1};`, 200, nil)
	ApplyLatestWAVersion(context.Background(), client)

	got := store.GetWAVersion()
	want := store.WAVersionContainer{2, 3000, 1044929606}
	if got != want {
		t.Fatalf("version = %s, want %s", got.String(), want.String())
	}
}

func TestApplyLatestWAVersionKeepsCurrentOnFailure(t *testing.T) {
	orig := store.GetWAVersion()
	t.Cleanup(func() { store.SetWAVersion(orig) })

	baseline := store.WAVersionContainer{2, 3000, 1044142122}
	store.SetWAVersion(baseline)

	cases := []struct {
		name   string
		client *http.Client
	}{
		{"transport error", mockHTTPClient("", 0, fmt.Errorf("network down"))},
		{"non-200", mockHTTPClient("nope", 503, nil)},
		{"missing client_revision", mockHTTPClient("<html>no version here</html>", 200, nil)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ApplyLatestWAVersion(context.Background(), tc.client)
			if got := store.GetWAVersion(); got != baseline {
				t.Fatalf("version changed to %s on %s; must keep %s", got.String(), tc.name, baseline.String())
			}
		})
	}
}
