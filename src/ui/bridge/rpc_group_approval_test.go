package bridge

import "testing"

func TestGroupApprovalActionFailsClosed(t *testing.T) {
	if _, err := groupApprovalAction("approve"); err != nil {
		t.Fatal(err)
	}
	if _, err := groupApprovalAction("reject"); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"", "add", "promote", "logout"} {
		if _, err := groupApprovalAction(value); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
}

func TestGroupExpirationOnlyAllowsWhatsAppPresets(t *testing.T) {
	for _, seconds := range []int64{0, 86400, 604800, 7776000} {
		if !validGroupExpiration(seconds) {
			t.Fatalf("rejected %d", seconds)
		}
	}
	for _, seconds := range []int64{-1, 1, 3600, 99999999} {
		if validGroupExpiration(seconds) {
			t.Fatalf("accepted %d", seconds)
		}
	}
}
