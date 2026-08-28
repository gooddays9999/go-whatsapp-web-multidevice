package usecase

import (
	"testing"
	"time"
)

func TestEphemeralExpirationCacheHitMissAndNegative(t *testing.T) {
	c := newEphemeralExpirationCache(5 * time.Minute)

	if _, ok := c.get("jid-1"); ok {
		t.Fatalf("expected miss for unknown jid")
	}

	c.put("jid-1", 604800)
	if v, ok := c.get("jid-1"); !ok || v != 604800 {
		t.Fatalf("expected hit 604800, got %d ok=%v", v, ok)
	}

	// A zero expiration (the common "no disappearing messages" case) must be
	// cached too, so negative lookups are served from memory.
	c.put("jid-2", 0)
	if v, ok := c.get("jid-2"); !ok || v != 0 {
		t.Fatalf("expected cached zero, got %d ok=%v", v, ok)
	}
}

func TestEphemeralExpirationCacheGenerationalEviction(t *testing.T) {
	c := newEphemeralExpirationCache(time.Hour)

	c.put("a", 1)
	if v, ok := c.get("a"); !ok || v != 1 {
		t.Fatalf("a should be present, got %d ok=%v", v, ok)
	}

	// Force one rotation: "a" moves from cur -> prev, still reachable.
	c.rotateAt = time.Now().Add(-time.Second)
	c.put("b", 2)
	if v, ok := c.get("a"); !ok || v != 1 {
		t.Fatalf("a should survive one rotation via prev, got %d ok=%v", v, ok)
	}
	if v, ok := c.get("b"); !ok || v != 2 {
		t.Fatalf("b should be present, got %d ok=%v", v, ok)
	}

	// Force a second rotation: "a" (which was only in prev) is now dropped,
	// proving memory is bounded to ~two generations rather than growing forever.
	c.rotateAt = time.Now().Add(-time.Second)
	c.put("c", 3)
	if _, ok := c.get("a"); ok {
		t.Fatalf("a should have been evicted after two rotations")
	}
	if v, ok := c.get("b"); !ok || v != 2 {
		t.Fatalf("b should survive one rotation, got %d ok=%v", v, ok)
	}
	if v, ok := c.get("c"); !ok || v != 3 {
		t.Fatalf("c should be present, got %d ok=%v", v, ok)
	}
}
