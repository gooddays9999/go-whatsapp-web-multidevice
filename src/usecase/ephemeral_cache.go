package usecase

import (
	"sync"
	"time"
)

// ephemeralExpirationCacheTTL bounds how long a chat's disappearing-message
// expiration is served from memory on the hot outgoing-send path before a fresh
// chatstorage (SQLite) GetChat lookup. The value changes rarely (only when a
// chat's disappearing-message timer is changed), so short-lived staleness is
// acceptable and avoids a per-send SQLite query that would otherwise queue on
// the small chat-storage connection pool under high send volume. A cached entry
// lives at most 2*TTL (see ephemeralExpirationCache). Set to 0 to disable.
var ephemeralExpirationCacheTTL = 5 * time.Minute

// ephemeralExpirationCache is a memory-bounded TTL cache keyed by recipient JID.
//
// A broadcast/farming workload sends to an unbounded set of recipients, so a
// plain sync.Map keyed by JID would grow without limit and leak memory. Instead
// we keep two generations of entries and rotate every TTL, dropping the older
// generation wholesale. This caps live memory at roughly two TTL-windows of
// distinct recipients with no background janitor goroutine. Because a lookup may
// hit the previous generation, a cached value can live up to 2*TTL.
type ephemeralExpirationCache struct {
	mu       sync.RWMutex
	ttl      time.Duration
	rotateAt time.Time
	cur      map[string]uint32
	prev     map[string]uint32
}

func newEphemeralExpirationCache(ttl time.Duration) *ephemeralExpirationCache {
	return &ephemeralExpirationCache{
		ttl: ttl,
		cur: make(map[string]uint32),
	}
}

// get returns the cached expiration for jid from either generation.
func (c *ephemeralExpirationCache) get(jid string) (uint32, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if v, ok := c.cur[jid]; ok {
		return v, true
	}
	if v, ok := c.prev[jid]; ok {
		return v, true
	}
	return 0, false
}

// put stores expiration for jid, rotating generations when the TTL window has
// elapsed so the previous generation (and its memory) is released.
func (c *ephemeralExpirationCache) put(jid string, expiration uint32) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.rotateAt.IsZero():
		c.rotateAt = now.Add(c.ttl)
	case now.After(c.rotateAt):
		c.prev = c.cur
		c.cur = make(map[string]uint32, len(c.prev))
		c.rotateAt = now.Add(c.ttl)
	}
	c.cur[jid] = expiration
}

// defaultEphemeralExpirationCache backs getDefaultEphemeralExpiration. It is
// package-level so it is shared across all send operations.
var defaultEphemeralExpirationCache = newEphemeralExpirationCache(ephemeralExpirationCacheTTL)
