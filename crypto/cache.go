package crypto

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// Default sizing for the per-pod DEK cache.
const (
	DefaultCacheCapacity = 4096
	DefaultCacheTTL      = 2 * time.Hour
)

// CachedProvider wraps a KeyProvider with a bounded in-memory LRU+TTL cache of
// unwrapped DEKs, plus a singleflight so concurrent misses for the same user collapse
// to a single upstream unwrap (thundering-herd guard on a fresh pod).
//
// It is designed for PER-POD use: the plaintext DEK lives only in this process's
// memory and is NEVER persisted to Redis/disk. Pod death loses the cache, not data —
// the next request re-unwraps from the durable wrapped DEK via the inner provider.
type CachedProvider struct {
	inner KeyProvider
	cap   int
	ttl   time.Duration
	now   func() time.Time // injectable for tests

	mu    sync.Mutex
	ll    *list.List
	items map[string]*list.Element

	sf singleflight
}

type cachedDEK struct {
	userID   string
	dek      []byte
	expireAt time.Time
}

// NewCachedProvider wraps inner. capacity<=0 and ttl<=0 fall back to the defaults.
func NewCachedProvider(inner KeyProvider, capacity int, ttl time.Duration) *CachedProvider {
	if capacity <= 0 {
		capacity = DefaultCacheCapacity
	}
	if ttl <= 0 {
		ttl = DefaultCacheTTL
	}
	return &CachedProvider{
		inner: inner,
		cap:   capacity,
		ttl:   ttl,
		now:   time.Now,
		ll:    list.New(),
		items: make(map[string]*list.Element),
	}
}

// UnwrapDEK returns a cached DEK or fetches+caches it via the inner provider.
func (c *CachedProvider) UnwrapDEK(ctx context.Context, userID string) ([]byte, error) {
	if dek, ok := c.get(userID); ok {
		return dek, nil
	}
	return c.sf.Do(userID, func() ([]byte, error) {
		if dek, ok := c.get(userID); ok { // lost the singleflight race
			return dek, nil
		}
		dek, err := c.inner.UnwrapDEK(ctx, userID)
		if err != nil {
			return nil, err
		}
		c.set(userID, dek)
		return dek, nil
	})
}

// Evict drops a user's DEK (call on logout / account deletion / key rotation).
func (c *CachedProvider) Evict(userID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[userID]; ok {
		c.ll.Remove(el)
		delete(c.items, userID)
	}
}

// Len reports cached DEK count (observability/tests).
func (c *CachedProvider) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

func (c *CachedProvider) get(userID string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[userID]
	if !ok {
		return nil, false
	}
	ent := el.Value.(*cachedDEK)
	if c.now().After(ent.expireAt) {
		c.ll.Remove(el)
		delete(c.items, userID)
		return nil, false
	}
	c.ll.MoveToFront(el)
	return ent.dek, true
}

func (c *CachedProvider) set(userID string, dek []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[userID]; ok {
		ent := el.Value.(*cachedDEK)
		ent.dek = dek
		ent.expireAt = c.now().Add(c.ttl)
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(&cachedDEK{userID: userID, dek: dek, expireAt: c.now().Add(c.ttl)})
	c.items[userID] = el
	for c.ll.Len() > c.cap {
		oldest := c.ll.Back()
		if oldest == nil {
			break
		}
		c.ll.Remove(oldest)
		delete(c.items, oldest.Value.(*cachedDEK).userID)
	}
}

// singleflight collapses concurrent calls for the same key into one execution of fn.
// Minimal in-package implementation to avoid pulling golang.org/x/sync.
type singleflight struct {
	mu sync.Mutex
	m  map[string]*sfCall
}

type sfCall struct {
	wg  sync.WaitGroup
	val []byte
	err error
}

func (g *singleflight) Do(key string, fn func() ([]byte, error)) ([]byte, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*sfCall)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := new(sfCall)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return c.val, c.err
}
