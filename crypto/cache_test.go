package crypto

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type countingProvider struct {
	calls int32
	delay time.Duration
	dek   []byte
}

func (p *countingProvider) UnwrapDEK(_ context.Context, _ string) ([]byte, error) {
	atomic.AddInt32(&p.calls, 1)
	if p.delay > 0 {
		time.Sleep(p.delay)
	}
	return p.dek, nil
}

func TestCachedProvider_HitMiss(t *testing.T) {
	p := &countingProvider{dek: testKey()}
	c := NewCachedProvider(p, 10, time.Hour)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := c.UnwrapDEK(ctx, "u-1"); err != nil {
			t.Fatal(err)
		}
	}
	if got := atomic.LoadInt32(&p.calls); got != 1 {
		t.Fatalf("expected 1 upstream call, got %d", got)
	}
}

func TestCachedProvider_TTLExpiry(t *testing.T) {
	p := &countingProvider{dek: testKey()}
	c := NewCachedProvider(p, 10, time.Minute)
	now := time.Unix(0, 0)
	c.now = func() time.Time { return now }
	ctx := context.Background()

	if _, err := c.UnwrapDEK(ctx, "u-1"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute) // past TTL
	if _, err := c.UnwrapDEK(ctx, "u-1"); err != nil {
		t.Fatal(err)
	}
	if got := atomic.LoadInt32(&p.calls); got != 2 {
		t.Fatalf("expected re-unwrap after TTL, calls=%d", got)
	}
}

func TestCachedProvider_LRUEviction(t *testing.T) {
	p := &countingProvider{dek: testKey()}
	c := NewCachedProvider(p, 2, time.Hour) // cap 2
	ctx := context.Background()

	_, _ = c.UnwrapDEK(ctx, "a")
	_, _ = c.UnwrapDEK(ctx, "b")
	_, _ = c.UnwrapDEK(ctx, "a") // touch a → b is now LRU
	_, _ = c.UnwrapDEK(ctx, "c") // evicts b
	if c.Len() != 2 {
		t.Fatalf("expected len 2, got %d", c.Len())
	}
	_, _ = c.UnwrapDEK(ctx, "b") // miss → re-unwrap
	// a(1) b(1) c(1) b-again(1) = 4
	if got := atomic.LoadInt32(&p.calls); got != 4 {
		t.Fatalf("expected 4 upstream calls, got %d", got)
	}
}

func TestCachedProvider_Singleflight(t *testing.T) {
	p := &countingProvider{dek: testKey(), delay: 30 * time.Millisecond}
	c := NewCachedProvider(p, 10, time.Hour)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.UnwrapDEK(ctx, "u-1"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if got := atomic.LoadInt32(&p.calls); got != 1 {
		t.Fatalf("singleflight failed: expected 1 upstream call, got %d", got)
	}
}

func TestCachedProvider_Evict(t *testing.T) {
	p := &countingProvider{dek: testKey()}
	c := NewCachedProvider(p, 10, time.Hour)
	ctx := context.Background()
	_, _ = c.UnwrapDEK(ctx, "u-1")
	c.Evict("u-1")
	_, _ = c.UnwrapDEK(ctx, "u-1") // miss again
	if got := atomic.LoadInt32(&p.calls); got != 2 {
		t.Fatalf("expected re-unwrap after evict, calls=%d", got)
	}
}

func TestCachedProvider_DistinctUsers(t *testing.T) {
	p := &countingProvider{dek: testKey()}
	c := NewCachedProvider(p, 10, time.Hour)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		_, _ = c.UnwrapDEK(ctx, fmt.Sprintf("u-%d", i))
	}
	if got := atomic.LoadInt32(&p.calls); got != 3 {
		t.Fatalf("expected one unwrap per distinct user, got %d", got)
	}
}
