package engine

import (
	"cmp"
	"slices"
	"sync"
	"time"
)

type chunk struct {
	start    int64
	end      int64
	done     int64
	reserved int64
	owned    bool
}

func (c *chunk) len() int64       { return c.end - c.start + 1 }
func (c *chunk) complete() bool   { return c.done >= c.len() }
func (c *chunk) unclaimed() int64 { return c.end - (c.start + c.reserved) + 1 }

type tracker struct {
	mu       sync.Mutex
	chunks   []*chunk
	maxConns int
	limit    int
	running  int
	lastCut  time.Time
	lastGrow time.Time
	changed  chan struct{}
}

func newTracker(chunks []*chunk, maxConns int) *tracker {
	maxConns = max(maxConns, 1)
	return &tracker{chunks: chunks, maxConns: maxConns, limit: maxConns, changed: make(chan struct{}, 1)}
}

func (t *tracker) signal() {
	select {
	case t.changed <- struct{}{}:
	default:
	}
}

func (t *tracker) hasWorkLocked() bool {
	for _, c := range t.chunks {
		if !c.owned && !c.complete() {
			return true
		}
	}
	return t.victimLocked() != nil
}

func (t *tracker) victimLocked() *chunk {
	var v *chunk
	for _, c := range t.chunks {
		if c.owned && (v == nil || c.unclaimed() > v.unclaimed()) {
			v = c
		}
	}
	if v == nil || v.unclaimed() < 2*MinChunk {
		return nil
	}
	return v
}

func (t *tracker) spawn() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running >= t.limit || !t.hasWorkLocked() {
		return false
	}
	t.running++
	return true
}

func (t *tracker) take() *chunk {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running > t.limit {
		t.running--
		t.signal()
		return nil
	}
	for _, c := range t.chunks {
		if !c.owned && !c.complete() {
			c.owned = true
			c.reserved = c.done
			return c
		}
	}
	if v := t.victimLocked(); v != nil {
		mid := v.start + v.reserved + v.unclaimed()/2
		c := &chunk{start: mid, end: v.end, owned: true}
		v.end = mid - 1
		t.chunks = append(t.chunks, c)
		return c
	}
	t.running--
	t.signal()
	return nil
}

func (t *tracker) release(c *chunk) {
	t.mu.Lock()
	c.owned = false
	c.reserved = c.done
	t.mu.Unlock()
	t.signal()
}

func (t *tracker) yield(c *chunk) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running <= t.limit {
		return false
	}
	c.owned = false
	c.reserved = c.done
	t.running--
	t.signal()
	return true
}

func (t *tracker) rewind(c *chunk) (pos, end int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c.reserved = c.done
	return c.start + c.done, c.end
}

func (t *tracker) reserve(c *chunk, n int) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	allowed := max(min(int64(n), c.unclaimed()), 0)
	c.reserved += allowed
	return allowed
}

func (t *tracker) commit(c *chunk, n int64) (complete bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c.done += n
	return c.complete()
}

func (t *tracker) throttled(now time.Time) (limit int, cut bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if now.Sub(t.lastCut) < 2*time.Second || t.limit <= 1 {
		return t.limit, false
	}
	t.lastCut = now
	t.limit = max(1, t.limit/2)
	return t.limit, true
}

func (t *tracker) grow(now time.Time, calm time.Duration) (limit int, grew bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.limit >= t.maxConns || now.Sub(t.lastCut) < calm || now.Sub(t.lastGrow) < calm {
		return t.limit, false
	}
	t.lastGrow = now
	t.limit++
	return t.limit, true
}

func (t *tracker) finished() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.chunks {
		if !c.complete() {
			return false
		}
	}
	return true
}

func (t *tracker) snapshot() []savedChunk {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]savedChunk, len(t.chunks))
	for i, c := range t.chunks {
		out[i] = savedChunk{Start: c.start, End: c.end, Done: c.done}
	}
	slices.SortFunc(out, func(a, b savedChunk) int { return cmp.Compare(a.Start, b.Start) })
	return out
}
