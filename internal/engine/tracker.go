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
	owner    int
}

func (c *chunk) len() int64       { return c.end - c.start + 1 }
func (c *chunk) complete() bool   { return c.done >= c.len() }
func (c *chunk) unclaimed() int64 { return c.end - (c.start + c.reserved) + 1 }

type slot struct {
	busy  bool
	state WorkerState
	bytes int64
	until time.Time
	err   string
}

type tracker struct {
	mu        sync.Mutex
	chunks    []*chunk
	slots     []slot
	maxConns  int
	limit     int
	running   int
	retries   int
	throttles int
	lastCut   time.Time
	lastGrow  time.Time
	changed   chan struct{}
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

func (t *tracker) spawn() (id int, ok bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running >= t.limit || !t.hasWorkLocked() {
		return -1, false
	}
	t.running++
	for i := range t.slots {
		if !t.slots[i].busy {
			t.slots[i].busy = true
			return i, true
		}
	}
	t.slots = append(t.slots, slot{busy: true})
	return len(t.slots) - 1, true
}

func (t *tracker) freeLocked(id int) {
	if id >= 0 && id < len(t.slots) {
		t.slots[id] = slot{bytes: t.slots[id].bytes}
	}
}

func (t *tracker) setStateLocked(id int, s WorkerState) {
	if id >= 0 && id < len(t.slots) {
		t.slots[id].state = s
		t.slots[id].until = time.Time{}
	}
}

func (t *tracker) take(id int) *chunk {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running > t.limit {
		t.running--
		t.freeLocked(id)
		t.signal()
		return nil
	}
	for _, c := range t.chunks {
		if !c.owned && !c.complete() {
			c.owned = true
			c.owner = id
			c.reserved = c.done
			t.setStateLocked(id, WorkerConnecting)
			return c
		}
	}
	if v := t.victimLocked(); v != nil {
		mid := v.start + v.reserved + v.unclaimed()/2
		c := &chunk{start: mid, end: v.end, owned: true, owner: id}
		v.end = mid - 1
		t.chunks = append(t.chunks, c)
		t.setStateLocked(id, WorkerConnecting)
		return c
	}
	t.running--
	t.freeLocked(id)
	t.signal()
	return nil
}

func (t *tracker) leave(id int) {
	t.mu.Lock()
	t.freeLocked(id)
	t.mu.Unlock()
}

func (t *tracker) connecting(id int) {
	t.mu.Lock()
	t.setStateLocked(id, WorkerConnecting)
	t.mu.Unlock()
}

func (t *tracker) waiting(id int, until time.Time, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.retries++
	if id >= 0 && id < len(t.slots) {
		t.slots[id].state = WorkerWaiting
		t.slots[id].until = until
		t.slots[id].err = err.Error()
	}
}

func (t *tracker) release(c *chunk) {
	t.mu.Lock()
	c.owned = false
	c.reserved = c.done
	t.mu.Unlock()
	t.signal()
}

func (t *tracker) yield(id int, c *chunk) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.running <= t.limit {
		return false
	}
	c.owned = false
	c.reserved = c.done
	t.running--
	t.freeLocked(id)
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

func (t *tracker) commit(c *chunk, n int64) (complete, over bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c.done += n
	if c.owned && c.owner >= 0 && c.owner < len(t.slots) {
		s := &t.slots[c.owner]
		s.bytes += n
		s.state = WorkerDownloading
		s.err = ""
	}
	return c.complete(), t.running > t.limit
}

func (t *tracker) throttled(now time.Time) (limit int, cut bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if now.Sub(t.lastCut) < 2*time.Second || t.limit <= 1 {
		return t.limit, false
	}
	t.lastCut = now
	t.limit = max(1, t.limit/2)
	t.throttles++
	return t.limit, true
}

func (t *tracker) setLimit(n int) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n = min(max(n, 1), MaxConns)
	t.limit = n
	t.maxConns = n
	t.signal()
	return n
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

func (t *tracker) stats() Stats {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := Stats{
		Limit:     t.limit,
		Running:   t.running,
		Retries:   t.retries,
		Throttles: t.throttles,
		Chunks:    make([]ChunkStat, len(t.chunks)),
		Workers:   make([]WorkerStat, len(t.slots)),
	}
	for i, c := range t.chunks {
		w := -1
		if c.owned {
			w = c.owner
		}
		s.Chunks[i] = ChunkStat{Start: c.start, End: c.end, Done: c.done, Worker: w}
		s.Size += c.len()
		s.Done += c.done
	}
	slices.SortFunc(s.Chunks, func(a, b ChunkStat) int { return cmp.Compare(a.Start, b.Start) })
	for i, sl := range t.slots {
		s.Workers[i] = WorkerStat{Busy: sl.busy, State: sl.state, Bytes: sl.bytes, Until: sl.until, Err: sl.err}
	}
	return s
}
