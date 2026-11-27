package engine

import (
	"sync/atomic"
	"time"
)

const MaxConns = 32

type WorkerState int

const (
	WorkerIdle WorkerState = iota
	WorkerConnecting
	WorkerDownloading
	WorkerWaiting
)

type Stats struct {
	Size      int64
	Done      int64
	Limit     int
	Running   int
	Retries   int
	Throttles int
	Chunks    []ChunkStat
	Workers   []WorkerStat
}

type ChunkStat struct {
	Start, End, Done int64
	Worker           int
}

type WorkerStat struct {
	Busy  bool
	State WorkerState
	Bytes int64
	Until time.Time
	Err   string
}

type Live struct {
	t     atomic.Pointer[tracker]
	limit atomic.Int64
}

func (l *Live) Stats() (Stats, bool) {
	t := l.t.Load()
	if t == nil {
		return Stats{}, false
	}
	return t.stats(), true
}

func (l *Live) SetLimit(n int) int {
	n = min(max(n, 1), MaxConns)
	l.limit.Store(int64(n))
	if t := l.t.Load(); t != nil {
		return t.setLimit(n)
	}
	return n
}

func (l *Live) attach(t *tracker) {
	if n := l.limit.Load(); n > 0 {
		t.setLimit(int(n))
	}
	l.t.Store(t)
}
