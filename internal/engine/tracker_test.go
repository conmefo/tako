package engine

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func slowServe(w http.ResponseWriter, r *http.Request, body []byte, piece int, pause time.Duration) {
	rs := requestedRange(r)
	if r.Header.Get("Range") == "" {
		rs = Range{0, int64(len(body)) - 1}
	}
	w.Header().Set("ETag", `"v1"`)
	w.Header().Set("Content-Range", contentRange(rs, int64(len(body))))
	w.Header().Set("Content-Length", fmt.Sprint(rs.Len()))
	w.WriteHeader(http.StatusPartialContent)
	for off := rs.Start; off <= rs.End; off += int64(piece) {
		if _, err := w.Write(body[off:min(off+int64(piece), rs.End+1)]); err != nil {
			return
		}
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			return
		case <-time.After(pause):
		}
	}
}

type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (l *logSink) logf(format string, args ...any) {
	l.mu.Lock()
	l.lines = append(l.lines, fmt.Sprintf(format, args...))
	l.mu.Unlock()
}

func (l *logSink) contains(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, x := range l.lines {
		if strings.Contains(x, s) {
			return true
		}
	}
	return false
}

func checkFile(t *testing.T, dst string, body []byte) {
	t.Helper()
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != sha256.Sum256(body) {
		t.Fatal("SHA-256 mismatch")
	}
	for _, leftover := range []string{dst + ".part", dst + ".part.state"} {
		if _, err := os.Stat(leftover); !os.IsNotExist(err) {
			t.Errorf("%s left behind", leftover)
		}
	}
}

func TestResumeAfterCancel(t *testing.T) {
	body := randomBody(t, 4<<20)
	size := int64(len(body))
	var log requestLog
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.add(r)
		slowServe(w, r, body, 32<<10, 5*time.Millisecond)
	}))
	defer srv.Close()

	info := FileInfo{URL: srv.URL, FinalURL: srv.URL, Size: size, AcceptsRanges: true, ETag: `"v1"`}
	dst := t.TempDir() + "/f.bin"
	ranges := splitN(size, 2)

	ctx, cancel := context.WithCancel(context.Background())
	var first atomic.Int64
	err := DownloadRanges(ctx, testClient(srv.Client()), info, ranges, dst, func(n int) {
		if first.Add(int64(n)) > 1<<20 {
			cancel()
		}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	st, err := loadState(dst + ".part.state")
	if err != nil {
		t.Fatalf("no state file after cancel: %v", err)
	}
	saved := st.doneBytes()
	if saved < 1<<20 || saved > first.Load() {
		t.Fatalf("state says %d bytes done, progress saw %d", saved, first.Load())
	}

	log = requestLog{}
	var second atomic.Int64
	c := testClient(srv.Client())
	var logs logSink
	c.Logf = logs.logf
	if err := DownloadRanges(context.Background(), c, info, ranges, dst, func(n int) { second.Add(int64(n)) }); err != nil {
		t.Fatal(err)
	}
	checkFile(t, dst, body)
	if second.Load() != size {
		t.Errorf("progress after resume = %d, want %d", second.Load(), size)
	}
	if !logs.contains("resuming") {
		t.Errorf("logs = %q, want a resume message", logs.lines)
	}
	for _, h := range log.ranges {
		if h == rangeHeader(ranges[0]) || h == rangeHeader(ranges[1]) {
			t.Errorf("second run asked for %s again from the start; requests: %v", h, log.ranges)
		}
	}
}

func TestResumeStartsOverWhenFileChanged(t *testing.T) {
	body := randomBody(t, 2<<20)
	size := int64(len(body))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serve(w, r, body)
	}))
	defer srv.Close()

	dst := t.TempDir() + "/f.bin"
	half := splitN(size, 2)
	stale := newState(FileInfo{URL: srv.URL, Size: size, ETag: `"v0"`}, []savedChunk{
		{Start: half[0].Start, End: half[0].End, Done: 1000},
		{Start: half[1].Start, End: half[1].End, Done: 0},
	})
	if err := saveState(dst+".part.state", stale); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dst+".part", make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}

	c := testClient(srv.Client())
	var logs logSink
	c.Logf = logs.logf
	info := FileInfo{URL: srv.URL, FinalURL: srv.URL, Size: size, AcceptsRanges: true, ETag: `"v1"`}
	if err := DownloadRanges(context.Background(), c, info, half, dst, nil); err != nil {
		t.Fatal(err)
	}
	checkFile(t, dst, body)
	if !logs.contains("changed") || !logs.contains("starting over") {
		t.Errorf("logs = %q", logs.lines)
	}
}

func TestWorkStealing(t *testing.T) {
	body := randomBody(t, 8<<20)
	size := int64(len(body))
	var log requestLog
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.add(r)
		if requestedRange(r).Start == 0 {
			slowServe(w, r, body, 64<<10, 20*time.Millisecond)
			return
		}
		serve(w, r, body)
	}))
	defer srv.Close()

	info := FileInfo{URL: srv.URL, FinalURL: srv.URL, Size: size, AcceptsRanges: true, ETag: `"v1"`}
	dst := t.TempDir() + "/f.bin"
	start := time.Now()
	if err := DownloadRanges(context.Background(), testClient(srv.Client()), info, splitN(size, 2), dst, nil); err != nil {
		t.Fatal(err)
	}
	checkFile(t, dst, body)

	stolen := 0
	for _, h := range log.ranges {
		var s int64
		fmt.Sscanf(h, "bytes=%d-", &s)
		if s > 0 && s < size/2 {
			stolen++
		}
	}
	if stolen == 0 {
		t.Errorf("no request for a stolen piece of the slow first half; requests: %v", log.ranges)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("took %s; the slow half alone takes about 2.6s, stealing should beat that", d)
	}
}

func TestAdaptiveConnections(t *testing.T) {
	body := randomBody(t, 8<<20)
	var inflight, peak atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inflight.Add(1)
		defer inflight.Add(-1)
		if n > 2 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "too many", http.StatusTooManyRequests)
			return
		}
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		slowServe(w, r, body, 256<<10, 5*time.Millisecond)
	}))
	defer srv.Close()

	c := testClient(srv.Client())
	var logs logSink
	c.Logf = logs.logf
	info := FileInfo{URL: srv.URL, FinalURL: srv.URL, Size: int64(len(body)), AcceptsRanges: true, ETag: `"v1"`}
	dst := t.TempDir() + "/f.bin"
	if err := DownloadRanges(context.Background(), c, info, splitN(info.Size, 4), dst, nil); err != nil {
		t.Fatalf("%v; logs: %q", err, logs.lines)
	}
	checkFile(t, dst, body)
	if !logs.contains("dropping to 2 connections") {
		t.Errorf("logs = %q, want the connection count cut to 2", logs.lines)
	}
}

func TestTrackerSplitsLargestChunk(t *testing.T) {
	tr := newTracker([]*chunk{{start: 0, end: 8*MinChunk - 1}, {start: 8 * MinChunk, end: 9*MinChunk - 1}}, 2)
	tr.running = 2
	a, b := tr.take(0), tr.take(1)
	tr.commit(b, b.len())
	tr.release(b)

	s := tr.take(1)
	if s == nil || s.start != 4*MinChunk || s.end != 8*MinChunk-1 || a.end != 4*MinChunk-1 {
		t.Fatalf("split went wrong: a=%+v stolen=%+v", a, s)
	}
	if got := tr.reserve(a, int(5*MinChunk)); got != 4*MinChunk {
		t.Errorf("victim may still write %d bytes, want %d", got, 4*MinChunk)
	}
	tr.reserve(s, int(3*MinChunk))
	if x := tr.take(2); x != nil {
		t.Errorf("split off %+v though nothing has 2 × MinChunk unclaimed", x)
	}
}
