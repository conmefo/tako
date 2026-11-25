package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

var testRetry = RetryPolicy{
	Attempts:      3,
	BaseDelay:     time.Millisecond,
	MaxDelay:      5 * time.Millisecond,
	MaxRetryAfter: 5 * time.Second,
	IdleTimeout:   5 * time.Second,
}

func testClient(hc *http.Client) *Client {
	return &Client{HTTP: hc, Retry: testRetry}
}

func requestedRange(r *http.Request) Range {
	var rs Range
	fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &rs.Start, &rs.End)
	return rs
}

type requestLog struct {
	mu     sync.Mutex
	ranges []string
}

func (l *requestLog) add(r *http.Request) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ranges = append(l.ranges, r.Header.Get("Range"))
	n := 0
	for _, h := range l.ranges {
		if h == r.Header.Get("Range") {
			n++
		}
	}
	return n
}

func (l *requestLog) has(h string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, x := range l.ranges {
		if x == h {
			return true
		}
	}
	return false
}

func (l *requestLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.ranges)
}

func downloadAndCheck(t *testing.T, c *Client, srv *httptest.Server, body []byte, n int) {
	t.Helper()
	info := FileInfo{FinalURL: srv.URL + "/f.bin", Size: int64(len(body)), AcceptsRanges: true, ETag: `"v1"`}
	dst := t.TempDir() + "/f.bin"
	if err := DownloadRanges(context.Background(), c, info, splitN(info.Size, n), dst, nil); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != sha256.Sum256(body) {
		t.Fatal("SHA-256 mismatch")
	}
}

func serve(w http.ResponseWriter, r *http.Request, body []byte) {
	w.Header().Set("ETag", `"v1"`)
	http.ServeContent(w, r, "f.bin", time.Time{}, bytes.NewReader(body))
}

func TestRetryOn429(t *testing.T) {
	body := randomBody(t, 2<<20)
	var log requestLog
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if log.add(r) == 1 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		serve(w, r, body)
	}))
	defer srv.Close()

	downloadAndCheck(t, testClient(srv.Client()), srv, body, 4)
	if got := log.count(); got != 8 {
		t.Errorf("server saw %d requests, want 8 (4 rejected + 4 retries)", got)
	}
}

func TestResumeAfterDroppedConnection(t *testing.T) {
	body := randomBody(t, 2<<20)
	size := int64(len(body))
	var log requestLog
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rs := requestedRange(r)
		if log.add(r) == 1 && rs.Start == 0 {
			w.Header().Set("Content-Range", contentRange(rs, size))
			w.Header().Set("Content-Length", fmt.Sprint(rs.Len()))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(body[:rs.Len()/2])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}
		serve(w, r, body)
	}))
	defer srv.Close()

	downloadAndCheck(t, testClient(srv.Client()), srv, body, 2)
	first := splitN(size, 2)[0]
	resumed := rangeHeader(Range{Start: first.Len() / 2, End: first.End})
	if !log.has(resumed) {
		t.Errorf("no resumed request %q; saw %v", resumed, log.ranges)
	}
}

func TestIdleTimeoutRetries(t *testing.T) {
	body := randomBody(t, 2<<20)
	size := int64(len(body))
	var log requestLog
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rs := requestedRange(r)
		if log.add(r) == 1 && rs.Start == 0 {
			w.Header().Set("Content-Range", contentRange(rs, size))
			w.Header().Set("Content-Length", fmt.Sprint(rs.Len()))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(body[:1000])
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
			return
		}
		serve(w, r, body)
	}))
	defer srv.Close()

	c := testClient(srv.Client())
	c.Retry.IdleTimeout = 100 * time.Millisecond
	var mu sync.Mutex
	var logs []string
	c.Logf = func(format string, args ...any) {
		mu.Lock()
		logs = append(logs, fmt.Sprintf(format, args...))
		mu.Unlock()
	}
	start := time.Now()
	downloadAndCheck(t, c, srv, body, 2)
	if d := time.Since(start); d > 3*time.Second {
		t.Errorf("took %s, the stalled range was not cut off", d)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(logs) != 1 || !strings.Contains(logs[0], "no data received") {
		t.Errorf("logs = %q, want one idle-timeout retry", logs)
	}
	if !log.has(rangeHeader(Range{Start: 1000, End: splitN(size, 2)[0].End})) {
		t.Errorf("did not resume from byte 1000; saw %v", log.ranges)
	}
}

func TestNoRetryOnFatalErrors(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusRequestedRangeNotSatisfiable} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var log requestLog
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				log.add(r)
				w.WriteHeader(code)
			}))
			defer srv.Close()

			info := FileInfo{FinalURL: srv.URL, Size: 4 << 20, AcceptsRanges: true}
			err := DownloadRanges(context.Background(), testClient(srv.Client()), info, splitN(info.Size, 1), t.TempDir()+"/f", nil)
			if err == nil || !strings.Contains(err.Error(), fmt.Sprint(code)) {
				t.Fatalf("err = %v", err)
			}
			if log.count() != 1 {
				t.Errorf("server saw %d requests, want 1 (no retry)", log.count())
			}
		})
	}
}

func TestGiveUpAfterAttempts(t *testing.T) {
	var log requestLog
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.add(r)
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	_, err := Probe(context.Background(), testClient(srv.Client()), srv.URL)
	if err == nil || !strings.Contains(err.Error(), "gave up after 3") {
		t.Fatalf("err = %v", err)
	}
	if log.count() != testRetry.Attempts {
		t.Errorf("server saw %d requests, want %d", log.count(), testRetry.Attempts)
	}
}

func TestRetryAfterTooLong(t *testing.T) {
	var log requestLog
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.add(r)
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	_, err := Probe(context.Background(), testClient(srv.Client()), srv.URL)
	if err == nil || !strings.Contains(err.Error(), "asked to wait 1h0m0s") {
		t.Fatalf("err = %v", err)
	}
	if log.count() != 1 {
		t.Errorf("server saw %d requests, want 1", log.count())
	}
}

func TestCancelDuringBackoff(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "busy", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := testClient(srv.Client())
	c.Retry.BaseDelay = time.Minute
	c.Retry.MaxDelay = time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := Probe(ctx, c, srv.URL)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the context error", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("cancel took %s", d)
	}
}

func TestDownloadRestartsWithoutRanges(t *testing.T) {
	body := randomBody(t, 1<<20)
	var log requestLog
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if log.add(r) == 1 {
			w.Header().Set("Content-Length", fmt.Sprint(len(body)))
			w.Write(body[:len(body)/3])
			w.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}
		w.Write(body)
	}))
	defer srv.Close()

	dst := t.TempDir() + "/f.bin"
	var progress int64
	err := Download(context.Background(), testClient(srv.Client()), srv.URL, dst, func(n int) { progress += int64(n) })
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(dst)
	if !bytes.Equal(got, body) {
		t.Fatal("content mismatch")
	}
	if progress != int64(len(body)) {
		t.Errorf("progress = %d, want %d", progress, len(body))
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"0", 0},
		{"120", 2 * time.Minute},
		{"-5", 0},
		{"Wed, 25 Nov 2026 10:00:30 GMT", 30 * time.Second},
		{"Wed, 25 Nov 2026 09:00:00 GMT", 0},
		{"soon", 0},
	}
	for _, tt := range tests {
		if got := parseRetryAfter(tt.in, now); got != tt.want {
			t.Errorf("%q: got %s, want %s", tt.in, got, tt.want)
		}
	}
}

func TestBackoffDelay(t *testing.T) {
	p := RetryPolicy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}
	for failures, ceiling := range []time.Duration{100, 200, 400, 800, 1000, 1000} {
		ceiling *= time.Millisecond
		for range 200 {
			if d := p.delay(failures, 0); d < 0 || d >= ceiling {
				t.Fatalf("failures=%d: delay %s outside [0, %s)", failures, d, ceiling)
			}
		}
	}
	if d := p.delay(0, 2*time.Second); d < 2*time.Second || d > 2200*time.Millisecond {
		t.Errorf("Retry-After 2s gave %s", d)
	}
}
