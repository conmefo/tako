package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func splitN(size int64, n int) []Range {
	var rs []Range
	for i := range int64(n) {
		rs = append(rs, Range{Start: size * i / int64(n), End: size*(i+1)/int64(n) - 1})
	}
	return rs
}

func randomBody(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func TestDownloadRanges(t *testing.T) {
	body := randomBody(t, 3<<20+17)
	want := sha256.Sum256(body)

	for _, n := range []int{1, 2, 4, 7} {
		t.Run("", func(t *testing.T) {
			var conns atomic.Int32
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"v1"`)
				http.ServeContent(w, r, "f.bin", time.Time{}, bytes.NewReader(body))
			}))
			srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
				if s == http.StateNew {
					conns.Add(1)
				}
			}
			srv.Start()
			defer srv.Close()

			info := FileInfo{FinalURL: srv.URL + "/f.bin", Size: int64(len(body)), AcceptsRanges: true, ETag: `"v1"`}
			dst := t.TempDir() + "/f.bin"
			var done atomic.Int64
			err := DownloadRanges(context.Background(), testClient(srv.Client()), info, splitN(info.Size, n), dst,
				func(k int) { done.Add(int64(k)) })
			if err != nil {
				t.Fatal(err)
			}

			got, err := os.ReadFile(dst)
			if err != nil {
				t.Fatal(err)
			}
			if sha256.Sum256(got) != want {
				t.Fatalf("n=%d: SHA-256 mismatch", n)
			}
			if done.Load() != info.Size {
				t.Errorf("n=%d: progress reported %d bytes, want %d", n, done.Load(), info.Size)
			}
			if int(conns.Load()) != n {
				t.Errorf("n=%d: server saw %d connections", n, conns.Load())
			}
			if _, err := os.Stat(dst + ".part"); !os.IsNotExist(err) {
				t.Errorf(".part file left behind")
			}
		})
	}
}

func TestDownloadRangesErrors(t *testing.T) {
	body := randomBody(t, 64<<10)
	size := int64(len(body))

	tests := []struct {
		name    string
		handler http.HandlerFunc
		etag    string
		wantErr string
	}{
		{
			name: "server ignores Range",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Write(body)
			},
			wantErr: "200",
		},
		{
			name: "file changed (If-Range fails)",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"v2"`)
				http.ServeContent(w, r, "f.bin", time.Time{}, bytes.NewReader(body))
			},
			etag:    `"v1"`,
			wantErr: "may have changed",
		},
		{
			name: "wrong Content-Range",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Range", "bytes 0-99/65536")
				w.WriteHeader(http.StatusPartialContent)
				w.Write(body[:100])
			},
			wantErr: "Content-Range",
		},
		{
			name: "short body",
			handler: func(w http.ResponseWriter, r *http.Request) {
				rs := requestedRange(r)
				w.Header().Set("Content-Range", contentRange(rs, size))
				w.WriteHeader(http.StatusPartialContent)
			},
			wantErr: "expected",
		},
		{
			name: "server error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "busy", http.StatusServiceUnavailable)
			},
			wantErr: "503",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			info := FileInfo{FinalURL: srv.URL + "/f.bin", Size: size, AcceptsRanges: true, ETag: tt.etag}
			dst := t.TempDir() + "/f.bin"
			err := DownloadRanges(context.Background(), testClient(srv.Client()), info, splitN(size, 2), dst, nil)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want one containing %q", err, tt.wantErr)
			}
			if _, err := os.Stat(dst); !os.IsNotExist(err) {
				t.Errorf("dst created despite the error")
			}
		})
	}
}

func TestDownloadRangesCancelsOthers(t *testing.T) {
	size := int64(2 << 20)
	var once sync.Once
	slowStarted := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.Header.Get("Range"), "bytes=0-") {
			w.Header().Set("Content-Range", contentRange(Range{0, size/2 - 1}, size))
			w.WriteHeader(http.StatusPartialContent)
			once.Do(func() { close(slowStarted) })
			for {
				if _, err := w.Write([]byte{0}); err != nil {
					return
				}
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
		}
		<-slowStarted
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	info := FileInfo{FinalURL: srv.URL, Size: size, AcceptsRanges: true}
	errc := make(chan error, 1)
	go func() {
		errc <- DownloadRanges(context.Background(), testClient(srv.Client()), info, splitN(size, 2), t.TempDir()+"/f", nil)
	}()
	select {
	case err := <-errc:
		if err == nil || !strings.Contains(err.Error(), "500") {
			t.Fatalf("err = %v, want the 500 from the second range", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow worker was not cancelled")
	}
}

func TestCheckCoverage(t *testing.T) {
	tests := []struct {
		name   string
		ranges []Range
		size   int64
		ok     bool
	}{
		{"exact", []Range{{0, 4}, {5, 9}}, 10, true},
		{"gap", []Range{{0, 3}, {5, 9}}, 10, false},
		{"overlap", []Range{{0, 5}, {5, 9}}, 10, false},
		{"short", []Range{{0, 4}}, 10, false},
		{"too long", []Range{{0, 10}}, 10, false},
		{"empty", nil, 10, false},
	}
	for _, tt := range tests {
		if err := checkCoverage(tt.ranges, tt.size); (err == nil) != tt.ok {
			t.Errorf("%s: err = %v", tt.name, err)
		}
	}
}

func TestParseContentRange(t *testing.T) {
	tests := []struct {
		in                string
		start, end, total int64
		ok                bool
	}{
		{"bytes 0-0/1000", 0, 0, 1000, true},
		{"bytes 100-199/1000", 100, 199, 1000, true},
		{"bytes 0-0/*", 0, 0, -1, true},
		{"bytes 5-4/10", 0, 0, 0, false},
		{"bytes 0-10/10", 0, 0, 0, false},
		{"bytes */1000", 0, 0, 0, false},
		{"items 0-1/2", 0, 0, 0, false},
		{"bytes 0-1", 0, 0, 0, false},
	}
	for _, tt := range tests {
		s, e, n, ok := parseContentRange(tt.in)
		if ok != tt.ok || (ok && (s != tt.start || e != tt.end || n != tt.total)) {
			t.Errorf("%q: got %d %d %d %v", tt.in, s, e, n, ok)
		}
	}
}

func rangeHeader(r Range) string { return fmt.Sprintf("bytes=%d-%d", r.Start, r.End) }

func contentRange(r Range, size int64) string {
	return fmt.Sprintf("bytes %d-%d/%d", r.Start, r.End, size)
}
