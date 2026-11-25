package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewClientOneConnectionPerRange(t *testing.T) {
	const n = 8
	body := randomBody(t, 1<<20)

	var (
		mu      sync.Mutex
		addrs   = map[string]bool{}
		protos  = map[string]bool{}
		arrived int
		allIn   = make(chan struct{})
		dials   atomic.Int32
	)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		addrs[r.RemoteAddr] = true
		protos[r.Proto] = true
		arrived++
		if arrived == n {
			close(allIn)
		}
		mu.Unlock()
		select {
		case <-allIn:
		case <-time.After(5 * time.Second):
		}
		http.ServeContent(w, r, "f.bin", time.Time{}, bytes.NewReader(body))
	}))
	srv.EnableHTTP2 = true
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			dials.Add(1)
		}
	}
	srv.StartTLS()
	defer srv.Close()

	client := NewClient(n)
	client.HTTP.Transport.(*http.Transport).TLSClientConfig.RootCAs = srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs

	info := FileInfo{FinalURL: srv.URL + "/f.bin", Size: int64(len(body)), AcceptsRanges: true}
	dst := t.TempDir() + "/f.bin"
	if err := DownloadRanges(context.Background(), client, info, splitN(info.Size, n), dst, nil); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(got) != sha256.Sum256(body) {
		t.Fatal("SHA-256 mismatch")
	}

	mu.Lock()
	if !protos["HTTP/1.1"] || len(protos) != 1 {
		t.Errorf("protocols used = %v, want only HTTP/1.1", protos)
	}
	if len(addrs) != n {
		t.Errorf("%d ranges were carried by %d TCP connections, want %d", n, len(addrs), n)
	}
	mu.Unlock()

	firstRound := dials.Load()
	for range n {
		req, err := newRequest(context.Background(), info.FinalURL)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Range", "bytes=0-0")
		go func() {
			resp, err := client.HTTP.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		done := arrived >= 2*n
		mu.Unlock()
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if extra := dials.Load() - firstRound; extra != 0 {
		t.Errorf("second round of %d requests opened %d new connections, want 0 (idle pool too small?)", n, extra)
	}
}
