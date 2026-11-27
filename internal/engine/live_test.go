package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLiveSetLimitMidDownload(t *testing.T) {
	body := randomBody(t, 4<<20)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slowServe(w, r, body, 32<<10, 5*time.Millisecond)
	}))
	defer srv.Close()

	info := FileInfo{URL: srv.URL, FinalURL: srv.URL, Size: int64(len(body)), AcceptsRanges: true, ETag: `"v1"`}
	dst := t.TempDir() + "/f.bin"
	live := &Live{}
	errc := make(chan error, 1)
	go func() {
		errc <- DownloadRangesLive(context.Background(), testClient(srv.Client()), info, splitN(info.Size, 4), dst, nil, live)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		s, ok := live.Stats()
		if ok && s.Running == 4 && s.Done > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never saw 4 running workers: %+v", s)
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := live.SetLimit(1); got != 1 {
		t.Fatalf("SetLimit(1) = %d", got)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		s, _ := live.Stats()
		if s.Running <= 1 {
			busy := 0
			for _, w := range s.Workers {
				if w.Busy {
					busy++
				}
			}
			if busy > 1 {
				t.Errorf("running %d but %d busy slots", s.Running, busy)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("workers did not give their chunks back: running %d, limit %d", s.Running, s.Limit)
		}
		time.Sleep(5 * time.Millisecond)
	}

	live.SetLimit(4)
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	checkFile(t, dst, body)

	s, _ := live.Stats()
	if s.Done != s.Size || s.Size != info.Size {
		t.Errorf("final stats: done %d size %d, want %d", s.Done, s.Size, info.Size)
	}
	var sum int64
	for _, w := range s.Workers {
		sum += w.Bytes
	}
	if sum != info.Size {
		t.Errorf("worker bytes add up to %d, want %d", sum, info.Size)
	}
}
