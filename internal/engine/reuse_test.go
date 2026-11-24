package engine

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"testing"
	"time"
)

func TestProbeConnectionReused(t *testing.T) {
	body := make([]byte, 4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "f.bin", time.Time{}, bytes.NewReader(body))
	}))
	defer srv.Close()

	var reused []bool
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(i httptrace.GotConnInfo) { reused = append(reused, i.Reused) },
	})
	client := srv.Client()
	info, err := Probe(ctx, client, srv.URL+"/f.bin")
	if err != nil {
		t.Fatal(err)
	}
	if err := Download(ctx, client, info.FinalURL, t.TempDir()+"/f.bin", nil); err != nil {
		t.Fatal(err)
	}
	if len(reused) != 2 || !reused[1] {
		t.Fatalf("download did not reuse the probe's connection: reused = %v", reused)
	}
}
