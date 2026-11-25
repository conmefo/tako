package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestProbe(t *testing.T) {
	tests := []struct {
		name      string
		handler   http.HandlerFunc
		path      string
		wantErr   bool
		wantSize  int64
		wantRange bool
		wantName  string
	}{
		{
			name: "206 with total size",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Range", "bytes 0-0/1000")
				w.Header().Set("ETag", `"v1"`)
				w.WriteHeader(http.StatusPartialContent)
				w.Write([]byte("x"))
			},
			path: "/files/big.iso", wantSize: 1000, wantRange: true, wantName: "big.iso",
		},
		{
			name: "206 with unknown total",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Range", "bytes 0-0/*")
				w.WriteHeader(http.StatusPartialContent)
				w.Write([]byte("x"))
			},
			path: "/a.bin", wantSize: -1, wantRange: false, wantName: "a.bin",
		},
		{
			name: "200 ignores range, has Content-Length",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Length", "5")
				w.Write([]byte("hello"))
			},
			path: "/a.txt", wantSize: 5, wantRange: false, wantName: "a.txt",
		},
		{
			name: "200 chunked, no Content-Length",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte("hello"))
				w.(http.Flusher).Flush()
				w.Write([]byte("world"))
			},
			path: "/stream", wantSize: -1, wantRange: false, wantName: "stream",
		},
		{
			name: "416 empty file",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			},
			path: "/empty", wantSize: 0, wantRange: false, wantName: "empty",
		},
		{
			name: "404 is an error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.NotFound(w, r)
			},
			path: "/missing", wantErr: true,
		},
		{
			name: "Content-Disposition beats URL, path traversal stripped",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Disposition", `attachment; filename="../../.bashrc"`)
				w.Header().Set("Content-Range", "bytes 0-0/10")
				w.WriteHeader(http.StatusPartialContent)
				w.Write([]byte("x"))
			},
			path: "/download?id=7", wantSize: 10, wantRange: true, wantName: ".bashrc",
		},
		{
			name: "Content-Disposition RFC 5987 UTF-8 name",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Disposition", `attachment; filename*=UTF-8''t%C3%A0i%20li%E1%BB%87u.pdf`)
				w.Header().Set("Content-Range", "bytes 0-0/10")
				w.WriteHeader(http.StatusPartialContent)
				w.Write([]byte("x"))
			},
			path: "/x", wantSize: 10, wantRange: true, wantName: "tài liệu.pdf",
		},
		{
			name: "no usable name falls back",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Range", "bytes 0-0/10")
				w.WriteHeader(http.StatusPartialContent)
				w.Write([]byte("x"))
			},
			path: "/", wantSize: 10, wantRange: true, wantName: "download.bin",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotHeaders http.Header
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotHeaders = r.Header.Clone()
				tt.handler(w, r)
			}))
			defer srv.Close()

			info, err := Probe(context.Background(), testClient(srv.Client()), srv.URL+tt.path)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("want error, got %+v", info)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if info.Size != tt.wantSize {
				t.Errorf("Size = %d, want %d", info.Size, tt.wantSize)
			}
			if info.AcceptsRanges != tt.wantRange {
				t.Errorf("AcceptsRanges = %v, want %v", info.AcceptsRanges, tt.wantRange)
			}
			if info.Filename != tt.wantName {
				t.Errorf("Filename = %q, want %q", info.Filename, tt.wantName)
			}
			if got := gotHeaders.Get("Range"); got != "bytes=0-0" {
				t.Errorf("Range header = %q", got)
			}
			if got := gotHeaders.Get("User-Agent"); got != UserAgent {
				t.Errorf("User-Agent = %q", got)
			}
			if got := gotHeaders.Get("Accept-Encoding"); got != "identity" {
				t.Errorf("Accept-Encoding = %q", got)
			}
		})
	}
}

func TestProbeFollowsRedirect(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/releases/latest/app.tar.gz", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/blobs/6460b392?sig=abc", http.StatusFound)
	})
	mux.HandleFunc("/blobs/6460b392", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "bytes=0-0" {
			t.Errorf("Range header lost on redirect: %q", r.Header.Get("Range"))
		}
		w.Header().Set("Content-Range", "bytes 0-0/4096")
		w.WriteHeader(http.StatusPartialContent)
		w.Write([]byte("x"))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	info, err := Probe(context.Background(), testClient(srv.Client()), srv.URL+"/releases/latest/app.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	if want := srv.URL + "/blobs/6460b392?sig=abc"; info.FinalURL != want {
		t.Errorf("FinalURL = %q, want %q", info.FinalURL, want)
	}
	if info.Size != 4096 || !info.AcceptsRanges {
		t.Errorf("Size = %d, AcceptsRanges = %v", info.Size, info.AcceptsRanges)
	}
	if info.Filename != "6460b392" {
		t.Errorf("Filename = %q", info.Filename)
	}
}

func TestParseContentRangeTotal(t *testing.T) {
	for in, want := range map[string]int64{
		"bytes 0-0/104857600": 104857600,
		"bytes 0-0/*":         -1,
		"bytes */1000":        1000,
		"":                    -1,
		"bytes 0-0/abc":       -1,
		"bytes 0-0/-5":        -1,
		"items 0-0/10":        -1,
	} {
		if got := parseContentRangeTotal(in); got != want {
			t.Errorf("parseContentRangeTotal(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestSafeName(t *testing.T) {
	for in, want := range map[string]string{
		"file.iso":         "file.iso",
		"/a/b/file.iso":    "file.iso",
		"../../etc/passwd": "passwd",
		`..\..\evil.exe`:   "evil.exe",
		"..":               "",
		"/":                "",
		"":                 "",
		"dir/":             "dir",
		"bad\x00name":      "",
		"tài liệu.pdf":     "tài liệu.pdf",
	} {
		if got := safeName(in); got != want {
			t.Errorf("safeName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFilenameFromURLFallback(t *testing.T) {
	final, _ := url.Parse("https://cdn.example.com/")
	got := filenameFrom("", final, "https://example.com/dl/report%20final.pdf")
	if got != "report final.pdf" {
		t.Errorf("got %q", got)
	}
	if !strings.Contains(filenameFrom("garbage;;;", final, "https://x/"), "download.bin") {
		t.Errorf("bad Content-Disposition should fall back")
	}
}
