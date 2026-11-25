package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	rate      int64
	drop      float64
	stall     float64
	throttle  float64
	maxConns  int
	noRanges  bool
	etag      string
	body      []byte
	requestID atomic.Int64
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "listen address")
	size := flag.String("size", "64MiB", "size of the generated file (ignored with -file)")
	file := flag.String("file", "", "serve this file instead of generated data")
	seed := flag.Uint64("seed", 1, "seed for the generated data")
	rate := flag.String("rate", "0", "per-connection speed limit, e.g. 2MiB (0 = unlimited)")
	drop := flag.Float64("drop", 0, "chance (0-1) that a response is cut off partway")
	stall := flag.Float64("stall", 0, "chance (0-1) that a response stops sending partway and hangs")
	throttle := flag.Float64("429", 0, "chance (0-1) that a request gets 429 with Retry-After: 1")
	maxConns := flag.Int("max-conns", 0, "answer 429 when one IP has more than this many requests in flight (0 = no limit)")
	noRanges := flag.Bool("no-ranges", false, "ignore Range headers and always send the whole file")
	quiet := flag.Bool("q", false, "don't log each request")
	flag.Parse()

	cfg := &config{drop: *drop, stall: *stall, throttle: *throttle, maxConns: *maxConns, noRanges: *noRanges}
	var err error
	if cfg.rate, err = parseSize(*rate); err != nil {
		log.Fatalf("-rate: %v", err)
	}
	if *file != "" {
		if cfg.body, err = os.ReadFile(*file); err != nil {
			log.Fatal(err)
		}
	} else {
		n, err := parseSize(*size)
		if err != nil {
			log.Fatalf("-size: %v", err)
		}
		cfg.body = make([]byte, n)
		r := rand.NewChaCha8([32]byte{byte(*seed), byte(*seed >> 8), byte(*seed >> 16), byte(*seed >> 24)})
		r.Read(cfg.body)
	}
	sum := sha256.Sum256(cfg.body)
	hexSum := hex.EncodeToString(sum[:])
	cfg.etag = `"` + hexSum[:16] + `"`

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Fprintf(os.Stderr, "serving %d bytes at http://%s/file.bin\nsha256 %s\n", len(cfg.body), ln.Addr(), hexSum)

	h := cfg.handler(*quiet)
	log.Fatal(http.Serve(ln, h))
}

func (cfg *config) handler(quiet bool) http.Handler {
	var mu sync.Mutex
	inflight := map[string]int{}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := cfg.requestID.Add(1)
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)
		mu.Lock()
		inflight[ip]++
		n := inflight[ip]
		mu.Unlock()
		defer func() {
			mu.Lock()
			inflight[ip]--
			mu.Unlock()
		}()

		logf := func(format string, args ...any) {
			if !quiet {
				log.Printf("#%d %s %s: "+format, append([]any{id, ip, r.Header.Get("Range")}, args...)...)
			}
		}

		if cfg.maxConns > 0 && n > cfg.maxConns {
			logf("429 (%d in flight, max %d)", n, cfg.maxConns)
			w.Header().Set("Retry-After", "1")
			http.Error(w, "too many connections", http.StatusTooManyRequests)
			return
		}
		if rand.Float64() < cfg.throttle {
			logf("429 (random)")
			w.Header().Set("Retry-After", "1")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}

		fw := &faultWriter{ResponseWriter: w, r: r, rate: cfg.rate, cutAt: -1, stallAt: -1, start: time.Now()}
		switch x := rand.Float64(); {
		case x < cfg.drop:
			fw.fault = "drop"
			logf("will drop partway")
		case x < cfg.drop+cfg.stall:
			fw.fault = "stall"
			logf("will stall partway")
		default:
			logf("ok")
		}

		w.Header().Set("ETag", cfg.etag)
		if cfg.noRanges {
			w.Header().Set("Content-Length", strconv.Itoa(len(cfg.body)))
			fw.Write(cfg.body)
			return
		}
		http.ServeContent(fw, r, "file.bin", time.Time{}, bytes.NewReader(cfg.body))
	})
}

type faultWriter struct {
	http.ResponseWriter
	r       *http.Request
	rate    int64
	cutAt   int64
	stallAt int64
	written int64
	start   time.Time
	fault   string
	armed   bool
}

func (fw *faultWriter) arm() {
	fw.armed = true
	if fw.fault == "" {
		return
	}
	length, err := strconv.ParseInt(fw.Header().Get("Content-Length"), 10, 64)
	if err != nil || length <= 0 {
		return
	}
	at := rand.Int64N(length)
	if fw.fault == "drop" {
		fw.cutAt = at
	} else {
		fw.stallAt = at
	}
}

func (fw *faultWriter) Write(p []byte) (int, error) {
	if !fw.armed {
		fw.arm()
	}
	total := 0
	for len(p) > 0 {
		n := min(len(p), 32<<10)
		if fw.cutAt >= 0 && fw.written+int64(n) > fw.cutAt {
			k := int(fw.cutAt - fw.written)
			fw.ResponseWriter.Write(p[:k])
			fw.ResponseWriter.(http.Flusher).Flush()
			panic(http.ErrAbortHandler)
		}
		if fw.stallAt >= 0 && fw.written+int64(n) > fw.stallAt {
			k := int(fw.stallAt - fw.written)
			fw.ResponseWriter.Write(p[:k])
			fw.ResponseWriter.(http.Flusher).Flush()
			<-fw.r.Context().Done()
			return total + k, fw.r.Context().Err()
		}
		m, err := fw.ResponseWriter.Write(p[:n])
		total += m
		fw.written += int64(m)
		if err != nil {
			return total, err
		}
		p = p[n:]
		if fw.rate > 0 {
			due := fw.start.Add(time.Duration(float64(fw.written) / float64(fw.rate) * float64(time.Second)))
			time.Sleep(time.Until(due))
		}
	}
	return total, nil
}

func (fw *faultWriter) Flush() {
	fw.ResponseWriter.(http.Flusher).Flush()
}

func parseSize(s string) (int64, error) {
	units := []struct {
		suffix string
		mult   int64
	}{{"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}, {"GB", 1e9}, {"MB", 1e6}, {"KB", 1e3}, {"B", 1}}
	for _, u := range units {
		if num, ok := strings.CutSuffix(s, u.suffix); ok {
			f, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
			if err != nil || f < 0 {
				return 0, fmt.Errorf("bad size %q", s)
			}
			return int64(f * float64(u.mult)), nil
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("bad size %q", s)
	}
	return n, nil
}
