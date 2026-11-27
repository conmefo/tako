package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/conmefo/tako/internal/engine"
	"golang.org/x/term"
)

var palette = []int{75, 114, 179, 204, 141, 81, 209, 79}

const (
	stDim    = "2"
	stBold   = "1"
	stFaint  = "38;5;239"
	stGreen  = "38;5;114"
	stYellow = "38;5;179"
	stRed    = "38;5;204"
	stCyan   = "38;5;81"
	stPurple = "38;5;141"
	stGray   = "38;5;245"
)

type seg struct {
	text  string
	style string
}

type line []seg

func (l line) width() int {
	n := 0
	for _, s := range l {
		n += len([]rune(s.text))
	}
	return n
}

func (l line) clip(w int) line {
	out := make(line, 0, len(l))
	left := w
	for _, s := range l {
		r := []rune(s.text)
		if len(r) > left {
			r = r[:left]
		}
		if len(r) > 0 {
			out = append(out, seg{string(r), s.style})
		}
		left -= len(r)
	}
	return out
}

type event struct {
	at    time.Duration
	text  string
	style string
}

type workerView struct {
	window []sample
	hist   []float64
	speed  float64
}

type dash struct {
	name  string
	host  string
	live  *engine.Live
	out   *os.File
	color bool
	start time.Time

	mu     sync.Mutex
	events []event

	stats      engine.Stats
	window     []sample
	speed      float64
	hist       []float64
	lastHist   time.Time
	workers    []workerView
	colors     map[int64]int
	prevStarts map[int64]bool
	steals     int
	firstDone  int64
	seen       bool
	peakLimit  int

	paused   bool
	quitting bool
	finished bool
	failed   error
	bigLog   bool
	details  bool
	done     bool

	prevLines int
	prevWidth int
}

func newDash(info engine.FileInfo, dst string, live *engine.Live, out *os.File) *dash {
	host := info.FinalURL
	if u, err := url.Parse(info.FinalURL); err == nil && u.Host != "" {
		host = u.Host
	}
	_, noColor := os.LookupEnv("NO_COLOR")
	return &dash{
		name:   dst,
		host:   host,
		live:   live,
		out:    out,
		color:  !noColor,
		start:  time.Now(),
		colors: map[int64]int{},
	}
}

func (d *dash) logf(format string, args ...any) {
	text := fmt.Sprintf(format, args...)
	style := stCyan
	switch {
	case strings.Contains(text, "rate limiting"):
		style = stYellow
	case strings.Contains(text, "; retry "):
		style = stRed
	case strings.Contains(text, "can't resume"):
		style = stYellow
	}
	d.addEvent(text, style)
}

func (d *dash) addEvent(text, style string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.events = append(d.events, event{time.Since(d.start), strings.ReplaceAll(text, "\n", " "), style})
	if len(d.events) > 200 {
		d.events = d.events[len(d.events)-200:]
	}
}

func rateOver(window *[]sample, now time.Time, n int64, span time.Duration) float64 {
	*window = append(*window, sample{now, n})
	for len(*window) > 2 && now.Sub((*window)[0].at) > span {
		*window = (*window)[1:]
	}
	first := (*window)[0]
	if len(*window) < 2 {
		return 0
	}
	return float64(n-first.n) / max(now.Sub(first.at).Seconds(), 1e-9)
}

func pushHist(h []float64, v float64) []float64 {
	h = append(h, v)
	if len(h) > 400 {
		h = h[len(h)-400:]
	}
	return h
}

func (d *dash) update(now time.Time) {
	s, ok := d.live.Stats()
	if !ok {
		return
	}
	d.stats = s
	d.peakLimit = max(d.peakLimit, s.Limit)
	if !d.seen {
		d.seen = true
		d.firstDone = s.Done
	}
	d.speed = rateOver(&d.window, now, s.Done, 5*time.Second)
	for len(d.workers) < len(s.Workers) {
		d.workers = append(d.workers, workerView{})
	}
	for i, w := range s.Workers {
		d.workers[i].speed = rateOver(&d.workers[i].window, now, w.Bytes, 2*time.Second)
	}
	if now.Sub(d.lastHist) >= 500*time.Millisecond {
		d.lastHist = now
		d.hist = pushHist(d.hist, d.speed)
		for i := range d.workers {
			d.workers[i].hist = pushHist(d.workers[i].hist, d.workers[i].speed)
		}
	}

	starts := make(map[int64]bool, len(s.Chunks))
	for i, c := range s.Chunks {
		starts[c.Start] = true
		if d.prevStarts != nil && !d.prevStarts[c.Start] && c.Worker >= 0 && i > 0 {
			victim := s.Chunks[i-1]
			from := "?"
			if v, ok := d.colors[victim.Start]; ok {
				from = fmt.Sprintf("w%d", v+1)
			}
			d.steals++
			d.addEvent(fmt.Sprintf("w%d took %s from %s", c.Worker+1, human(c.End-c.Start+1), from), stPurple)
		}
		if c.Worker >= 0 {
			d.colors[c.Start] = c.Worker
		}
	}
	d.prevStarts = starts
}

func workerStyle(w int, faint bool) string {
	st := fmt.Sprintf("38;5;%d", palette[w%len(palette)])
	if faint {
		st = "2;" + st
	}
	return st
}

func (d *dash) chunkAt(pos int64) engine.ChunkStat {
	chunks := d.stats.Chunks
	j, _ := slices.BinarySearchFunc(chunks, pos, func(c engine.ChunkStat, t int64) int {
		switch {
		case c.End < t:
			return -1
		case c.Start > t:
			return 1
		}
		return 0
	})
	return chunks[min(j, len(chunks)-1)]
}

func (d *dash) cells(n int, cell func(a, b, mid int64, c engine.ChunkStat) seg) []seg {
	size := d.stats.Size
	out := make([]seg, 0, n)
	if size <= 0 || len(d.stats.Chunks) == 0 {
		return []seg{{strings.Repeat("▔", n), stFaint}}
	}
	for i := 0; i < n; i++ {
		a := size * int64(i) / int64(n)
		b := max(size*int64(i+1)/int64(n), a+1)
		mid := (a + b) / 2
		out = append(out, cell(a, b, mid, d.chunkAt(mid)))
	}
	return out
}

func (d *dash) strip(n int) []seg {
	return d.cells(n, func(a, b, mid int64, c engine.ChunkStat) seg {
		head := c.Start + c.Done
		w, known := d.colors[c.Start]
		switch {
		case mid < head && known:
			return seg{"▀", workerStyle(w, false)}
		case mid < head:
			return seg{"▀", stGray}
		case c.Worker >= 0 && head >= a && head < b:
			return seg{"▘", workerStyle(c.Worker, false)}
		case c.Worker >= 0:
			return seg{"▔", workerStyle(c.Worker, true)}
		}
		return seg{"▔", stFaint}
	})
}

func (d *dash) lane(slot, n int) []seg {
	return d.cells(n, func(a, b, mid int64, c engine.ChunkStat) seg {
		w, known := d.colors[c.Start]
		switch {
		case mid < c.Start+c.Done && known && w == slot:
			return seg{"━", workerStyle(slot, false)}
		case c.Worker == slot && mid >= c.Start+c.Done:
			return seg{"─", workerStyle(slot, true)}
		}
		return seg{"·", stFaint}
	})
}

func thinBar(f float64, w int) []seg {
	n := min(max(int(f*float64(w)+0.5), 0), w)
	return []seg{{strings.Repeat("━", n), stCyan}, {strings.Repeat("─", w-n), stFaint}}
}

func braille(h []float64, w, rows int) []string {
	v := make([]float64, 2*w)
	if len(h) > len(v) {
		h = h[len(h)-len(v):]
	}
	copy(v[len(v)-len(h):], h)
	top := 1.0
	for _, x := range v {
		top = max(top, x*1.05)
	}
	left := []rune{0x40, 0x04, 0x02, 0x01}
	right := []rune{0x80, 0x20, 0x10, 0x08}
	out := make([]string, rows)
	for r := range rows {
		var b strings.Builder
		for c := range w {
			bits := rune(0)
			for k, x := range []float64{v[2*c], v[2*c+1]} {
				tbl := left
				if k == 1 {
					tbl = right
				}
				lvl := int(x/top*float64(rows*4) + 0.5)
				fill := min(max(lvl-(rows-1-r)*4, 0), 4)
				for i := range fill {
					bits |= tbl[i]
				}
			}
			b.WriteRune(0x2800 + bits)
		}
		out[r] = b.String()
	}
	return out
}

func rate(bps float64) string { return human(int64(bps)) + "/s" }

func clock(d time.Duration) string {
	s := int(d.Seconds())
	if s >= 3600 {
		return fmt.Sprintf("%d:%02d:%02d", s/3600, s/60%60, s%60)
	}
	return fmt.Sprintf("%d:%02d", s/60, s%60)
}

func (d *dash) slots() int { return max(len(d.stats.Workers), d.stats.Limit) }

func (d *dash) spare() bool { return d.stats.Running < d.stats.Limit }

func (d *dash) worker(i int) engine.WorkerStat {
	if i < len(d.stats.Workers) {
		return d.stats.Workers[i]
	}
	return engine.WorkerStat{}
}

func (d *dash) dots() []seg {
	var out []seg
	for i := range d.slots() {
		if i > 0 {
			out = append(out, seg{" ", ""})
		}
		w := d.worker(i)
		switch {
		case d.done:
			out = append(out, seg{"●", stGreen})
		case w.Busy && w.State == engine.WorkerWaiting:
			out = append(out, seg{"↻", stRed})
		case w.Busy && w.State == engine.WorkerConnecting:
			out = append(out, seg{"◌", stYellow})
		case w.Busy:
			out = append(out, seg{"●", workerStyle(i, false)})
		case d.spare():
			out = append(out, seg{"○", stDim})
		default:
			out = append(out, seg{"·", stFaint})
		}
	}
	return out
}

func (d *dash) laneTail(i int) seg {
	w := d.worker(i)
	text, style := "", stDim
	switch {
	case d.done:
		text, style = human(w.Bytes), stGreen
	case w.Busy && w.State == engine.WorkerWaiting:
		text, style = fmt.Sprintf("retry %.1fs", max(time.Until(w.Until), 0).Seconds()), stRed
	case w.Busy && w.State == engine.WorkerConnecting:
		text, style = "connecting", stYellow
	case w.Busy && i < len(d.workers):
		text, style = rate(d.workers[i].speed), ""
	case d.spare():
		text = "idle"
	default:
		text, style = "off", stFaint
	}
	return seg{fmt.Sprintf("%11s", text), style}
}

func (d *dash) status(elapsed time.Duration) line {
	s := d.stats
	sep := seg{"  ·  ", stFaint}
	amount := line{{human(s.Done), ""}, {" of " + human(s.Size), stDim}}
	switch {
	case d.done:
		avg := rate(float64(s.Done-d.firstDone) / max(elapsed.Seconds(), 1e-9))
		return line{{"done", stGreen}, sep, {human(s.Size), ""}, {" in ", stDim}, {clock(elapsed), ""}, sep, {"avg ", stDim}, {avg, ""}}
	case d.finished && errors.Is(d.failed, context.Canceled):
		return append(append(line{{"stopped", stYellow}, sep}, amount...), sep, seg{"run the same command to resume", stDim})
	case d.finished:
		return append(append(line{{"failed", stRed}, sep}, amount...), sep, seg{"progress saved", stDim})
	case d.quitting:
		return line{{"saving progress…", stYellow}}
	case d.paused:
		return append(append(line{{"paused", stYellow}, sep}, amount...), sep, seg{"p to resume", stDim})
	}
	left := "--"
	if d.speed > 0 && s.Done < s.Size {
		left = eta(float64(s.Size-s.Done) / d.speed)
	}
	connStyle := ""
	if s.Limit < d.peakLimit {
		connStyle = stYellow
	}
	conns := fmt.Sprintf("%d connection", s.Running)
	if s.Running != 1 {
		conns += "s"
	}
	return append(amount, seg{"   ", ""}, seg{rate(d.speed), stCyan}, seg{"   ", ""}, seg{left, ""}, seg{" left", stDim},
		seg{"   ", ""}, seg{conns, connStyle})
}

func (d *dash) frame(width, height int) []line {
	s := d.stats
	iw := width - 2
	ind := seg{"  ", ""}
	elapsed := time.Since(d.start)
	blank := line{}

	f := 0.0
	if s.Size > 0 {
		f = float64(s.Done) / float64(s.Size)
	}
	pct := fmt.Sprintf("%6s", fmt.Sprintf("%.0f%%", f*100))
	head := []line{
		{ind, {d.name, stBold}, {"  ·  " + d.host, stDim}, {"  ·  " + clock(elapsed), stFaint}},
		blank,
		append(append(line{ind}, thinBar(f, iw-len(pct))...), seg{pct, ""}),
		append(line{ind}, d.status(elapsed)...),
	}

	dots := d.dots()
	dotsW := line(dots).width()
	rightW := max(dotsW+3, 20)
	dotsBelow := iw-rightW < 20
	if dotsBelow {
		rightW = 20
	}
	gw := iw - rightW

	peak := 0.0
	for _, v := range d.hist {
		peak = max(peak, v)
	}
	g := braille(d.hist, gw, 2)
	graph := []line{
		blank,
		{ind, {g[0], stCyan}, {"   peak " + rate(peak), stFaint}},
		{ind, {g[1], stCyan}, {"   speed, last " + clock(time.Duration(gw)*time.Second), stFaint}},
	}

	chunks := []line{blank}
	stripLine := append(line{ind}, d.strip(gw)...)
	if dotsBelow {
		chunks = append(chunks, stripLine, append(line{ind}, dots...))
	} else {
		chunks = append(chunks, append(append(stripLine, seg{"   ", ""}), dots...))
	}

	var lanes []line
	if d.details {
		lanes = append(lanes, blank)
		for i := range d.slots() {
			style := workerStyle(i, false)
			if !d.worker(i).Busy && !d.spare() {
				style = stFaint
			}
			l := append(line{ind}, d.lane(i, gw)...)
			lanes = append(lanes, append(l, seg{fmt.Sprintf("   %2d", i+1), style}, d.laneTail(i)))
		}
	}

	d.mu.Lock()
	evs := slices.Clone(d.events)
	d.mu.Unlock()
	logRows := 1
	if d.bigLog {
		logRows = 8
	}
	if len(evs) > logRows {
		evs = evs[len(evs)-logRows:]
	}
	events := []line{blank}
	for _, e := range evs {
		text := []rune(e.text)
		if len(text) > iw-7 {
			text = append(text[:max(iw-8, 0)], '…')
		}
		events = append(events, line{ind, {fmt.Sprintf("%5s  ", clock(e.at)), stFaint}, {string(text), e.style}})
	}
	if len(evs) == 0 {
		events = nil
	}

	var keys []line
	if !d.finished {
		k := "p pause"
		if d.paused {
			k = "p resume"
		}
		k += "   +/- connections   d details   l log   q quit"
		if d.details {
			k = strings.Replace(k, "d details", "d hide details", 1)
		}
		keys = []line{blank, {ind, {k, stFaint}}}
	}

	budget := height - 1
	total := func() int { return len(head) + len(graph) + len(chunks) + len(lanes) + len(events) + len(keys) }
	for _, drop := range []*[]line{&lanes, &graph, &events} {
		if total() > budget {
			*drop = nil
		}
	}
	out := slices.Concat(head, graph, chunks, lanes, events, keys)
	for i := range out {
		out[i] = out[i].clip(width)
	}
	return out
}

func (d *dash) paint(l line, width int) string {
	var b strings.Builder
	left := width
	for _, s := range l {
		if left <= 0 {
			break
		}
		text := s.text
		if r := []rune(text); len(r) > left {
			text = string(r[:left])
		}
		left -= len([]rune(text))
		if d.color && s.style != "" {
			b.WriteString("\x1b[" + s.style + "m" + text + "\x1b[0m")
		} else {
			b.WriteString(text)
		}
	}
	return b.String()
}

func (d *dash) draw() {
	w, h, err := term.GetSize(int(d.out.Fd()))
	if err != nil || w <= 0 {
		w, h = 80, 24
	}
	width := min(max(w-1, 40), 120)
	var b strings.Builder
	if d.prevWidth != 0 && width != d.prevWidth {
		b.WriteString("\x1b[H\x1b[2J")
		d.prevLines = 0
	}
	d.prevWidth = width
	if d.prevLines > 0 {
		fmt.Fprintf(&b, "\x1b[%dF", d.prevLines)
	}
	lines := d.frame(width, max(h, 12))
	for _, l := range lines {
		b.WriteString(d.paint(l, width))
		b.WriteString("\x1b[K\r\n")
	}
	b.WriteString("\x1b[J")
	d.prevLines = len(lines)
	d.out.WriteString(b.String())
}

func readKeys(f *os.File) <-chan byte {
	ch := make(chan byte, 16)
	go func() {
		buf := make([]byte, 32)
		for {
			n, err := f.Read(buf)
			if err != nil {
				return
			}
			for _, k := range buf[:n] {
				select {
				case ch <- k:
				default:
				}
			}
		}
	}()
	return ch
}

func useDash(plain bool) bool {
	return !plain && term.IsTerminal(int(os.Stderr.Fd())) && os.Getenv("TERM") != "dumb"
}

func runDash(ctx context.Context, client *engine.Client, info engine.FileInfo, ranges []engine.Range, dst string) error {
	live := &engine.Live{}
	d := newDash(info, dst, live, os.Stderr)
	client.Logf = d.logf

	var keys <-chan byte
	restore := func() {}
	if fd := int(os.Stdin.Fd()); term.IsTerminal(fd) {
		if old, err := term.MakeRaw(fd); err == nil {
			restore = func() { term.Restore(fd, old) }
			keys = readKeys(os.Stdin)
		}
	}
	os.Stderr.WriteString("\x1b[?25l")
	defer func() {
		restore()
		os.Stderr.WriteString("\x1b[?25h")
	}()

	var errc chan error
	cancel := context.CancelFunc(func() {})
	run := func() {
		rctx, c := context.WithCancel(ctx)
		cancel = c
		errc = make(chan error, 1)
		d.paused = false
		go func() { errc <- engine.DownloadRangesLive(rctx, client, info, ranges, dst, nil, live) }()
	}
	run()
	defer func() { cancel() }()

	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	sig := ctx.Done()
	pausing := false
	for {
		select {
		case err := <-errc:
			errc = nil
			cancel()
			d.update(time.Now())
			if pausing && !d.quitting && errors.Is(err, context.Canceled) && ctx.Err() == nil {
				pausing = false
				d.paused = true
				d.addEvent("paused; progress saved", stYellow)
				d.draw()
				continue
			}
			d.done = err == nil
			d.finished = true
			d.failed = err
			d.quitting = false
			d.draw()
			return err
		case <-sig:
			sig = nil
			d.quitting = true
			if errc == nil {
				d.finished, d.failed = true, context.Canceled
				d.draw()
				return context.Canceled
			}
			cancel()
		case now := <-tick.C:
			d.update(now)
			d.draw()
		case k := <-keys:
			switch k {
			case 3, 'q', 'Q':
				if d.quitting {
					restore()
					os.Stderr.WriteString("\x1b[?25h\r\n")
					os.Exit(130)
				}
				d.quitting = true
				if errc == nil {
					d.finished, d.failed = true, context.Canceled
					d.quitting = false
					d.draw()
					return context.Canceled
				}
				cancel()
			case 'p', 'P', ' ':
				switch {
				case d.quitting || pausing:
				case errc == nil:
					d.addEvent("resuming", stCyan)
					run()
				default:
					pausing = true
					cancel()
				}
			case '+', '=':
				n := live.SetLimit(d.stats.Limit + 1)
				d.addEvent(fmt.Sprintf("connection limit set to %d", n), stCyan)
			case '-', '_':
				n := live.SetLimit(d.stats.Limit - 1)
				d.addEvent(fmt.Sprintf("connection limit set to %d", n), stCyan)
			case 'l', 'L':
				d.bigLog = !d.bigLog
			case 'd', 'D':
				d.details = !d.details
			}
			d.update(time.Now())
			d.draw()
		}
	}
}
