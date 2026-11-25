package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

type sample struct {
	at time.Time
	n  int64
}

func startProgress(done *atomic.Int64, total int64, quiet bool) (stop func()) {
	if quiet {
		return func() {}
	}
	quit := make(chan struct{})
	finished := make(chan struct{})
	var window []sample

	render := func() {
		now := time.Now()
		n := done.Load()
		window = append(window, sample{now, n})
		for len(window) > 2 && now.Sub(window[0].at) > 5*time.Second {
			window = window[1:]
		}
		var rate float64
		if first := window[0]; len(window) > 1 {
			rate = float64(n-first.n) / max(now.Sub(first.at).Seconds(), 1e-9)
		}
		line := human(n)
		if total > 0 {
			line = fmt.Sprintf("%s / %s  %5.1f%%", human(n), human(total), float64(n)*100/float64(total))
		}
		line += fmt.Sprintf("  %7.1f Mbit/s", rate*8/1e6)
		if total > 0 && rate > 0 && n < total {
			line += "  ETA " + eta(float64(total-n)/rate)
		}
		fmt.Fprintf(os.Stderr, "\r\033[K%s", line)
	}

	go func() {
		defer close(finished)
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
		render()
		for {
			select {
			case <-t.C:
				render()
			case <-quit:
				render()
				fmt.Fprintln(os.Stderr)
				return
			}
		}
	}()

	return func() {
		close(quit)
		<-finished
	}
}

func eta(secs float64) string {
	d := time.Duration(secs * float64(time.Second)).Round(time.Second)
	if d >= 100*time.Hour {
		return "--"
	}
	h, m, s := int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60
	if h > 0 {
		return fmt.Sprintf("%dh%02dm", h, m)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
}

func human(n int64) string {
	const unit = 1024
	if n < 0 {
		return "-" + human(-n)
	}
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
