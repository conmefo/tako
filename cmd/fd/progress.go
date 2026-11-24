package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

func startProgress(done *atomic.Int64, total int64) (stop func()) {
	quit := make(chan struct{})
	finished := make(chan struct{})
	start := time.Now()

	render := func() {
		n := done.Load()
		secs := time.Since(start).Seconds()
		mbit := float64(n) * 8 / 1e6 / max(secs, 1e-9)
		if total > 0 {
			fmt.Fprintf(os.Stderr, "\r%s / %s  %5.1f%%  %6.1f Mbit/s ",
				human(n), human(total), float64(n)*100/float64(total), mbit)
		} else {
			fmt.Fprintf(os.Stderr, "\r%s  %6.1f Mbit/s ", human(n), mbit)
		}
	}

	go func() {
		defer close(finished)
		t := time.NewTicker(500 * time.Millisecond)
		defer t.Stop()
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

func human(n int64) string {
	const unit = 1024
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
