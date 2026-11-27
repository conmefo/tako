package main

import (
	"strings"
	"testing"
	"time"

	"github.com/conmefo/tako/internal/engine"
)

func testDash(paused, done bool) *dash {
	d := &dash{name: "file.iso", host: "example.com", start: time.Now().Add(-90 * time.Second), colors: map[int64]int{0: 0, 600: 1}}
	d.stats = engine.Stats{
		Size: 1000, Done: 450, Limit: 3, Running: 2, Retries: 1, Throttles: 1,
		Chunks: []engine.ChunkStat{
			{Start: 0, End: 599, Done: 300, Worker: 0},
			{Start: 600, End: 999, Done: 150, Worker: -1},
		},
		Workers: []engine.WorkerStat{
			{Busy: true, State: engine.WorkerDownloading, Bytes: 300},
			{Busy: true, State: engine.WorkerWaiting, Until: time.Now().Add(time.Second), Err: "boom"},
			{},
			{},
		},
	}
	d.workers = make([]workerView, 4)
	d.hist = []float64{1, 5, 3, 8}
	d.speed = 8
	d.paused, d.done, d.finished = paused, done, done
	for i := range 30 {
		d.addEvent(strings.Repeat("x", i*5), stCyan)
	}
	return d
}

func TestDashFrameWidths(t *testing.T) {
	for _, size := range [][2]int{{40, 12}, {79, 24}, {120, 50}} {
		for _, state := range [][2]bool{{false, false}, {true, false}, {false, true}} {
			for _, details := range []bool{false, true} {
				d := testDash(state[0], state[1])
				d.details = details
				d.bigLog = details
				lines := d.frame(size[0], size[1])
				if len(lines) > size[1]-1 {
					t.Errorf("%v %v: %d lines, terminal has %d rows", size, state, len(lines), size[1])
				}
				for i, l := range lines {
					if w := len([]rune(d.paint(l, 1000))); w > size[0] {
						t.Errorf("%v %v line %d is %d wide, max %d: %q", size, state, i, w, size[0], d.paint(l, 1000))
					}
				}
			}
		}
	}
}

func TestDashStripShowsProgress(t *testing.T) {
	d := testDash(false, false)
	got := ""
	for _, c := range d.strip(10) {
		got += c.text
	}
	if got != "▀▀▀▘▔▔▀▔▔▔" {
		t.Errorf("strip = %q", got)
	}
}

func TestDashLaneShowsOwnWork(t *testing.T) {
	d := testDash(false, false)
	got := ""
	for _, c := range d.lane(0, 10) {
		got += c.text
	}
	if got != "━━━───····" {
		t.Errorf("lane = %q", got)
	}
}
