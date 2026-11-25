package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

var errYield = errors.New("worker gave its chunk back")

const growAfter = 15 * time.Second

type rangeDownload struct {
	c          *Client
	info       FileInfo
	f          *os.File
	t          *tracker
	statePath  string
	onProgress func(int)
}

func DownloadRanges(ctx context.Context, c *Client, info FileInfo, ranges []Range, dst string, onProgress func(n int)) error {
	if err := checkCoverage(ranges, info.Size); err != nil {
		return err
	}

	part, statePath := dst+".part", dst+".part.state"
	f, chunks, err := openPart(c, info, ranges, part, statePath)
	if err != nil {
		return err
	}
	d := &rangeDownload{c: c, info: info, f: f, t: newTracker(chunks, len(ranges)), statePath: statePath, onProgress: onProgress}
	if resumed := sumDone(chunks); resumed > 0 {
		c.logf("resuming: %.1f%% already downloaded", float64(resumed)*100/float64(info.Size))
		if onProgress != nil {
			onProgress(int(resumed))
		}
	}

	err = d.run(ctx)
	if err != nil {
		saveErr := d.save()
		f.Close()
		if saveErr != nil {
			return errors.Join(err, fmt.Errorf("saving progress: %w", saveErr))
		}
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(part, dst); err != nil {
		return err
	}
	os.Remove(statePath)
	return nil
}

func openPart(c *Client, info FileInfo, ranges []Range, part, statePath string) (*os.File, []*chunk, error) {
	if st, err := loadState(statePath); err == nil {
		why := st.mismatch(info)
		if why == "" {
			f, err := os.OpenFile(part, os.O_RDWR, 0)
			if err == nil {
				if fi, err := f.Stat(); err == nil && fi.Size() == info.Size {
					chunks := make([]*chunk, len(st.Chunks))
					for i, sc := range st.Chunks {
						chunks[i] = &chunk{start: sc.Start, end: sc.End, done: sc.Done, reserved: sc.Done}
					}
					return f, chunks, nil
				}
				f.Close()
			}
			why = "the .part file is missing or has the wrong size"
		}
		c.logf("can't resume (%s); starting over", why)
	}
	os.Remove(statePath)

	f, err := os.Create(part)
	if err != nil {
		return nil, nil, err
	}
	if err := f.Truncate(info.Size); err != nil {
		f.Close()
		return nil, nil, err
	}
	chunks := make([]*chunk, len(ranges))
	for i, r := range ranges {
		chunks[i] = &chunk{start: r.Start, end: r.End}
	}
	return f, chunks, nil
}

func sumDone(chunks []*chunk) int64 {
	var n int64
	for _, c := range chunks {
		n += c.done
	}
	return n
}

func (d *rangeDownload) save() error {
	if err := d.f.Sync(); err != nil {
		return err
	}
	return saveState(d.statePath, newState(d.info, d.t.snapshot()))
}

func (d *rangeDownload) run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return d.coordinate(gctx, g) })
	err := g.Wait()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil && !d.t.finished() {
		err = errors.New("workers stopped before the download finished")
	}
	return err
}

func (d *rangeDownload) coordinate(ctx context.Context, g *errgroup.Group) error {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		for d.t.spawn() {
			g.Go(func() error { return d.worker(ctx) })
		}
		if d.t.finished() {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-d.t.changed:
		case now := <-tick.C:
			if limit, grew := d.t.grow(now, growAfter); grew {
				d.c.logf("no rate limiting for a while: going back up to %d connections", limit)
			}
			if err := d.save(); err != nil {
				return fmt.Errorf("saving progress: %w", err)
			}
		}
	}
}

func (d *rangeDownload) worker(ctx context.Context) error {
	buf := make([]byte, 256<<10)
	for {
		ch := d.t.take()
		if ch == nil {
			return nil
		}
		err := d.fetchChunk(ctx, ch, buf)
		if errors.Is(err, errYield) {
			return nil
		}
		d.t.release(ch)
		if err != nil {
			return err
		}
	}
}

func (d *rangeDownload) fetchChunk(ctx context.Context, ch *chunk, buf []byte) error {
	what := fmt.Sprintf("range %d-%d", ch.start, ch.end)
	return d.c.retry(ctx, what, func() (bool, error) {
		n, err := d.fetchOnce(ctx, ch, buf)
		if err != nil && isThrottle(err) {
			if limit, cut := d.t.throttled(time.Now()); cut {
				d.c.logf("server is rate limiting: dropping to %d connections", limit)
			}
			if d.t.yield(ch) {
				return n > 0, errYield
			}
		}
		return n > 0, err
	})
}

func (d *rangeDownload) fetchOnce(ctx context.Context, ch *chunk, buf []byte) (int64, error) {
	pos, end := d.t.rewind(ch)
	if pos > end {
		return 0, nil
	}

	wd := newWatchdog(ctx, d.c.Retry.IdleTimeout)
	defer wd.stop()

	req, err := newRequest(wd.ctx, d.info.FinalURL)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", pos, end))
	if v := ifRangeValidator(d.info); v != "" {
		req.Header.Set("If-Range", v)
	}

	resp, err := d.c.HTTP.Do(req)
	if err != nil {
		return 0, wd.wrap(&retryable{err: err})
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusPartialContent:
	case http.StatusOK:
		return 0, fmt.Errorf("server sent the whole file (200) instead of bytes %d-%d; the file may have changed", pos, end)
	default:
		return 0, statusError(resp)
	}

	cr := resp.Header.Get("Content-Range")
	start, stop, total, ok := parseContentRange(cr)
	if !ok || start != pos || stop != end || total != d.info.Size {
		return 0, fmt.Errorf("asked for bytes %d-%d, server answered with Content-Range %q", pos, end, cr)
	}

	body := wd.reader(resp.Body)
	var got int64
	for {
		nr, rerr := body.Read(buf)
		if nr > 0 {
			allowed := d.t.reserve(ch, nr)
			if allowed > 0 {
				if _, err := d.f.WriteAt(buf[:allowed], pos+got); err != nil {
					return got, err
				}
				got += allowed
				complete := d.t.commit(ch, allowed)
				if d.onProgress != nil {
					d.onProgress(int(allowed))
				}
				if complete {
					return got, nil
				}
			}
			if allowed < int64(nr) {
				return got, nil
			}
		}
		if rerr == io.EOF {
			return got, &retryable{err: fmt.Errorf("got %d bytes, expected %d", got, end-pos+1)}
		}
		if rerr != nil {
			return got, wd.wrap(&retryable{err: rerr})
		}
	}
}

func ifRangeValidator(info FileInfo) string {
	if info.ETag != "" && !strings.HasPrefix(info.ETag, "W/") {
		return info.ETag
	}
	return info.LastModified
}

func checkCoverage(ranges []Range, size int64) error {
	if size <= 0 || len(ranges) == 0 {
		return fmt.Errorf("nothing to split: size %d, %d ranges", size, len(ranges))
	}
	var next int64
	for _, r := range ranges {
		if r.Start != next || r.End < r.Start {
			return fmt.Errorf("bad range %d-%d: expected one starting at %d", r.Start, r.End, next)
		}
		next = r.End + 1
	}
	if next != size {
		return fmt.Errorf("ranges end at %d, file size is %d", next, size)
	}
	return nil
}
