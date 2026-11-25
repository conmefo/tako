package engine

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"golang.org/x/sync/errgroup"
)

func DownloadRanges(ctx context.Context, c *Client, info FileInfo, ranges []Range, dst string, onProgress func(n int)) error {
	if err := checkCoverage(ranges, info.Size); err != nil {
		return err
	}

	part := dst + ".part"
	f, err := os.Create(part)
	if err != nil {
		return err
	}
	if err := f.Truncate(info.Size); err != nil {
		f.Close()
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, r := range ranges {
		g.Go(func() error {
			return fetchRange(gctx, c, info, r, f, onProgress)
		})
	}
	err = g.Wait()
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(part, dst)
}

func fetchRange(ctx context.Context, c *Client, info FileInfo, r Range, f io.WriterAt, onProgress func(int)) error {
	var got int64
	return c.retry(ctx, fmt.Sprintf("range %d-%d", r.Start, r.End), func() (bool, error) {
		n, err := fetchRangeOnce(ctx, c, info, Range{Start: r.Start + got, End: r.End}, f, onProgress)
		got += n
		return n > 0, err
	})
}

func fetchRangeOnce(ctx context.Context, c *Client, info FileInfo, r Range, f io.WriterAt, onProgress func(int)) (int64, error) {
	wd := newWatchdog(ctx, c.Retry.IdleTimeout)
	defer wd.stop()

	req, err := newRequest(wd.ctx, info.FinalURL)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", r.Start, r.End))
	if v := ifRangeValidator(info); v != "" {
		req.Header.Set("If-Range", v)
	}

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, wd.wrap(&retryable{err: err})
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusPartialContent:
	case http.StatusOK:
		return 0, fmt.Errorf("server sent the whole file (200) instead of bytes %d-%d; the file may have changed", r.Start, r.End)
	default:
		return 0, statusError(resp)
	}

	cr := resp.Header.Get("Content-Range")
	start, end, total, ok := parseContentRange(cr)
	if !ok || start != r.Start || end != r.End || total != info.Size {
		return 0, fmt.Errorf("asked for bytes %d-%d, server answered with Content-Range %q", r.Start, r.End, cr)
	}

	n, err := copyAt(f, r.Start, io.LimitReader(wd.reader(resp.Body), r.Len()), onProgress)
	if err != nil {
		return n, wd.wrap(err)
	}
	if n != r.Len() {
		return n, &retryable{err: fmt.Errorf("got %d bytes, expected %d", n, r.Len())}
	}
	return n, nil
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
