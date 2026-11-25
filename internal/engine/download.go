package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
)

func Download(ctx context.Context, c *Client, rawURL, dst string, onProgress func(n int)) error {
	part := dst + ".part"
	f, err := os.Create(part)
	if err != nil {
		return err
	}

	var got int64
	err = c.retry(ctx, "download", func() (bool, error) {
		if got > 0 {
			if onProgress != nil {
				onProgress(-int(got))
			}
			got = 0
			if err := f.Truncate(0); err != nil {
				return false, err
			}
		}
		n, err := downloadOnce(ctx, c, rawURL, f, onProgress)
		got = n
		return n > 0, err
	})
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(part)
		return err
	}
	return os.Rename(part, dst)
}

func downloadOnce(ctx context.Context, c *Client, rawURL string, f io.WriterAt, onProgress func(int)) (int64, error) {
	wd := newWatchdog(ctx, c.Retry.IdleTimeout)
	defer wd.stop()

	req, err := newRequest(wd.ctx, rawURL)
	if err != nil {
		return 0, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, wd.wrap(&retryable{err: err})
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, statusError(resp)
	}

	n, err := copyAt(f, 0, wd.reader(resp.Body), onProgress)
	if err != nil {
		return n, wd.wrap(fmt.Errorf("after %d bytes: %w", n, err))
	}
	if resp.ContentLength >= 0 && n != resp.ContentLength {
		return n, &retryable{err: fmt.Errorf("size mismatch: got %d bytes, expected %d", n, resp.ContentLength)}
	}
	return n, nil
}

func copyAt(f io.WriterAt, off int64, r io.Reader, onProgress func(int)) (int64, error) {
	buf := make([]byte, 256<<10)
	var written int64
	for {
		nr, rerr := r.Read(buf)
		if nr > 0 {
			nw, werr := f.WriteAt(buf[:nr], off+written)
			written += int64(nw)
			if onProgress != nil && nw > 0 {
				onProgress(nw)
			}
			if werr != nil {
				return written, werr
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return written, nil
			}
			return written, &retryable{err: rerr}
		}
	}
}
