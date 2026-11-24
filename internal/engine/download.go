package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
)

func Download(ctx context.Context, client *http.Client, rawURL, dst string, onProgress func(n int)) error {
	req, err := newRequest(ctx, rawURL)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
	total := resp.ContentLength

	part := dst + ".part"
	f, err := os.Create(part)
	if err != nil {
		return err
	}

	n, copyErr := copyAt(f, 0, resp.Body, onProgress)
	closeErr := f.Close()
	if copyErr != nil {
		return fmt.Errorf("after %d bytes: %w", n, copyErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if total >= 0 && n != total {
		return fmt.Errorf("size mismatch: got %d bytes, expected %d", n, total)
	}
	return os.Rename(part, dst)
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
			return written, rerr
		}
	}
}
