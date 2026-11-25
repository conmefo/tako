package engine

import (
	"context"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
)

type FileInfo struct {
	URL           string
	FinalURL      string
	Status        int
	Size          int64
	AcceptsRanges bool
	ETag          string
	LastModified  string
	Filename      string
}

func Probe(ctx context.Context, c *Client, rawURL string) (FileInfo, error) {
	var info FileInfo
	err := c.retry(ctx, "probe", func() (bool, error) {
		var err error
		info, err = probeOnce(ctx, c, rawURL)
		return false, err
	})
	return info, err
}

func probeOnce(ctx context.Context, c *Client, rawURL string) (FileInfo, error) {
	wd := newWatchdog(ctx, c.Retry.IdleTimeout)
	defer wd.stop()

	req, err := newRequest(wd.ctx, rawURL)
	if err != nil {
		return FileInfo{}, err
	}
	req.Header.Set("Range", "bytes=0-0")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return FileInfo{}, wd.wrap(&retryable{err: err})
	}
	defer resp.Body.Close()

	info := FileInfo{
		URL:          rawURL,
		FinalURL:     resp.Request.URL.String(),
		Status:       resp.StatusCode,
		Size:         -1,
		ETag:         resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"),
	}

	switch resp.StatusCode {
	case http.StatusPartialContent:
		info.Size = parseContentRangeTotal(resp.Header.Get("Content-Range"))
		info.AcceptsRanges = info.Size >= 0
	case http.StatusOK:
		info.Size = resp.ContentLength
	case http.StatusRequestedRangeNotSatisfiable:
		info.Size = 0
	default:
		return FileInfo{}, statusError(resp)
	}

	info.Filename = filenameFrom(resp.Header.Get("Content-Disposition"), resp.Request.URL, rawURL)
	return info, nil
}

func parseContentRangeTotal(h string) int64 {
	if !strings.HasPrefix(h, "bytes ") {
		return -1
	}
	i := strings.LastIndexByte(h, '/')
	if i < 0 {
		return -1
	}
	n, err := strconv.ParseInt(h[i+1:], 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

func parseContentRange(h string) (start, end, total int64, ok bool) {
	rest, found := strings.CutPrefix(h, "bytes ")
	if !found {
		return 0, 0, 0, false
	}
	span, size, found := strings.Cut(rest, "/")
	if !found {
		return 0, 0, 0, false
	}
	a, b, found := strings.Cut(span, "-")
	if !found {
		return 0, 0, 0, false
	}
	start, err1 := strconv.ParseInt(a, 10, 64)
	end, err2 := strconv.ParseInt(b, 10, 64)
	if err1 != nil || err2 != nil || start < 0 || end < start {
		return 0, 0, 0, false
	}
	total = -1
	if size != "*" {
		n, err := strconv.ParseInt(size, 10, 64)
		if err != nil || n <= end {
			return 0, 0, 0, false
		}
		total = n
	}
	return start, end, total, true
}

func filenameFrom(contentDisposition string, final *url.URL, original string) string {
	if contentDisposition != "" {
		if _, params, err := mime.ParseMediaType(contentDisposition); err == nil {
			if name := safeName(params["filename"]); name != "" {
				return name
			}
		}
	}
	if final != nil {
		if name := safeName(final.Path); name != "" {
			return name
		}
	}
	if u, err := url.Parse(original); err == nil {
		if name := safeName(u.Path); name != "" {
			return name
		}
	}
	return "download.bin"
}

func safeName(s string) string {
	s = strings.ReplaceAll(s, `\`, "/")
	name := path.Base(s)
	switch name {
	case "", ".", "..", "/":
		return ""
	}
	if strings.ContainsRune(name, 0) {
		return ""
	}
	return name
}
