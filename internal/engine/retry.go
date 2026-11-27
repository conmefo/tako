package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

type RetryPolicy struct {
	Attempts      int
	BaseDelay     time.Duration
	MaxDelay      time.Duration
	MaxRetryAfter time.Duration
	IdleTimeout   time.Duration
}

var DefaultRetry = RetryPolicy{
	Attempts:      6,
	BaseDelay:     500 * time.Millisecond,
	MaxDelay:      30 * time.Second,
	MaxRetryAfter: 2 * time.Minute,
	IdleTimeout:   30 * time.Second,
}

type retryable struct {
	err   error
	after time.Duration
	code  int
}

func (e *retryable) Error() string { return e.err.Error() }
func (e *retryable) Unwrap() error { return e.err }

func (p RetryPolicy) delay(failures int, after time.Duration) time.Duration {
	if after > 0 {
		return after + rand.N(after/10+time.Millisecond)
	}
	b := p.MaxDelay
	if failures < 30 {
		b = min(b, p.BaseDelay<<failures)
	}
	if b <= 0 {
		return 0
	}
	return rand.N(b)
}

func (c *Client) retry(ctx context.Context, what string, attempt func() (progressed bool, err error)) error {
	return c.retryWith(ctx, what, nil, attempt)
}

func (c *Client) retryWith(ctx context.Context, what string, onWait func(time.Duration, error), attempt func() (progressed bool, err error)) error {
	p := c.Retry
	limit := max(p.Attempts, 1)
	failures := 0
	for {
		progressed, err := attempt()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%s: %w", what, ctx.Err())
		}
		var re *retryable
		if !errors.As(err, &re) {
			return fmt.Errorf("%s: %w", what, err)
		}
		if progressed {
			failures = 0
		}
		failures++
		if failures >= limit {
			return fmt.Errorf("%s: %w (gave up after %d attempts in a row)", what, err, failures)
		}
		if re.after > p.MaxRetryAfter {
			return fmt.Errorf("%s: %w (server asked to wait %s, more than the %s limit)", what, err, re.after, p.MaxRetryAfter)
		}
		d := p.delay(failures-1, re.after)
		c.logf("%s: %v; retry %d/%d in %s", what, err, failures, limit-1, d.Round(time.Millisecond))
		if onWait != nil {
			onWait(d, err)
		}
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("%s: %w", what, ctx.Err())
		case <-t.C:
		}
	}
}

func statusError(resp *http.Response) error {
	err := fmt.Errorf("unexpected status: %s", resp.Status)
	code := resp.StatusCode
	if code == http.StatusTooManyRequests || code == http.StatusRequestTimeout || code >= 500 {
		return &retryable{err: err, after: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()), code: code}
	}
	return err
}

func isThrottle(err error) bool {
	var re *retryable
	return errors.As(err, &re) && (re.code == http.StatusTooManyRequests || re.code == http.StatusServiceUnavailable)
}

func parseRetryAfter(h string, now time.Time) time.Duration {
	if h == "" {
		return 0
	}
	if s, err := strconv.Atoi(h); err == nil {
		return time.Duration(max(s, 0)) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		return max(t.Sub(now), 0)
	}
	return 0
}

var errIdle = errors.New("no data received")

type watchdog struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	timer  *time.Timer
	d      time.Duration
}

func newWatchdog(parent context.Context, d time.Duration) *watchdog {
	ctx, cancel := context.WithCancelCause(parent)
	w := &watchdog{ctx: ctx, cancel: cancel, d: d}
	if d > 0 {
		w.timer = time.AfterFunc(d, func() { cancel(fmt.Errorf("%w for %s", errIdle, d)) })
	}
	return w
}

func (w *watchdog) kick() {
	if w.timer != nil {
		w.timer.Reset(w.d)
	}
}

func (w *watchdog) stop() {
	if w.timer != nil {
		w.timer.Stop()
	}
	w.cancel(nil)
}

func (w *watchdog) wrap(err error) error {
	if cause := context.Cause(w.ctx); errors.Is(cause, errIdle) {
		return &retryable{err: cause}
	}
	return err
}

func (w *watchdog) reader(r io.Reader) io.Reader { return kickReader{r: r, w: w} }

type kickReader struct {
	r io.Reader
	w *watchdog
}

func (k kickReader) Read(p []byte) (int, error) {
	n, err := k.r.Read(p)
	if n > 0 {
		k.w.kick()
	}
	return n, err
}
