package engine

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

const UserAgent = "tako/0.1"

type Client struct {
	HTTP  *http.Client
	Retry RetryPolicy
	Logf  func(format string, args ...any)
}

func NewClient(maxConns int) *Client {
	return &Client{
		HTTP:  &http.Client{Transport: newTransport(maxConns)},
		Retry: DefaultRetry,
	}
}

func newTransport(maxConns int) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Protocols = new(http.Protocols)
	t.Protocols.SetHTTP1(true)
	if t.TLSClientConfig == nil {
		t.TLSClientConfig = &tls.Config{}
	}
	t.TLSClientConfig.NextProtos = []string{"http/1.1"}
	t.MaxIdleConnsPerHost = max(maxConns, 1)
	t.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	t.TLSHandshakeTimeout = 10 * time.Second
	return t
}

func (c *Client) logf(format string, args ...any) {
	if c.Logf != nil {
		c.Logf(format, args...)
	}
}

func newRequest(ctx context.Context, rawURL string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", UserAgent)
	req.Header.Set("Accept-Encoding", "identity")
	return req, nil
}
