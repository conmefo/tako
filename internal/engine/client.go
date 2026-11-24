package engine

import (
	"context"
	"net/http"
)

const UserAgent = "tako/0.1"

func NewClient(maxConns int) *http.Client {
	return &http.Client{Transport: newTransport(maxConns)}
}

func newTransport(maxConns int) *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Protocols = new(http.Protocols)
	t.Protocols.SetHTTP1(true)
	t.MaxIdleConnsPerHost = max(maxConns, 1)
	return t
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
