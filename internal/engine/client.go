package engine

import (
	"context"
	"net/http"
)

const UserAgent = "fast-download/0.1"

func NewClient() *http.Client {
	return &http.Client{}
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
