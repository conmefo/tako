package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const stateVersion = 1

type savedState struct {
	Version      int          `json:"version"`
	URL          string       `json:"url"`
	FinalURL     string       `json:"final_url"`
	Size         int64        `json:"size"`
	ETag         string       `json:"etag,omitempty"`
	LastModified string       `json:"last_modified,omitempty"`
	Chunks       []savedChunk `json:"chunks"`
}

type savedChunk struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
	Done  int64 `json:"done"`
}

func newState(info FileInfo, chunks []savedChunk) *savedState {
	return &savedState{
		Version:      stateVersion,
		URL:          info.URL,
		FinalURL:     info.FinalURL,
		Size:         info.Size,
		ETag:         info.ETag,
		LastModified: info.LastModified,
		Chunks:       chunks,
	}
}

func loadState(path string) (*savedState, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s savedState
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &s, nil
}

func saveState(path string, s *savedState) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		d.Sync()
		d.Close()
	}
	return nil
}

func (s *savedState) mismatch(info FileInfo) string {
	switch {
	case s.Version != stateVersion:
		return "state file is from another version"
	case s.URL != info.URL:
		return "state file is for another URL"
	case s.Size != info.Size:
		return fmt.Sprintf("size changed from %d to %d", s.Size, info.Size)
	case s.ETag == "" && s.LastModified == "":
		return "server gives no ETag or Last-Modified, so a change can't be detected"
	case s.ETag != info.ETag || s.LastModified != info.LastModified:
		return "the file on the server changed"
	}
	var next int64
	for _, c := range s.Chunks {
		if c.Start != next || c.End < c.Start || c.Done < 0 || c.Done > c.End-c.Start+1 {
			return "state file is corrupt"
		}
		next = c.End + 1
	}
	if next != s.Size {
		return "state file is corrupt"
	}
	return ""
}

func (s *savedState) doneBytes() int64 {
	var n int64
	for _, c := range s.Chunks {
		n += c.Done
	}
	return n
}
