package testkit

import (
	"encoding/json"
	"sync"
	"testing"
)

// RecordedResult is one WriteResult call.
type RecordedResult struct {
	ID     json.RawMessage
	Result any
}

// RecordedError is one WriteError call.
type RecordedError struct {
	ID  json.RawMessage
	Err error
}

// RecordingWriter captures MessageWriter traffic for protocol tests.
type RecordingWriter struct {
	mu      sync.Mutex
	Results []RecordedResult
	Errors  []RecordedError
	Frames  [][]byte
}

func (r *RecordingWriter) WriteResult(id json.RawMessage, result any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Results = append(r.Results, RecordedResult{ID: append(json.RawMessage(nil), id...), Result: result})
	return nil
}

func (r *RecordingWriter) WriteError(id json.RawMessage, err error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Errors = append(r.Errors, RecordedError{ID: append(json.RawMessage(nil), id...), Err: err})
	return nil
}

func (r *RecordingWriter) WriteFrame(data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Frames = append(r.Frames, append([]byte(nil), data...))
	return nil
}

// FrameCount returns the number of recorded frames (safe for concurrent poll).
func (r *RecordingWriter) FrameCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.Frames)
}

// SnapshotFrames returns a copy of recorded frames (safe for concurrent poll).
func (r *RecordingWriter) SnapshotFrames() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.Frames))
	for i, f := range r.Frames {
		out[i] = append([]byte(nil), f...)
	}
	return out
}

// FramesAsMaps decodes recorded frames as JSON objects.
func (r *RecordingWriter) FramesAsMaps(t *testing.T) []map[string]any {
	t.Helper()
	frames := r.SnapshotFrames()
	out := make([]map[string]any, 0, len(frames))
	for _, f := range frames {
		var m map[string]any
		if err := json.Unmarshal(f, &m); err != nil {
			t.Fatalf("decode frame: %v\nframe: %s", err, f)
		}
		out = append(out, m)
	}
	return out
}
