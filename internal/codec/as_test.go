package codec_test

import (
	"testing"

	"github.com/ryanaldo34/tacklr/internal/codec"
)

// TestAs_typedJSONAndReject covers the three return paths: typed hit, JSON
// coerce from a wire map, and a value that cannot marshal.
func TestAs_typedJSONAndReject(t *testing.T) {
	got, ok := codec.As[map[string]bool](map[string]bool{"a": true})
	if !ok || !got["a"] {
		t.Fatalf("typed: %v ok=%v", got, ok)
	}
	got, ok = codec.As[map[string]bool](map[string]any{"b": true})
	if !ok || !got["b"] {
		t.Fatalf("json: %v ok=%v", got, ok)
	}
	if _, ok = codec.As[map[string]bool](make(chan int)); ok {
		t.Fatal("unmarshalable value")
	}
}
