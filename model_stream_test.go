package tacklr

import (
	"context"
	"runtime"
	"testing"

	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/telemetry"
)

// TestWatchModelStream_aCancelEndsTheStreamAndFreesTheProvider: after a
// cancel the watched stream closes and the provider is not left blocked,
// whether the reader is waiting for a chunk or has stopped reading. The
// real client cannot hold a chunk the watcher cannot hand on at the
// moment of a cancel, so this is a unit test.
func TestWatchModelStream_aCancelEndsTheStreamAndFreesTheProvider(t *testing.T) {
	for name, unread := range map[string]int{"a reader waiting for a chunk": 0, "a reader that stopped reading": 17} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			in := make(chan LLMResponseChunk, 32)
			_, span := telemetry.StartModelSpan(ctx, telemetry.ModelPhaseTurn, 1, telemetry.WindowShape{})
			out := watchModelStream(ctx, span, in)
			for range unread + 3 {
				in <- LLMResponseChunk{Type: session.EventMessage, Content: "partial"}
			}
			// The watcher holds one chunk it cannot hand on: out is full.
			for unread > 0 && len(in) > 3 {
				runtime.Gosched()
			}

			cancel()
			got := 0
			for range out {
				got++
			}

			if got > 16 || len(in) != 0 {
				t.Fatalf("read %d chunks, %d left with the provider; want the stream closed and the provider's chunks drained", got, len(in))
			}
		})
	}
}
