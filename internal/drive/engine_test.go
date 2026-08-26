package drive

import (
	"testing"

	"github.com/ryanaldo34/tacklr/streaming"
)

func TestPipeStreamEvents_forwardsThenStops(t *testing.T) {
	got := make([]streaming.StreamEvent, 0, 1)
	ch, stop := PipeStreamEvents(func(ev streaming.StreamEvent) {
		got = append(got, ev)
	})
	ch <- streaming.StreamEvent{Type: streaming.StreamEventComplete}
	stop()
	if len(got) != 1 || got[0].Type != streaming.StreamEventComplete {
		t.Fatalf("got %#v", got)
	}

	ch2, stop2 := PipeStreamEvents(nil)
	ch2 <- streaming.StreamEvent{Type: streaming.StreamEventComplete}
	stop2()
}
