package tacklr

import (
	"testing"
)

func TestPipeStreamEvents_forwardsThenStops(t *testing.T) {
	got := make([]StreamEvent, 0, 1)
	ch, stop := PipeStreamEvents(func(ev StreamEvent) {
		got = append(got, ev)
	})
	ch <- StreamEvent{Type: StreamEventComplete}
	stop()
	if len(got) != 1 || got[0].Type != StreamEventComplete {
		t.Fatalf("got %#v", got)
	}

	ch2, stop2 := PipeStreamEvents(nil)
	ch2 <- StreamEvent{Type: StreamEventComplete}
	stop2()
}
