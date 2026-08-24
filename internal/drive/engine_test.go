package drive

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/streaming"
)

func TestBind_onceThenEngineOf(t *testing.T) {
	func() {
		defer func() {
			r := recover()
			if r == nil || !strings.Contains(fmt.Sprint(r), "not bound") {
				t.Fatalf("unbound EngineOf: %v", r)
			}
		}()
		EngineOf(nil)
	}()

	Bind(func(any) Engine { return nil })
	if EngineOf(nil) != nil {
		t.Fatal("bound adapter returned a non-nil engine")
	}

	defer func() {
		r := recover()
		if r == nil || !strings.Contains(fmt.Sprint(r), "already bound") {
			t.Fatalf("second Bind: %v", r)
		}
	}()
	Bind(func(any) Engine { return nil })
}

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
