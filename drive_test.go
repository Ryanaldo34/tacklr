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

func TestNext_waitLoopActions(t *testing.T) {
	if Next(2, true, true, true) != ActionRunTools {
		t.Fatal("leftover tools")
	}
	if Next(0, true, false, false) != ActionYield {
		t.Fatal("park")
	}
	if Next(0, false, true, true) != ActionWait {
		t.Fatal("jobs remain")
	}
	if Next(0, false, true, false) != ActionComplete {
		t.Fatal("complete")
	}
	if Next(0, false, false, false) != ActionInfer {
		t.Fatal("infer")
	}
}
