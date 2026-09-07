package tacklr

import "testing"

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
