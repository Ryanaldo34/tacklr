package tacklr

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/drive"
)

func TestEngineOf_wrapsAgentHarness(t *testing.T) {
	eng := drive.EngineOf(&AgentHarness{})
	if eng == nil {
		t.Fatal("engine")
	}
	if calls := eng.PendingToolCalls(); calls == nil {
		t.Fatal("pending tool calls")
	}
}

func TestEngineOf_rejectsNonHarness(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil || !strings.Contains(fmt.Sprint(r), "not *AgentHarness") {
			t.Fatalf("got %v", r)
		}
	}()
	drive.EngineOf("host")
}

func TestBind_alreadyBound(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil || !strings.Contains(fmt.Sprint(r), "already bound") {
			t.Fatalf("got %v", r)
		}
	}()
	drive.Bind(func(any) drive.Engine { return nil })
}
