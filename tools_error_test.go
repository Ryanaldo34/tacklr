package tacklr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/interrupt"
	"github.com/ryanaldo34/tacklr/vfs"
)

func TestAgentError_isAgentAndCause(t *testing.T) {
	err := AgentError(vfs.ErrNotExist, "read: that path does not exist. List the parent with run_command (ls)")
	if !errors.Is(err, ErrAgent) {
		t.Fatal("Is ErrAgent")
	}
	if !errors.Is(err, vfs.ErrNotExist) {
		t.Fatal("Is cause")
	}
	if errors.Is(err, ErrFailed) {
		t.Fatal("must not be harness ErrFailed")
	}
	if strings.Contains(err.Error(), "vfs:") || err.Error() == ErrAgent.Error() {
		t.Fatalf("model text: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "does not exist") || !strings.Contains(err.Error(), "ls") {
		t.Fatalf("correction: %q", err.Error())
	}
}

func TestPresentToolError_vfsSentinelsTellHowToFix(t *testing.T) {
	cases := []struct {
		name string
		err  error
		is   error
		want []string
		not  []string
	}{
		{"not exist", vfs.ErrNotExist, vfs.ErrNotExist, []string{"does not exist", "ls"}, []string{"vfs:"}},
		{"stale", vfs.ErrStaleContent, vfs.ErrStaleContent, []string{"changed", "Omit rev"}, []string{"vfs:"}},
		{"empty html", vfs.ErrEmptyReplace, vfs.ErrEmptyReplace, []string{"<p>", "<h1>"}, []string{"IR"}},
		{"invalid write", vfs.ErrInvalidWrite, vfs.ErrInvalidWrite, []string{"not saved", "write the full HTML again"}, []string{"retry", "vfs:"}},
		{"timeout", fmt.Errorf("tool %q: %w", "web_search", ErrToolTimeout), ErrToolTimeout, []string{"timed out", "smaller"}, nil},
		{"exa publication", fmt.Errorf(`exa search: status 400: {"error":"The provided domain 'census.gov' is not supported for category=publication","tag":"UNSUPPORTED_PUBLICATION_INCLUDE_FILTER"}`), nil, []string{"publication", "Omit category", "include_domains"}, []string{"googleapi", "status 400"}},
	}
	for _, tc := range cases {
		got := presentToolError("read", tc.err)
		if got == nil {
			t.Fatalf("%s: nil", tc.name)
		}
		if !errors.Is(got, ErrAgent) {
			t.Fatalf("%s: want ErrAgent, got %v", tc.name, got)
		}
		if tc.is != nil && !errors.Is(got, tc.is) {
			t.Fatalf("%s: want Is %v", tc.name, tc.is)
		}
		msg := got.Error()
		for _, w := range tc.want {
			if !strings.Contains(msg, w) {
				t.Fatalf("%s: want %q in %q", tc.name, w, msg)
			}
		}
		for _, n := range tc.not {
			if strings.Contains(msg, n) {
				t.Fatalf("%s: did not want %q in %q", tc.name, n, msg)
			}
		}
	}
}

func TestPresentToolError_passesInterruptCancelAndFailed(t *testing.T) {
	intr := &interrupt.ToolPermissionInterrupt{ToolName: "write"}
	if got := presentToolError("write", intr); got != intr {
		t.Fatalf("interrupt rewritten: %v", got)
	}
	if got := presentToolError("write", context.Canceled); !errors.Is(got, context.Canceled) || errors.Is(got, ErrAgent) {
		t.Fatalf("cancel: %v", got)
	}
	harness := fmt.Errorf("worker panic: %w", ErrFailed)
	got := presentToolError("spawn_worker", harness)
	if !errors.Is(got, ErrFailed) || errors.Is(got, ErrAgent) {
		t.Fatalf("ErrFailed became agent: %v", got)
	}
}

func TestPresentToolError_doesNotDoubleWrap(t *testing.T) {
	first := presentWriteError("/doc", vfs.ErrInvalidWrite)
	second := presentToolError("write", first)
	if second.Error() != first.Error() {
		t.Fatalf("double wrap:\n%s\n%s", first, second)
	}
	if !errors.Is(second, vfs.ErrInvalidWrite) || !errors.Is(second, ErrAgent) {
		t.Fatal("Is")
	}
}
