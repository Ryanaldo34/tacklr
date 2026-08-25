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

func TestCorrection_isAgentAndCause(t *testing.T) {
	err := Correction(vfs.ErrNotExist, "read: that path does not exist. List the parent with run_command (ls)")
	if !errors.Is(err, ErrCorrection) {
		t.Fatal("Is ErrCorrection")
	}
	if !errors.Is(err, vfs.ErrNotExist) {
		t.Fatal("Is cause")
	}
	if errors.Is(err, ErrFailed) {
		t.Fatal("must not be harness ErrFailed")
	}
	if strings.Contains(err.Error(), "vfs:") || err.Error() == ErrCorrection.Error() {
		t.Fatalf("model text: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "does not exist") || !strings.Contains(err.Error(), "ls") {
		t.Fatalf("correction: %q", err.Error())
	}
}

func TestCorrection_emptyMsgAndAlreadyWrapped(t *testing.T) {
	if got := Correction(nil, "  "); !errors.Is(got, ErrCorrection) || got.Error() != ErrCorrection.Error() {
		t.Fatalf("empty: %v", got)
	}
	fromCause := Correction(vfs.ErrNotExist, "")
	if !errors.Is(fromCause, ErrCorrection) || !errors.Is(fromCause, vfs.ErrNotExist) {
		t.Fatalf("cause: %v", fromCause)
	}
	if strings.Contains(fromCause.Error(), "vfs:") || fromCause.Error() != "not found" {
		t.Fatalf("stripped: %q", fromCause.Error())
	}
	first := Correctionf(vfs.ErrUseHTML, "write HTML, not plain text")
	again := Correction(first, "other")
	if again.Error() != first.Error() || !errors.Is(again, vfs.ErrUseHTML) {
		t.Fatalf("already wrapped: %v", again)
	}
	var nilAE *correctionError
	if nilAE.Error() != ErrCorrection.Error() {
		t.Fatal("nil receiver")
	}
	if (&correctionError{}).Error() != ErrCorrection.Error() {
		t.Fatal("empty msg")
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
		{"use html", vfs.ErrUseHTML, vfs.ErrUseHTML, []string{"Pass content as HTML", "<h1>"}, []string{"vfs:"}},
		{"tab id", vfs.ErrTabIDRequired, vfs.ErrTabIDRequired, []string{"more than one tab", "tab_id"}, nil},
		{"projected", vfs.ErrProjected, vfs.ErrProjected, []string{"not supported", "Sheet!A1"}, nil},
		{"invalid path", vfs.ErrInvalidPath, vfs.ErrInvalidPath, []string{"not a valid virtual path", "/workspace/"}, []string{"vfs:"}},
		{"is dir", vfs.ErrIsDir, vfs.ErrIsDir, []string{"directory", "ls"}, []string{"vfs:"}},
		{"not dir", vfs.ErrNotDir, vfs.ErrNotDir, []string{"is a file, not a directory"}, []string{"vfs:"}},
		{"auth expired", vfs.ErrAuthExpired, vfs.ErrAuthExpired, []string{"credentials expired", "refresh"}, []string{"vfs:"}},
		{"permission", vfs.ErrPermission, vfs.ErrPermission, []string{"permission denied"}, []string{"vfs:"}},
		{"read only", vfs.ErrReadOnly, vfs.ErrReadOnly, []string{"read-only", "writable mount"}, []string{"vfs:"}},
		{"line range", vfs.ErrLineOutOfRange, vfs.ErrLineOutOfRange, []string{"line range", "line_count"}, []string{"vfs:"}},
		{"invalid line", vfs.ErrInvalidLine, vfs.ErrInvalidLine, []string{"newline", "array entry"}, []string{"vfs:"}},
		{"too large", vfs.ErrTooLarge, vfs.ErrTooLarge, []string{"too large", "smaller window"}, []string{"vfs:"}},
		{"fuse", vfs.ErrFuseNotMounted, vfs.ErrFuseNotMounted, []string{"not mounted", "virtual paths"}, []string{"vfs:"}},
		{"not textual", vfs.ErrNotTextual, vfs.ErrNotTextual, []string{"not text"}, []string{"vfs:"}},
		{"timeout", fmt.Errorf("tool %q: %w", "web_search", ErrToolTimeout), ErrToolTimeout, []string{"timed out", "smaller"}, nil},
		{"permission denied", ErrToolPermissionDenied, ErrToolPermissionDenied, []string{"rejected by the user", "Do not retry"}, nil},
		{"not found specialist", fmt.Errorf("spawn: %w: specialist missing", ErrNotFound), ErrNotFound, []string{"specialist is not registered"}, nil},
		{"not found job", fmt.Errorf("get_child: %w: unknown job", ErrNotFound), ErrNotFound, []string{"child_id is unknown", "list_children"}, nil},
		{"not found other", ErrNotFound, ErrNotFound, []string{"not found", "id or path"}, nil},
		{"invalid child_id", fmt.Errorf("get_child: %w: child_id missing", ErrInvalid), ErrInvalid, []string{"child_id is required", "list_children"}, nil},
		{"invalid command", fmt.Errorf("run_command: %w: command is required", ErrInvalid), ErrInvalid, []string{"command is required", "ls work"}, nil},
		{"invalid empty task", fmt.Errorf("spawn: %w: empty task", ErrInvalid), ErrInvalid, []string{"task_description_and_context is required"}, nil},
		{"invalid required", fmt.Errorf("write: %w: path is required", ErrInvalid), ErrInvalid, []string{"path is required"}, []string{"invalid arguments"}},
		{"invalid generic", fmt.Errorf("write: %w: bad json", ErrInvalid), ErrInvalid, []string{"invalid arguments", "bad json"}, nil},
		{"generic named", errors.New("provider exploded"), nil, []string{"read: provider exploded"}, nil},
		{"exa publication", fmt.Errorf(`exa search: status 400: {"error":"The provided domain 'census.gov' is not supported for category=publication","tag":"UNSUPPORTED_PUBLICATION_INCLUDE_FILTER"}`), nil, []string{"publication", "Omit category", "include_domains"}, []string{"googleapi", "status 400"}},
		{"exa 401", errors.New("exa search: status 401: unauthorized"), nil, []string{"API key", "Exa credentials"}, []string{"status 401"}},
		{"exa 429", errors.New("exa search: status 429: slow down"), nil, []string{"rate-limited", "Wait and retry"}, []string{"status 429"}},
		{"exa 400", errors.New("exa search: status 400: bad filters"), nil, []string{"rejected that request", "omit category"}, []string{"status 400"}},
		{"exa down", errors.New("exa search: connection reset"), nil, []string{"search provider failed", "simpler query"}, nil},
	}
	for _, tc := range cases {
		got := presentToolError("read", tc.err)
		if got == nil {
			t.Fatalf("%s: nil", tc.name)
		}
		if !errors.Is(got, ErrCorrection) {
			t.Fatalf("%s: want ErrCorrection, got %v", tc.name, got)
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
	if got := presentToolError("write", nil); got != nil {
		t.Fatalf("nil: %v", got)
	}
	intr := &interrupt.ToolPermissionInterrupt{ToolName: "write"}
	if got := presentToolError("write", intr); !errors.Is(got, intr) {
		t.Fatalf("interrupt rewritten: %v", got)
	}
	if got := presentToolError("write", context.Canceled); !errors.Is(got, context.Canceled) || errors.Is(got, ErrCorrection) {
		t.Fatalf("cancel: %v", got)
	}
	harness := fmt.Errorf("worker panic: %w", ErrFailed)
	got := presentToolError("spawn_specialist", harness)
	if !errors.Is(got, ErrFailed) || errors.Is(got, ErrCorrection) {
		t.Fatalf("ErrFailed became agent: %v", got)
	}
	named := errors.New("read: already prefixed")
	if got := presentToolError("read", named); got.Error() != "read: already prefixed" {
		t.Fatalf("prefix: %q", got.Error())
	}
}

func TestPresentToolError_doesNotDoubleWrap(t *testing.T) {
	first := presentWriteError("/doc", vfs.ErrInvalidWrite)
	second := presentToolError("write", first)
	if second.Error() != first.Error() {
		t.Fatalf("double wrap:\n%s\n%s", first, second)
	}
	if !errors.Is(second, vfs.ErrInvalidWrite) || !errors.Is(second, ErrCorrection) {
		t.Fatal("Is")
	}
}
