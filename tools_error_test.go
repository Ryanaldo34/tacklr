package tacklr

import (
	"errors"
	"strings"
	"testing"

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
