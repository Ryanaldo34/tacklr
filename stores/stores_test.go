package stores

import (
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/streaming"
)

func TestNewCheckpoint_rejectsInvalidContextWindow(t *testing.T) {
	_, err := NewCheckpoint(
		[]*streaming.Message{{Role: streaming.RoleTool, Content: "missing id"}},
		nil, nil, nil, nil, nil,
	)
	if err == nil || !strings.Contains(err.Error(), "invalid context window") {
		t.Fatalf("error = %v", err)
	}
}

func TestNewCheckpoint_marshalInterruptErrors(t *testing.T) {
	if _, err := NewCheckpoint(nil, nil, nil, nil, make(chan int), nil); err == nil {
		t.Fatal("expected pending interrupt marshal error")
	}
	if _, err := NewCheckpoint(nil, nil, nil, nil, nil, make(chan int)); err == nil {
		t.Fatal("expected resolved interrupt marshal error")
	}
}
