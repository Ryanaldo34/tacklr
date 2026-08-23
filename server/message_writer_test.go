package server

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/streaming"
)

func TestPresentStreamEvent_withError(t *testing.T) {
	ev, err := presentStreamEvent(streaming.StreamEvent{Type: streaming.StreamEventError, Error: errors.New("e")})
	if err != nil {
		t.Fatal(err)
	}
	if ev.ErrorText != "e" {
		t.Fatalf("%+v", ev)
	}
	if _, err := presentStreamEvent(streaming.StreamEvent{}); err == nil {
		t.Fatal("unknown stream event type was silently accepted")
	}
}

func TestPresentStreamEvent_message(t *testing.T) {
	presented, err := presentStreamEvent(streaming.StreamEvent{Type: streaming.StreamEventMessage, Content: "x"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(presented)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("empty payload")
	}
}
