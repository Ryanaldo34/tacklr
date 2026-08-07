package streaming

import "testing"

func TestToolCall_KeyAndWireID(t *testing.T) {
	if (ToolCall{ID: "i", CallID: "c"}).Key() != "i" {
		t.Fatal("Key prefers ID")
	}
	if (ToolCall{CallID: "c"}).Key() != "c" {
		t.Fatal("Key falls back to CallID")
	}
	if (ToolCall{ID: "i", CallID: "c"}).WireID() != "c" {
		t.Fatal("WireID prefers CallID")
	}
	if (ToolCall{ID: "i"}).WireID() != "i" {
		t.Fatal("WireID falls back to ID")
	}
}
