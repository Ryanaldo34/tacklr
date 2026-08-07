package brain

import "testing"

func TestDegradeMode_String(t *testing.T) {
	if DegradeMode("").String() != string(DegradeNone) {
		t.Fatal("empty → none")
	}
	if DegradeLexicalOnly.String() != "lexical_only" {
		t.Fatal(DegradeLexicalOnly.String())
	}
}
