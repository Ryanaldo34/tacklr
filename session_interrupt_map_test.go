package tacklr

import (
	"encoding/json"
	"testing"

	"github.com/ryanaldo34/tacklr/interrupt"
)

func TestInterruptMap_marshalNilAndRoundTrip(t *testing.T) {
	// Arrange
	var nilMap interruptMap
	empty := interruptMap{}

	// Act
	nilJSON, err := json.Marshal(nilMap)
	if err != nil {
		t.Fatal(err)
	}
	emptyJSON, err := json.Marshal(empty)
	if err != nil {
		t.Fatal(err)
	}

	intr, ok := interrupt.New("tool_permission")
	if !ok {
		t.Fatal("tool_permission not registered")
	}
	if err := json.Unmarshal([]byte(`{"toolName":"grep"}`), intr); err != nil {
		t.Fatal(err)
	}
	withValue := interruptMap{"call-1": intr}
	valueJSON, err := json.Marshal(withValue)
	if err != nil {
		t.Fatal(err)
	}

	var decoded interruptMap
	if err := json.Unmarshal(valueJSON, &decoded); err != nil {
		t.Fatal(err)
	}
	var nullDecoded interruptMap
	if err := json.Unmarshal([]byte("null"), &nullDecoded); err != nil {
		t.Fatal(err)
	}

	// Assert
	if string(nilJSON) != "null" || string(emptyJSON) != "{}" {
		t.Fatalf("nil=%s empty=%s", nilJSON, emptyJSON)
	}
	if decoded["call-1"] == nil {
		t.Fatal("round trip lost interrupt")
	}
	if nullDecoded != nil {
		t.Fatalf("null decoded = %#v", nullDecoded)
	}
}

func TestInterruptMap_unmarshalEmptyBytes(t *testing.T) {
	var decoded interruptMap
	if err := decoded.UnmarshalJSON(nil); err != nil {
		t.Fatal(err)
	}
	if decoded != nil {
		t.Fatalf("decoded = %#v", decoded)
	}
}

func TestInterruptMap_unmarshalInvalidJSON(t *testing.T) {
	var decoded interruptMap
	if err := decoded.UnmarshalJSON([]byte(`{`)); err == nil {
		t.Fatal("want invalid JSON")
	}
}
