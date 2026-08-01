package session

import (
	"encoding/json"
	"fmt"

	"github.com/ryanaldo34/tacklr/interrupt"
)

type interruptEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// interruptMap is a map[string]interrupt.Interrupt with polymorphic JSON.
type interruptMap map[string]interrupt.Interrupt

func (m interruptMap) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	envelopes := make(map[string]interruptEnvelope, len(m))
	for k, intr := range m {
		data, _ := json.Marshal(intr)
		envelopes[k] = interruptEnvelope{Type: intr.TypeName(), Data: data}
	}
	return json.Marshal(envelopes)
}

func (m *interruptMap) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		*m = nil
		return nil
	}
	var envelopes map[string]interruptEnvelope
	if err := json.Unmarshal(b, &envelopes); err != nil {
		return err
	}
	*m = make(interruptMap, len(envelopes))
	for k, env := range envelopes {
		intr, ok := interrupt.New(env.Type)
		if !ok {
			return fmt.Errorf("unknown interrupt type: %s", env.Type)
		}
		if err := json.Unmarshal(env.Data, intr); err != nil {
			return fmt.Errorf("unmarshal interrupt %q: %w", k, err)
		}
		(*m)[k] = intr
	}
	return nil
}
