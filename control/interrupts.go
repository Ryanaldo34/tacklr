package control

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Interrupt represents a pending interrupt in an agent workflow requesting
// the user for additional input or approval. Implementations must also
// satisfy the error interface so they can be returned from tool handlers
// and detected via errors.As.
type Interrupt interface {
	// TypeName returns a stable, unique name for this interrupt type. It is
	// used as the JSON discriminator when persisting interrupts to session
	// storage and as the lookup key in the interrupt registry.
	TypeName() string
	// Serialize the interrupt to a byte slice for transmission.
	Serialize() ([]byte, error)
	// Return the interrupt to the agent workflow with a serialized
	// payload of the interrupt result.
	Return([]byte) error
	// Error makes Interrupt compatible with the error interface.
	Error() string
}

// PayloadValidator is an optional capability for Interrupt types that want
// to pre-validate the consumer's response payload before it is passed to
// Return(). Implementations should check field presence, types, and bounds
// without mutating the Interrupt's internal state (that happens in Return).
// Detected via type assertion in ReturnInterrupt.
type PayloadValidator interface {
	ValidatePayload([]byte) error
}

type UserChoice struct {
	Title         string `json:"title"`
	Description   string `json:"description"`
	IsRecommended bool   `json:"isRecommended"`
}

type UserSelectionPayload struct {
	InterruptId  string `json:"interruptId"`
	SelectionIdx int    `json:"selectionIdx"`
}

type UserSelectionInterrupt struct {
	Options         []UserChoice `json:"options"`
	ConfirmedChoice *UserChoice
}

func (c *UserSelectionInterrupt) TypeName() string {
	return "user_selection_choice"
}

func (c *UserSelectionInterrupt) Serialize() ([]byte, error) {
	return json.Marshal(c)
}

func (c *UserSelectionInterrupt) ValidatePayload(payload []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if _, ok := raw["selectionIdx"]; !ok {
		return errors.New("missing required field: selectionIdx")
	}
	var selection UserSelectionPayload
	if err := json.Unmarshal(payload, &selection); err != nil {
		return fmt.Errorf("invalid payload shape: %w", err)
	}
	if selection.SelectionIdx < 0 || selection.SelectionIdx >= len(c.Options) {
		return fmt.Errorf("selectionIdx %d out of range [0, %d)", selection.SelectionIdx, len(c.Options))
	}
	return nil
}

func (c *UserSelectionInterrupt) Return(payload []byte) error {
	var selection UserSelectionPayload
	if err := json.Unmarshal(payload, &selection); err != nil {
		return err
	}
	if selection.SelectionIdx < 0 || selection.SelectionIdx >= len(c.Options) {
		return fmt.Errorf("selectionIdx %d out of range [0, %d)", selection.SelectionIdx, len(c.Options))
	}
	c.ConfirmedChoice = &c.Options[selection.SelectionIdx]
	return nil
}

func (c *UserSelectionInterrupt) Error() string {
	b, err := json.Marshal(c)
	if err != nil {
		return "[failed to marshal interrupt]"
	}
	return string(b)
}

// --- Interrupt registry & polymorphic JSON ---

// interruptFactories creates fresh, empty Interrupt values for a given type name.
// Used by both RaiseInterrupt (which then calls InitFromPayload to populate
// from the tool-author-provided payload) and interruptMap.UnmarshalJSON
// (which then calls json.Unmarshal on the saved struct data).
var interruptFactories = map[string]func() Interrupt{}

// registerInterrupt adds a factory for an Interrupt type under its TypeName.
// It is called at package init time for each concrete Interrupt implementation.
func registerInterrupt(factory func() Interrupt) {
	intr := factory()
	name := intr.TypeName()
	if _, ok := interruptFactories[name]; ok {
		panic(fmt.Sprintf("interrupt type %q registered twice", name))
	}
	interruptFactories[name] = factory
}

func init() {
	registerInterrupt(func() Interrupt { return &UserSelectionInterrupt{} })
}

// payloadInitializer is an optional capability that Interrupt types implement
// to populate themselves from the raw payload provided to RaiseInterrupt.
// The payload format is type-specific (e.g., []UserChoice for
// UserSelectionInterrupt) and may differ from the struct's own JSON shape.
// This is detected via type assertion, so it stays off the public Interrupt
// interface.
type payloadInitializer interface {
	InitFromPayload([]byte) error
}

func (c *UserSelectionInterrupt) InitFromPayload(payload []byte) error {
	return json.Unmarshal(payload, &c.Options)
}

type interruptEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// interruptMap is a map[string]Interrupt with polymorphic JSON marshaling.
// Each entry is wrapped in an interruptEnvelope so the concrete Interrupt
// type can be reconstructed on unmarshal via the interruptFactories registry.
type interruptMap map[string]Interrupt

func (m interruptMap) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	envelopes := make(map[string]interruptEnvelope, len(m))
	for k, intr := range m {
		data, err := json.Marshal(intr)
		if err != nil {
			return nil, fmt.Errorf("marshal interrupt %q: %w", k, err)
		}
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
		factory, ok := interruptFactories[env.Type]
		if !ok {
			return fmt.Errorf("unknown interrupt type: %s", env.Type)
		}
		intr := factory()
		if err := json.Unmarshal(env.Data, intr); err != nil {
			return fmt.Errorf("unmarshal interrupt %q: %w", k, err)
		}
		(*m)[k] = intr
	}
	return nil
}
