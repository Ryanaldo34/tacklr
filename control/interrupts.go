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
	b, _ := json.Marshal(c) // value type always marshals
	return string(b)
}

// ACP PermissionOptionKind values.
const (
	PermissionAllowOnce    = "allow_once"
	PermissionAllowAlways  = "allow_always"
	PermissionRejectOnce   = "reject_once"
	PermissionRejectAlways = "reject_always"
)

// PermissionOption is a single choice offered when a tool requires user approval.
// Field names match ACP session/request_permission options.
type PermissionOption struct {
	OptionID string `json:"optionId"`
	Name     string `json:"name"`
	Kind     string `json:"kind"`
}

// DefaultPermissionOptions are the standard ACP permission choices.
func DefaultPermissionOptions() []PermissionOption {
	return []PermissionOption{
		{OptionID: "allow-once", Name: "Allow once", Kind: PermissionAllowOnce},
		{OptionID: "allow-always", Name: "Allow always", Kind: PermissionAllowAlways},
		{OptionID: "reject-once", Name: "Reject", Kind: PermissionRejectOnce},
		{OptionID: "reject-always", Name: "Reject always", Kind: PermissionRejectAlways},
	}
}

// ToolPermissionPayload is the consumer resolution for a tool permission interrupt.
type ToolPermissionPayload struct {
	InterruptId string `json:"interruptId,omitempty"`
	OptionID    string `json:"optionId"`
}

// ToolPermissionInterrupt asks the user to approve or reject a tool call.
type ToolPermissionInterrupt struct {
	ToolName string             `json:"toolName,omitempty"`
	Options  []PermissionOption `json:"options"`

	// Set by Return after the consumer selects an option.
	SelectedOptionID string `json:"-"`
	SelectedKind     string `json:"-"`
	Allowed          bool   `json:"-"`
}

func (p *ToolPermissionInterrupt) TypeName() string { return "tool_permission" }

func (p *ToolPermissionInterrupt) Serialize() ([]byte, error) {
	return json.Marshal(p)
}

func (p *ToolPermissionInterrupt) InitFromPayload(payload []byte) error {
	if len(payload) == 0 || string(payload) == "null" {
		p.Options = DefaultPermissionOptions()
		return nil
	}
	var init struct {
		ToolName string             `json:"toolName"`
		Options  []PermissionOption `json:"options"`
	}
	if err := json.Unmarshal(payload, &init); err != nil {
		return err
	}
	p.ToolName = init.ToolName
	if len(init.Options) == 0 {
		p.Options = DefaultPermissionOptions()
	} else {
		p.Options = init.Options
	}
	return nil
}

func (p *ToolPermissionInterrupt) ValidatePayload(payload []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if _, ok := raw["optionId"]; !ok {
		return errors.New("missing required field: optionId")
	}
	var res ToolPermissionPayload
	if err := json.Unmarshal(payload, &res); err != nil {
		return fmt.Errorf("invalid payload shape: %w", err)
	}
	for i := range p.Options {
		if p.Options[i].OptionID == res.OptionID {
			return nil
		}
	}
	return fmt.Errorf("unknown optionId %q", res.OptionID)
}

func (p *ToolPermissionInterrupt) Return(payload []byte) error {
	var res ToolPermissionPayload
	if err := json.Unmarshal(payload, &res); err != nil {
		return err
	}
	var opt *PermissionOption
	for i := range p.Options {
		if p.Options[i].OptionID == res.OptionID {
			opt = &p.Options[i]
			break
		}
	}
	if opt == nil {
		return fmt.Errorf("unknown optionId %q", res.OptionID)
	}
	p.SelectedOptionID = opt.OptionID
	p.SelectedKind = opt.Kind
	switch opt.Kind {
	case PermissionAllowOnce, PermissionAllowAlways:
		p.Allowed = true
	case PermissionRejectOnce, PermissionRejectAlways:
		p.Allowed = false
	default:
		return fmt.Errorf("unknown permission kind %q", opt.Kind)
	}
	return nil
}

func (p *ToolPermissionInterrupt) Error() string {
	b, _ := json.Marshal(p) // value type always marshals
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
	registerInterrupt(func() Interrupt { return &ToolPermissionInterrupt{} })
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
		// Registered interrupt types are plain JSON structs.
		data, _ := json.Marshal(intr)
		envelopes[k] = interruptEnvelope{Type: intr.TypeName(), Data: data}
	}
	return json.Marshal(envelopes)
}

// cloneInterrupt returns a deep copy via JSON so SnapshotState does not share
// live Interrupt pointers with concurrent ReturnInterrupt mutations.
func cloneInterrupt(intr Interrupt) Interrupt {
	if intr == nil {
		return nil
	}
	factory, ok := interruptFactories[intr.TypeName()]
	if !ok {
		return nil
	}
	data, err := json.Marshal(intr)
	if err != nil {
		return nil
	}
	cp := factory()
	if err := json.Unmarshal(data, cp); err != nil {
		return nil
	}
	return cp
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
