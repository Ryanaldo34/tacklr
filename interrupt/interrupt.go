package interrupt

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

// CallEffect is an optional capability for interrupts raised from Tool.OnCall.
// After Return, the harness applies the effect before the handler runs.
type CallEffect interface {
	// ReplacementArgs is the args JSON to use for the call. Empty keeps the original.
	ReplacementArgs() string
	// CallDenied is true when the handler must not run.
	CallDenied() bool
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
	Title    string             `json:"title,omitempty"` // human-readable invocation label
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
		Title    string             `json:"title"`
		Options  []PermissionOption `json:"options"`
	}
	if err := json.Unmarshal(payload, &init); err != nil {
		return err
	}
	p.ToolName = init.ToolName
	p.Title = init.Title
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

func (p *ToolPermissionInterrupt) ReplacementArgs() string { return "" }

func (p *ToolPermissionInterrupt) CallDenied() bool {
	return p.SelectedKind != "" && !p.Allowed
}

// Predecided is true when the constructor already resolved this interrupt
// (session allow-always / reject-always) and the harness must not park.
func (p *ToolPermissionInterrupt) Predecided() bool {
	return p.SelectedKind != ""
}

// Write-approval actions returned by the host.
const (
	WriteApprovalApprove = "approve"
	WriteApprovalEdit    = "edit"
	WriteApprovalReject  = "reject"
	WriteApprovalType    = "write_approval"
)

// WriteApprovalPayload is the host resolution for a write-approval interrupt.
type WriteApprovalPayload struct {
	InterruptId string `json:"interruptId,omitempty"`
	Action      string `json:"action"`
	Args        string `json:"args,omitempty"`
}

// WriteApprovalInterrupt parks a write tool until the host approves, edits, or rejects.
type WriteApprovalInterrupt struct {
	ToolName string `json:"toolName,omitempty"`
	Title    string `json:"title,omitempty"`
	Args     string `json:"args,omitempty"`

	// Set by Return after the host chooses an action.
	Action string `json:"-"`
}

func (w *WriteApprovalInterrupt) TypeName() string { return WriteApprovalType }

func (w *WriteApprovalInterrupt) Serialize() ([]byte, error) {
	return json.Marshal(w)
}

func (w *WriteApprovalInterrupt) InitFromPayload(payload []byte) error {
	if len(payload) == 0 || string(payload) == "null" {
		return nil
	}
	return json.Unmarshal(payload, w)
}

func (p WriteApprovalPayload) valid() error {
	switch p.Action {
	case WriteApprovalApprove, WriteApprovalReject:
		return nil
	case WriteApprovalEdit:
		if p.Args == "" {
			return errors.New("edit requires args")
		}
		return nil
	default:
		return fmt.Errorf("unknown action %q", p.Action)
	}
}

func (w *WriteApprovalInterrupt) ValidatePayload(payload []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	if _, ok := raw["action"]; !ok {
		return errors.New("missing required field: action")
	}
	var res WriteApprovalPayload
	if err := json.Unmarshal(payload, &res); err != nil {
		return fmt.Errorf("invalid payload shape: %w", err)
	}
	return res.valid()
}

func (w *WriteApprovalInterrupt) Return(payload []byte) error {
	var res WriteApprovalPayload
	if err := json.Unmarshal(payload, &res); err != nil {
		return err
	}
	if err := res.valid(); err != nil {
		return err
	}
	w.Action = res.Action
	if res.Action == WriteApprovalEdit {
		w.Args = res.Args
	}
	return nil
}

func (w *WriteApprovalInterrupt) Error() string {
	b, _ := json.Marshal(w)
	return string(b)
}

func (w *WriteApprovalInterrupt) ReplacementArgs() string {
	if w.Action == WriteApprovalEdit {
		return w.Args
	}
	return ""
}

func (w *WriteApprovalInterrupt) CallDenied() bool {
	return w.Action == WriteApprovalReject
}

// --- Interrupt registry ---

var interruptFactories = map[string]func() Interrupt{}

// Register adds a factory for an Interrupt type under its TypeName.
// Call at init for custom interrupts so checkpoints can rehydrate them.
func Register(factory func() Interrupt) {
	intr := factory()
	name := intr.TypeName()
	if _, ok := interruptFactories[name]; ok {
		panic(fmt.Sprintf("interrupt type %q registered twice", name))
	}
	interruptFactories[name] = factory
}

// New returns a fresh Interrupt for a registered type name.
func New(typeName string) (Interrupt, bool) {
	f, ok := interruptFactories[typeName]
	if !ok {
		return nil, false
	}
	return f(), true
}

// Clone returns a deep copy via JSON for checkpoint snapshots.
// Returns nil if the type is unknown or serialization fails (best-effort).
func Clone(intr Interrupt) Interrupt {
	if intr == nil {
		return nil
	}
	cp, ok := New(intr.TypeName())
	if !ok {
		return nil
	}
	// Success-only path: avoid "err != nil then return nil" (nilerr).
	data, err := json.Marshal(intr)
	if err == nil {
		if err = json.Unmarshal(data, cp); err == nil {
			return cp
		}
	}
	return nil
}

func init() {
	Register(func() Interrupt { return &UserSelectionInterrupt{} })
	Register(func() Interrupt { return &ToolPermissionInterrupt{} })
	Register(func() Interrupt { return &WriteApprovalInterrupt{} })
}

// PayloadInitializer is an optional capability Interrupt types implement
// to populate themselves from the raw payload provided to RaiseInterrupt.
type PayloadInitializer interface {
	InitFromPayload([]byte) error
}

func (c *UserSelectionInterrupt) InitFromPayload(payload []byte) error {
	return json.Unmarshal(payload, &c.Options)
}
