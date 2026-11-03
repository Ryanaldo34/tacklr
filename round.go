package tacklr

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/ryanaldo34/tacklr/message"

	"github.com/ryanaldo34/tacklr/brain"
)

// roundCall is the call a Tool step runs: its park and the OnCall layers it
// completed, from its prior outcome. It lives for the step only; the
// runtime keeps the outcome.
type roundCall struct {
	Call message.ToolCall
	// Park is the interrupt the call waits on.
	Park *callPark
	// Layers are the OnCall layers the call completed; a run skips them.
	Layers []onCallLayer
}

// layer is the call's completed OnCall layer of typeName.
func (c *roundCall) layer(typeName string) (onCallLayer, bool) {
	i := slices.IndexFunc(c.Layers, func(l onCallLayer) bool { return l.TypeName == typeName })
	if i < 0 {
		return onCallLayer{}, false
	}
	return c.Layers[i], true
}

// callPark is a call's interrupt. Answered is set by resume; the call's run
// takes the answered interrupt and continues.
type callPark struct {
	Interrupt Interrupt
	Answered  bool
}

// callParkJSON is a callPark as an outcome holds it: the interrupt's type
// names the registered type its data decodes into.
type callParkJSON[D any] struct {
	Type     string `json:"type"`
	Data     D      `json:"data"`
	Answered bool   `json:"answered,omitempty"`
}

func (p callPark) MarshalJSON() ([]byte, error) {
	return json.Marshal(callParkJSON[Interrupt]{Type: p.Interrupt.TypeName(), Data: p.Interrupt, Answered: p.Answered})
}

func (p *callPark) UnmarshalJSON(b []byte) error {
	var raw callParkJSON[json.RawMessage]
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	intr, ok := NewInterrupt(raw.Type)
	if !ok {
		return fmt.Errorf("unknown interrupt type: %s", raw.Type)
	}
	if err := json.Unmarshal(raw.Data, intr); err != nil {
		return fmt.Errorf("interrupt %s: %w", raw.Type, err)
	}
	p.Interrupt, p.Answered = intr, raw.Answered
	return nil
}

// onCallLayer is one OnCall layer a call passed: its interrupt type and the
// arguments it left. A denied layer ends the call, so it is not kept.
type onCallLayer struct {
	TypeName string `json:"typeName"`
	Args     string `json:"args"`
}

// callRecord is the harness's part of a CallOutcome: the call's result
// message and window effect once it has one, its park, the OnCall layers it
// completed, and every shared-state write its runs made, in order.
type callRecord struct {
	Result *message.Message `json:"result,omitempty"`
	Effect ToolResultEffect `json:"effect,omitempty"`
	Park   *callPark        `json:"park,omitempty"`
	Layers []onCallLayer    `json:"layers,omitempty"`
	Writes callWrites       `json:"writes,omitzero"`
}

// RecordResult is the result text in an outcome record (CallOutcome.Record):
// what the call's tool-result event carried. A record without a result,
// a park or a wait, has none.
func RecordResult(record []byte) (string, error) {
	var rec struct {
		Result struct {
			Content string `json:"content"`
		} `json:"result"`
	}
	err := json.Unmarshal(record, &rec)
	return rec.Result.Content, err
}

// record decodes o's harness part; an outcome without one has none. A
// worker that does not register the interrupt type of a park fails it.
func record(o *CallOutcome) (callRecord, error) {
	var rec callRecord
	if o == nil || len(o.Record) == 0 {
		return rec, nil
	}
	if err := json.Unmarshal(o.Record, &rec); err != nil {
		return rec, fmt.Errorf("tool call %s outcome: %w", o.Call.Key(), err)
	}
	return rec, nil
}

// callWrites is the shared session state one call changed, in order. A
// Tool step never saves it; Commit applies each call's writes in call order,
// so calls that ran at once all land, and a later write of a key wins.
type callWrites struct {
	State       []stateWrite      `json:"state,omitempty"`
	Permissions []permissionWrite `json:"permissions,omitempty"`
	Rests       []restWrite       `json:"rests,omitempty"`
	Plan        []planOp          `json:"plan,omitempty"`
}

// stateWrite sets a user-state key, or deletes it.
type stateWrite struct {
	Key     string          `json:"key"`
	Value   json.RawMessage `json:"value,omitempty"`
	Deleted bool            `json:"deleted,omitempty"`
}

type permissionWrite struct {
	Tool     string             `json:"tool"`
	Decision permissionDecision `json:"decision"`
}

type restWrite struct {
	Key  string     `json:"key"`
	Rest brain.Rest `json:"rest"`
}
