package tacklr

import (
	"encoding/json"
	"fmt"
	"slices"
)

// openRound is a model reply that called tools, and the results that have
// arrived. It stays out of the window until each call has a result; then it
// commits as one round. The checkpoint keeps it, so a park, a resume, or a
// blocking specialist can span steps.
type openRound struct {
	// Reply is the reply's reasoning and text messages, then the message
	// that carries its calls.
	Reply []*Message `json:"reply"`
	// Calls are the reply's calls in call order.
	Calls []roundCall `json:"calls"`
	// Effect is the window effect the round's results asked for. It applies
	// when the round commits.
	Effect ToolResultEffect `json:"effect,omitempty"`
}

// roundCall is one call of an open round: its result once it has one, and
// everything the call needs while it waits for one. It is the only store of
// a call's park and its completed OnCall layers, so they go when the round
// commits.
type roundCall struct {
	Call   ToolCall `json:"call"`
	Result *Message `json:"result,omitempty"`
	// Park is the interrupt the call waits on.
	Park *callPark `json:"park,omitempty"`
	// Layers are the OnCall layers the call completed; its next run skips
	// them.
	Layers []onCallLayer `json:"layers,omitempty"`
	// AwaitID is the blocking specialist the call waits on.
	AwaitID string `json:"awaitId,omitempty"`
}

// callPark is a call's interrupt. Answered is set by resume; the call's next
// run takes the answered interrupt and continues.
type callPark struct {
	Interrupt Interrupt
	Answered  bool
}

// callParkJSON is a callPark as the checkpoint holds it: the interrupt's
// type names the registered type its data decodes into.
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

// stepReport is what Tools step Seq reported. The checkpoint keeps it, so a
// retry of the step, after its first attempt saved, reports the same.
type stepReport struct {
	Seq   int        `json:"seq,omitempty"`
	Steps []ToolStep `json:"steps,omitempty"`
}

func newOpenRound(reply []*Message, calls []ToolCall) *openRound {
	r := &openRound{Reply: reply, Calls: make([]roundCall, len(calls))}
	for i, tc := range calls {
		r.Calls[i] = roundCall{Call: tc}
	}
	return r
}

// call is the call keyed key, or nil when the round has no such call.
func (r *openRound) call(key string) *roundCall {
	if r == nil {
		return nil
	}
	i := slices.IndexFunc(r.Calls, func(c roundCall) bool { return c.Call.Key() == key })
	if i < 0 {
		return nil
	}
	return &r.Calls[i]
}

// runnable is the indexes, in call order, of the calls that run next: no
// result, not waiting on an unanswered park, not waiting on a child.
func (r *openRound) runnable() []int {
	var out []int
	for i, c := range r.Calls {
		if c.Result == nil && (c.Park == nil || c.Park.Answered) && c.AwaitID == "" {
			out = append(out, i)
		}
	}
	return out
}

// complete reports whether each call has a result.
func (r *openRound) complete() bool {
	return !slices.ContainsFunc(r.Calls, func(c roundCall) bool { return c.Result == nil })
}

// messages is the round as the window holds it: the reply, then each
// result in call order.
func (r *openRound) messages() []*Message {
	out := slices.Clone(r.Reply)
	for _, c := range r.Calls {
		out = append(out, c.Result)
	}
	return out
}

// layer is the call's completed OnCall layer of typeName.
func (c *roundCall) layer(typeName string) (onCallLayer, bool) {
	i := slices.IndexFunc(c.Layers, func(l onCallLayer) bool { return l.TypeName == typeName })
	if i < 0 {
		return onCallLayer{}, false
	}
	return c.Layers[i], true
}

// merge is the effect of two results of one round: installing a plan wins
// over a handoff.
func (e ToolResultEffect) merge(o ToolResultEffect) ToolResultEffect {
	return max(e, o)
}
