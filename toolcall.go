package tacklr

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// Call is one tool call of a reply, through its whole life: what the model
// asked, what its runs did, and the input of its next run. It is the one
// value that travels: the session keeps it in its open round, a runtime
// hands it to the harness to run (Harness.RunCall) and gets it back, and
// Commit applies it once the round is complete.
//
// A run ends in one of three ways: the call has its result (Done), it parks
// on an interrupt the user answers (Park), or it waits on a job (Await).
// Body is the harness's own part; a runtime may keep it elsewhere between
// runs (in its session store, by Key), so what it keeps in its history
// stays small.
type Call struct {
	Tool    message.ToolCall `json:"tool"`
	Timeout time.Duration    `json:"timeout,omitempty"`

	Done  bool   `json:"done,omitempty"`
	Park  *Park  `json:"park,omitempty"`
	Await string `json:"await,omitempty"`
	// Starts and Cancels are the jobs the call's last run started and
	// stopped; the session starts and stops them.
	Starts  []JobStart `json:"starts,omitempty"`
	Cancels []string   `json:"cancels,omitempty"`

	// The next run's input, which the session sets: the answer to the
	// call's park, the result of the job it waits on, and the session's
	// running jobs with how many it may run at once (zero: no limit). A run
	// clears them.
	Answer  []byte             `json:"answer,omitempty"`
	JobEnd  *session.JobResult `json:"jobEnd,omitempty"`
	Jobs    []string           `json:"jobs,omitempty"`
	MaxJobs int                `json:"maxJobs,omitempty"`

	Body *CallBody `json:"body,omitempty"`

	// Err is why the call's step failed, when it did: not the tool's own
	// failure, which is the call's result. It does not travel.
	Err error `json:"-"`
}

// NewCall is a call of a reply that has not run.
func NewCall(tool message.ToolCall, timeout time.Duration) Call {
	return Call{Tool: tool, Timeout: timeout}
}

// Key is the call's id.
func (c Call) Key() string { return c.Tool.Key() }

// Parked reports whether the call waits on its park.
func (c Call) Parked() bool { return !c.Done && c.Park != nil }

// Waits reports whether the call waits on a job.
func (c Call) Waits() bool { return !c.Done && c.Await != "" }

// Pending reports whether the call has not run, or its run counts for
// nothing yet (another call of its reply parked first).
func (c Call) Pending() bool { return !c.Done && c.Park == nil && c.Await == "" }

// Park is a call's interrupt as the client sees it. InterruptID is the call
// id; Resume answers it by that id.
type Park struct {
	InterruptID string `json:"interruptId"`
	Data        []byte `json:"data,omitempty"`
}

// CallBody is the harness's part of a call: its result message and the
// window effect it asks for, its park, the OnCall layers it passed, and the
// shared state its runs wrote, in order. Only the harness reads it; a
// runtime moves it as JSON.
type CallBody struct {
	result *message.Message
	effect ToolResultEffect
	park   *callPark
	layers []onCallLayer
	writes callWrites
}

// Text is the call's result text: what its tool-result event carried.
func (b *CallBody) Text() string {
	if b == nil || b.result == nil {
		return ""
	}
	return b.result.Content
}

// ResultText is the result text in an encoded body: what the call's
// tool-result event carried. It reads nothing else, so a reader that does
// not run the harness (a session client) needs none of its types.
func ResultText(body []byte) (string, error) {
	var b struct {
		Result struct {
			Content string `json:"content"`
		} `json:"result"`
	}
	err := json.Unmarshal(body, &b)
	return b.Result.Content, err
}

type callBodyJSON struct {
	Result *message.Message `json:"result,omitempty"`
	Effect ToolResultEffect `json:"effect,omitempty"`
	Park   *callPark        `json:"park,omitempty"`
	Layers []onCallLayer    `json:"layers,omitempty"`
	Writes callWrites       `json:"writes,omitzero"`
}

// MarshalJSON implements json.Marshaler.
func (b CallBody) MarshalJSON() ([]byte, error) {
	return json.Marshal(callBodyJSON{Result: b.result, Effect: b.effect, Park: b.park, Layers: b.layers, Writes: b.writes})
}

// UnmarshalJSON implements json.Unmarshaler. A park of an interrupt type
// this process does not register fails it.
func (b *CallBody) UnmarshalJSON(raw []byte) error {
	var j callBodyJSON
	if err := json.Unmarshal(raw, &j); err != nil {
		return err
	}
	*b = CallBody{result: j.Result, effect: j.Effect, park: j.Park, layers: j.Layers, writes: j.Writes}
	return nil
}

// layer is the call's passed OnCall layer of typeName.
func (b *CallBody) layer(typeName string) (onCallLayer, bool) {
	i := slices.IndexFunc(b.layers, func(l onCallLayer) bool { return l.TypeName == typeName })
	if i < 0 {
		return onCallLayer{}, false
	}
	return b.layers[i], true
}

// callPark is a call's interrupt. Answered is set by resume; the call's run
// takes the answered interrupt and continues.
type callPark struct {
	Interrupt Interrupt
	Answered  bool
}

// callParkJSON is a callPark as a body holds it: the interrupt's type names
// the registered type its data decodes into.
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
