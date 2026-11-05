package tacklr_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/vfs"
)

type basicArgs struct {
	Name string `json:"name" desc:"The name"`
	Age  int    `json:"age" desc:"The age"`
}

// TestNewTool_runsAHandlerOfEachShape: a tool's result is a string as it
// is, any other value as JSON; its arguments decode into its struct or a
// pointer to one, with or without a context before them. Arguments that
// do not decode, a result that does not encode, a handler error, a handler
// past the tool's timeout or its own deadline, and the end of the step's
// context are each the call's result.
func TestNewTool_runsAHandlerOfEachShape(t *testing.T) {
	t.Parallel()
	// release frees the handler that does not watch its context, when the
	// test ends.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	tools := []*tacklr.Tool{
		tacklr.NewTool(tacklr.ToolConfig{Name: "text", Handler: func(context.Context) (string, error) { return "hello", nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "bare", Handler: func() (string, error) { return "plain", nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "argsOnly", Handler: func(a basicArgs) (string, error) { return a.Name, nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "number", Handler: func(context.Context) (int, error) { return 42, nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "args", Handler: func(_ context.Context, a basicArgs) (string, error) { return fmt.Sprintf("%s %d", a.Name, a.Age), nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "pointer", Handler: func(_ context.Context, a *basicArgs) (string, error) { return a.Name, nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "channel", Handler: func(context.Context) (chan int, error) { return make(chan int), nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "fails", Handler: func(context.Context, basicArgs) (string, error) { return "", errors.New("boom") }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "inner", Timeout: time.Minute, Handler: func(ctx context.Context) (string, error) {
			short, cancel := context.WithTimeout(ctx, time.Nanosecond)
			defer cancel()
			<-short.Done()
			return "", short.Err()
		}}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "ignores", Timeout: 5 * time.Millisecond, Handler: func(context.Context) (string, error) { time.Sleep(50 * time.Millisecond); return "late", nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "waits", Timeout: time.Minute, Handler: func(context.Context) (string, error) { <-release; return "", nil }}),
	}
	model, _ := scripted(t)
	h := newHarness(t, "shapes", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: tools})
	ended, cancel := context.WithCancel(t.Context())
	cancel()
	cases := []struct {
		ctx              context.Context
		name, args, want string
	}{
		{t.Context(), "text", "", "hello"},
		{t.Context(), "bare", "", "plain"},
		{t.Context(), "argsOnly", `{"name":"solo"}`, "solo"},
		{t.Context(), "number", "", "42"},
		{t.Context(), "args", `{"name":"test","age":10}`, "test 10"},
		{t.Context(), "pointer", `{"name":"x"}`, "x"},
		{t.Context(), "args", `{"name":123}`, "unmarshal args"},
		{t.Context(), "args", `{bad`, "args: arguments were not valid JSON"},
		{t.Context(), "channel", "", "marshal result"},
		{t.Context(), "fails", `{"name":"x","age":1}`, "fails: boom"},
		{t.Context(), "inner", "", "inner timed out"},
		{t.Context(), "ignores", "", "ignores timed out"},
		{ended, "waits", "", "context canceled"},
	}
	for _, tc := range cases {
		c, err := h.RunCall(tc.ctx, tacklr.NewCall(call("c", tc.name, tc.args), 0), newStream(t).out)
		if err != nil || !c.Done || !strings.Contains(c.Body.Text(), tc.want) {
			t.Errorf("%s(%s) = %q %v, want %q", tc.name, tc.args, c.Body.Text(), err, tc.want)
		}
	}
}

// TestToolsAsJson_describesEachToolAsAStrictFunction: each tool is a
// strict function named by its qualified name; its arguments' schema names
// each field with its type, description and enum, lists every field as
// required, makes an optional field nullable, flattens an embedded struct,
// treats a time as a string, and stops a recursive type at a fixed depth.
// A tool without arguments takes an empty object, and no tools are [].
// TypeToJSONSchema, for structured output, keeps an optional field
// optional.
func TestToolsAsJson_describesEachToolAsAStrictFunction(t *testing.T) {
	t.Parallel()
	type Inner struct {
		Value string `json:"value"`
	}
	type node struct {
		Weight   float64 `json:"weight"`
		Children []node  `json:"children"`
	}
	type args struct {
		Inner
		Field    string         `json:"field_name" desc:"A field" enum:"a,b,c"`
		Optional *string        `json:"optional,omitempty"`
		Skipped  string         `json:"-"`
		Hidden   string         `schema:"-"`
		private  string         //nolint:unused // unexported fields are not arguments
		Plain    bool           // no json tag: the Go name
		Items    []string       `json:"items"`
		Lookup   map[string]int `json:"lookup"`
		When     time.Time      `json:"when"`
		Tree     node           `json:"tree"`
	}
	raw := tacklr.ToolsAsJson([]*tacklr.Tool{
		tacklr.NewTool(tacklr.ToolConfig{Name: "get", Description: "Get it", Handler: func(context.Context, args) (string, error) { return "", nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "greet", Namespace: "ado", Handler: func(context.Context) (string, error) { return "", nil }}),
	})
	var defs []struct {
		Type, Name, Description string
		Strict                  bool
		Parameters              schema
	}

	err := json.Unmarshal([]byte(raw), &defs)

	if err != nil || len(defs) != 2 || defs[0].Type != "function" || defs[0].Name != "get" || defs[0].Description != "Get it" || !defs[0].Strict || defs[1].Name != "ado__greet" {
		t.Fatalf("tools = %s %v, want two strict functions by qualified name", raw, err)
	}
	p := defs[0].Parameters
	want := []string{"Plain", "field_name", "items", "lookup", "optional", "tree", "value", "when"}
	if got := slices.Sorted(maps.Keys(p.Properties)); !slices.Equal(got, want) || !slices.Equal(slices.Sorted(slices.Values(p.Required)), want) || p.AdditionalProperties != false {
		t.Fatalf("properties %v, required %v, want %v for both", got, p.Required, want)
	}
	props := p.Properties
	if f := props["field_name"]; f.Type != "string" || f.Description != "A field" || !slices.Equal(f.Enum, []string{"a", "b", "c"}) ||
		fmt.Sprint(props["optional"].Type) != "[string null]" || props["Plain"].Type != "boolean" || props["when"].Type != "string" ||
		props["items"].Items.Type != "string" || props["lookup"].AdditionalProperties.(map[string]any)["type"] != "integer" ||
		props["tree"].Properties["weight"].Type != "number" {
		t.Fatalf("properties = %+v", props)
	}
	depth := 0
	for n := props["tree"]; n.Properties["children"].Items != nil && n.Properties["children"].Items.Type == "object"; n = *n.Properties["children"].Items {
		depth++
	}
	if depth == 0 || depth > 10 {
		t.Fatalf("recursive depth = %d, want nested objects that stop", depth)
	}
	if g := defs[1].Parameters; g.Type != "object" || len(g.Properties) != 0 || tacklr.ToolsAsJson(nil) != "[]" {
		t.Fatalf("no arguments = %+v, want an empty object; no tools %s", g, tacklr.ToolsAsJson(nil))
	}
	type hostOut struct {
		Title string  `json:"title"`
		Note  *string `json:"note,omitempty"`
	}
	out, err := tacklr.TypeToJSONSchema(hostOut{})
	if _, nilErr := tacklr.TypeToJSONSchema(nil); err != nil || !slices.Equal(out["required"].([]string), []string{"title"}) || out["properties"].(map[string]any)["note"].(map[string]any)["type"] != "string" || nilErr == nil {
		t.Fatalf("structured output schema = %v %v, want note optional and plain; nil = %v", out, err, nilErr)
	}
}

// schema is a JSON schema as a model gets it.
type schema struct {
	Type                 any               `json:"type"`
	Description          string            `json:"description"`
	Enum                 []string          `json:"enum"`
	Properties           map[string]schema `json:"properties"`
	Required             []string          `json:"required"`
	Items                *schema           `json:"items"`
	AdditionalProperties any               `json:"additionalProperties"`
}

// TestNewTool_panicsOnAnInvalidConfig: a tool without a name, a handler of
// the wrong shape, or an OnCall that is nil or parks on an unregistered
// type is a programming error.
func TestNewTool_panicsOnAnInvalidConfig(t *testing.T) {
	t.Parallel()
	ok := func(context.Context) (string, error) { return "", nil }
	for name, tc := range map[string]struct {
		cfg  tacklr.ToolConfig
		want string
	}{
		"no name":                       {tacklr.ToolConfig{Handler: ok}, "name is required"},
		"not a function":                {tacklr.ToolConfig{Name: "t", Handler: "not a func"}, "must be a function"},
		"one result":                    {tacklr.ToolConfig{Name: "t", Handler: func() string { return "" }}, "must return"},
		"a second result not an error":  {tacklr.ToolConfig{Name: "t", Handler: func() (string, string) { return "", "" }}, "must return"},
		"arguments not a struct":        {tacklr.ToolConfig{Name: "t", Handler: func(context.Context, string) (string, error) { return "", nil }}, "must be a struct"},
		"too many parameters":           {tacklr.ToolConfig{Name: "t", Handler: func(context.Context, basicArgs, basicArgs, basicArgs) (string, error) { return "", nil }}, "too many parameters"},
		"a parameter not a CallContext": {tacklr.ToolConfig{Name: "t", Handler: func(context.Context, basicArgs, string) (string, error) { return "", nil }}, "want *CallContext"},
		"a nil OnCall":                  {tacklr.ToolConfig{Name: "t", Handler: ok, OnCall: []tacklr.OnCallFunc{nil}}, "must not be nil"},
		"an unregistered OnCall":        {tacklr.ToolConfig{Name: "t", Handler: ok, OnCall: []tacklr.OnCallFunc{func(tacklr.ToolInvocation) tacklr.Interrupt { return hostChoice{} }}}, "not registered"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if r := recover(); r == nil || !strings.Contains(fmt.Sprint(r), tc.want) {
					t.Fatalf("panic = %v, want %q", r, tc.want)
				}
			}()
			tacklr.NewTool(tc.cfg)
		})
	}
}

// TestCorrection_isTheModelsToCorrect: a correction is ErrCorrection and
// its cause, with its own text, or the cause's without the vfs prefix, or
// ErrCorrection's when it has neither; a correction of a correction is the
// first. A network mark keeps the error's text, a second mark keeps it
// too, and a mark of no error is no error.
func TestCorrection_isTheModelsToCorrect(t *testing.T) {
	t.Parallel()
	empty := tacklr.Correction(nil, "  ")
	fromCause := tacklr.Correction(vfs.ErrNotExist, "")
	first := tacklr.Correctionf(vfs.ErrUseHTML, "write %s, not plain text", "HTML")
	again := tacklr.Correction(first, "other")
	network := tacklr.Network(vfs.ErrNotExist)
	marks := []error{tacklr.Network(network), tacklr.Network(nil)}

	if !errors.Is(empty, tacklr.ErrCorrection) || empty.Error() != tacklr.ErrCorrection.Error() {
		t.Fatalf("empty = %v", empty)
	}
	if !errors.Is(fromCause, tacklr.ErrCorrection) || !errors.Is(fromCause, vfs.ErrNotExist) || fromCause.Error() != "not found" {
		t.Fatalf("from its cause = %q", fromCause)
	}
	if again.Error() != "write HTML, not plain text" || !errors.Is(again, vfs.ErrUseHTML) {
		t.Fatalf("already a correction = %v", again)
	}
	if !errors.Is(network, tacklr.ErrNetwork) || !errors.Is(network, vfs.ErrNotExist) || network.Error() != vfs.ErrNotExist.Error() ||
		!errors.Is(marks[0], tacklr.ErrNetwork) || marks[0].Error() != network.Error() || marks[1] != nil {
		t.Fatalf("network marks = %v, then %v, want one mark that keeps the text", network, marks)
	}
}

// TestRunCall_aToolsOutcomeIsItsCallsResult: a tool's failure, a tool that
// is not registered, and an interceptor's change are each the call's
// result in the window; a tool whose credentials expired parks the call on
// auth_expired. A tool's progress updates name the call and never block it.
// An outcome that installs the plan document fails its round's Commit when
// the session has no plan.
func TestRunCall_aToolsOutcomeIsItsCallsResult(t *testing.T) {
	t.Parallel()
	emitted := make(chan struct{})
	tools := []*tacklr.Tool{
		tacklr.NewTool(tacklr.ToolConfig{Name: "boom", Handler: func(context.Context) (string, error) {
			return "", fmt.Errorf("upstream: the search provider failed: %w", tacklr.ErrFailed)
		}}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "expired", Handler: func(context.Context) (string, error) {
			return "", fmt.Errorf("token refresh: %w", tacklr.ErrAuthExpired)
		}}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "big", Handler: func(context.Context) (string, error) { return "very long output", nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "needs_call", Handler: func(context.Context, *tacklr.CallContext) (string, error) { return "ran", nil }}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "install", Handler: func(context.Context) (tacklr.ToolOutcome, error) {
			return tacklr.ToolOutcome{Output: "installed", Effect: tacklr.EffectInstallPlanDocument}, nil
		}}),
		tacklr.NewTool(tacklr.ToolConfig{Name: "progress", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
			cc.EmitUpdate("half way")
			cc.EmitUpdate("dropped: the stream is full")
			close(emitted)
			return "done", nil
		}}),
	}
	capBig := func(ctx context.Context, inv tacklr.ToolInvocation, next tacklr.ToolCallFunc) (tacklr.ToolOutcome, error) {
		if inv.Tool.Name() == "needs_call" {
			inv.CallContext = nil
		}
		out, err := next(ctx, inv)
		if inv.Tool.Name() == "big" {
			out.Output, out.Effect = out.Output[:4], tacklr.EffectHandoff
		}
		return out, err
	}
	model, _ := scripted(t, []tacklr.LLMResponseChunk{text("the handoff")})
	h := newHarness(t, "outcomes", tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: tools, ToolInterceptors: []tacklr.ToolInterceptor{capBig}})
	prompt(t, h, "goal")
	full := make(chan session.Event, 1)
	progress := make(chan tacklr.Call)
	go func() {
		c, _ := h.RunCall(t.Context(), tacklr.NewCall(call("p1", "progress", "{}"), 0), full)
		progress <- c
	}()
	<-emitted
	update := <-full
	resultEvent := <-full

	failed, failErr := runRound(t, h, call("b1", "boom", "{}"), call("x1", "missing", "{}"))
	window := checkpoint(t, h).window
	parked, parkErr := h.RunCall(t.Context(), tacklr.NewCall(call("e1", "expired", "{}"), 0), newStream(t).out)
	stripped := result(t, h, call("n1", "needs_call", "{}"))
	capped, cappedErr := runRound(t, h, call("c1", "big", "{}"))
	after := checkpoint(t, h).window
	_, noPlanErr := runRound(t, h, call("i1", "install", "{}"))

	if update.Type != session.EventToolUpdate || update.Content != "half way" || update.MessageID != "p1" || resultEvent.Type != session.EventToolResult || (<-progress).Body.Text() != "done" {
		t.Fatalf("events = %+v then %+v, want the update named by the call, then the result", update, resultEvent)
	}
	if got := contents(window[len(window)-2:]); failErr != nil || !strings.Contains(got[0], "search provider failed") || !strings.Contains(got[1], "missing: not found") ||
		!strings.Contains(failed.Calls[1].Body.Text(), "not a registered tool") {
		t.Fatalf("results %q %v, want the failure and the unknown tool in the window", got, failErr)
	}
	if parkErr != nil || parkType(t, parked) != tacklr.TypeAuthExpired || !strings.Contains(stripped, "requires a CallContext") {
		t.Fatalf("expired = %+v %v, stripped = %q; want an auth_expired park and the missing CallContext named", parked, parkErr, stripped)
	}
	if cappedErr != nil || capped.Calls[0].Body.Text() != "very" || !slices.Equal(contents(after), []string{"goal", "the handoff"}) {
		t.Fatalf("capped = %q %v, window %q; want the cut output and the interceptor's handoff", capped.Calls[0].Body.Text(), cappedErr, contents(after))
	}
	if noPlanErr == nil || !strings.Contains(noPlanErr.Error(), "no plan document") {
		t.Fatalf("install without a plan = %v, want the commit to fail for want of a plan document", noPlanErr)
	}
}

// TestLLMResponseChunk_anErrorChunkFails: an error chunk fails with its
// error, or with its text when it has none; any other chunk does not fail.
func TestLLMResponseChunk_anErrorChunkFails(t *testing.T) {
	t.Parallel()
	cause := errors.New("cause")

	got := []error{
		tacklr.LLMResponseChunk{Type: session.EventError, Error: cause}.Err(),
		tacklr.LLMResponseChunk{Type: session.EventError, Content: "provider said no"}.Err(),
		tacklr.LLMResponseChunk{Type: session.EventError}.Err(),
		tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "hi"}.Err(),
	}

	if !errors.Is(got[0], cause) || got[1].Error() != "provider said no" || got[2].Error() != "model stream failed" || got[3] != nil {
		t.Fatalf("errors = %v", got)
	}
}
