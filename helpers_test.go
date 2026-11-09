package tacklr_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
)

// request is one model request as the provider got it: the system prompt
// is its first message.
type request struct {
	msgs  []*message.Message
	tools []*tacklr.Tool
}

// toolNames are the names the request offers the model, in order.
func (r request) toolNames() []string {
	var names []string
	for _, t := range r.tools {
		names = append(names, t.QualifiedName())
	}
	return names
}

// provider is a model provider on a script: request n gets the n-th reply,
// a request after the last gets "done", and each request is kept.
type provider struct {
	mu       sync.Mutex
	replies  [][]tacklr.LLMResponseChunk
	requests []request
}

func (p *provider) respond(_ context.Context, msgs []*message.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
	p.mu.Lock()
	n := len(p.requests)
	p.requests = append(p.requests, request{msgs: msgs, tools: tools})
	reply := []tacklr.LLMResponseChunk{text("done")}
	if n < len(p.replies) {
		reply = p.replies[n]
	}
	p.mu.Unlock()
	for _, c := range reply {
		ch <- c
	}
}

// sent are the requests the provider got.
func (p *provider) sent() []request {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.requests)
}

// scripted is the real model client on a provider that answers with
// replies, in order.
func scripted(t *testing.T, replies ...[]tacklr.LLMResponseChunk) (tacklr.Model, *provider) {
	t.Helper()
	p := &provider{replies: replies}
	return testkit.HTTPModel(t, p.respond), p
}

// counted is scripted on a provider that counts a request's tokens with
// count. The provider also prices each response with count, so a script
// that fails its n-th count counts those too.
func counted(t *testing.T, count func([]*message.Message) (int, error), replies ...[]tacklr.LLMResponseChunk) (tacklr.Model, *provider) {
	t.Helper()
	p := &provider{replies: replies}
	return testkit.CountedHTTPModel(t, p.respond, count), p
}

// failAfter is windowTokens for n counts, then a count that fails.
func failAfter(n int) func([]*message.Message) (int, error) {
	var mu sync.Mutex
	return func(msgs []*message.Message) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		if n == 0 {
			return 0, errors.New("count failed")
		}
		n--
		return windowTokens(msgs)
	}
}

// windowTokens counts the content length of a request's messages after
// the system prompt as its tokens, so a test sets the window in
// characters of the conversation alone.
func windowTokens(msgs []*message.Message) (int, error) {
	return testkit.ContentTokenEstimate(msgs[min(len(msgs), 1):]), nil
}

func text(s string) tacklr.LLMResponseChunk {
	return tacklr.LLMResponseChunk{Type: session.EventMessage, Content: s, IsComplete: true}
}

func calls(tcs ...message.ToolCall) tacklr.LLMResponseChunk {
	return tacklr.LLMResponseChunk{Type: session.EventFunctionCall, ToolCalls: tcs, IsComplete: true}
}

// failure is a provider that fails the request with err.
func failure(err error) tacklr.LLMResponseChunk {
	return tacklr.LLMResponseChunk{Type: session.EventError, Error: err}
}

func call(id, name, args string) message.ToolCall {
	return message.ToolCall{ID: id, CallID: id, Name: name, Arguments: args}
}

func userMsg(content string) *message.Message {
	return &message.Message{Role: message.RoleUser, Content: content}
}

// newHarness builds the harness of session id from opts.
func newHarness(t *testing.T, id string, opts tacklr.AgentOptions) *tacklr.Harness {
	t.Helper()
	h, err := tacklr.NewHarness(t.Context(), tacklr.BindTurn(opts, id, nil, nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	return h
}

// stream is a step's event channel; events are what went through it.
type stream struct {
	out  chan session.Event
	mu   sync.Mutex
	evs  []session.Event
	stop func()
}

func newStream(t *testing.T) *stream {
	s := &stream{}
	out, stop := tacklr.PipeEvents(func(ev session.Event) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.evs = append(s.evs, ev)
	})
	s.out, s.stop = out, sync.OnceFunc(stop)
	t.Cleanup(s.stop)
	return s
}

// events closes the stream and returns every event it carried.
func (s *stream) events() []session.Event {
	s.stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.evs)
}

// prompt commits a user message.
func prompt(t *testing.T, h *tacklr.Harness, content string) {
	t.Helper()
	if err := h.Commit(t.Context(), tacklr.CommitIn{Users: []*message.Message{userMsg(content)}}, newStream(t).out); err != nil {
		t.Fatal(err)
	}
}

// reply is the reply of a model that asked for tcs.
func reply(tcs ...message.ToolCall) tacklr.Reply {
	return tacklr.Reply{Messages: []*message.Message{{Role: message.RoleAssistant, ToolCalls: tcs}}, Calls: tcs}
}

// runRound runs tcs as the round of one reply on h, as a runtime does:
// each call's Tool step, then the round's Commit. It returns the round and
// the Commit's error.
func runRound(t *testing.T, h *tacklr.Harness, tcs ...message.ToolCall) (*tacklr.Round, error) {
	t.Helper()
	out := newStream(t).out
	r := tacklr.NewRound(reply(tcs...))
	for i, tc := range tcs {
		c, err := h.RunCall(t.Context(), tacklr.NewCall(tc, 0), out)
		if err != nil {
			t.Fatal(err)
		}
		r.Calls[i] = c
	}
	return r, h.Commit(t.Context(), tacklr.CommitIn{Round: r}, out)
}

// result is the result text of one run of tc on h. Nothing commits.
func result(t *testing.T, h *tacklr.Harness, tc message.ToolCall) string {
	t.Helper()
	c, err := h.RunCall(t.Context(), tacklr.NewCall(tc, 0), newStream(t).out)
	if err != nil {
		t.Fatal(err)
	}
	return c.Body.Text()
}

// committed is the session's committed state, as its checkpoint holds it.
type committed struct {
	window []*message.Message
	todos  []tacklr.Todo
	plan   string
	state  map[string]any
}

func checkpoint(t *testing.T, h *tacklr.Harness) committed {
	t.Helper()
	rows, err := h.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	c := committed{state: map[string]any{}}
	for _, r := range rows {
		var err error
		switch r.Kind {
		case "message":
			m := new(message.Message)
			err = json.Unmarshal(r.Value, m)
			c.window = append(c.window, m)
		case "todo":
			var td tacklr.Todo
			err = json.Unmarshal(r.Value, &td)
			c.todos = append(c.todos, td)
		case "plan":
			c.plan = string(r.Value)
		case "state":
			var v any
			err = json.Unmarshal(r.Value, &v)
			c.state[r.Key] = v
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	return c
}

// contents are the messages' contents, in order.
func contents(msgs []*message.Message) []string {
	var out []string
	for _, m := range msgs {
		out = append(out, m.Content)
	}
	return out
}

// containsAll reports whether s contains each of subs.
func containsAll(s string, subs ...string) bool {
	return !slices.ContainsFunc(subs, func(sub string) bool { return !strings.Contains(s, sub) })
}
