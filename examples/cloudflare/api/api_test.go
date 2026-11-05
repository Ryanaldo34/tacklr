package api_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/examples/cloudflare/api"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
	"github.com/ryanaldo34/tacklr/session/durableobject/dotest"
)

const token = "secret"

// server is the API over durable object sessions in memory, whose model
// calls the confirm tool and then answers with its result.
func server(t *testing.T) (*httptest.Server, *dotest.Platform) {
	t.Helper()
	model := testkit.HTTPModel(t, func(_ context.Context, msgs []*message.Message, _ []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := msgs[len(msgs)-1]
		if last.Role == message.RoleTool {
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done: " + last.Content, IsComplete: true}
			return
		}
		id := fmt.Sprintf("call-%d", len(msgs))
		ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{{ID: id, CallID: id, Name: "confirm", Arguments: `{}`}}}
	})
	confirm := tacklr.NewTool(tacklr.ToolConfig{Name: "confirm", Handler: func(_ context.Context, cc *tacklr.CallContext) (string, error) {
		answer, err := cc.Park(&tacklr.UserSelectionInterrupt{Question: "Ship?", Options: []tacklr.UserChoice{{Title: "Ship"}}})
		if err != nil {
			return "", err
		}
		return "chose " + answer.(*tacklr.UserSelectionInterrupt).ConfirmedChoice.Title, nil
	}})
	sealer, err := session.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	p := dotest.New(durableobject.Config{Agent: tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{confirm}}, Sealer: sealer, RetryBackoff: time.Millisecond})
	srv := httptest.NewServer(api.Handler(durableobject.NewClient(p), token))
	t.Cleanup(srv.Close)
	return srv, p
}

// call sends one request with the token and returns its status and body.
func call(t *testing.T, srv *httptest.Server, method, path, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// events reads id's event stream from the start until an event of type
// until.
func events(t *testing.T, srv *httptest.Server, id string, until session.EventType) []session.Event {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/sessions/"+id+"/events?after=0", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var got []session.Event
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		var ev session.Event
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			t.Fatal(err)
		}
		got = append(got, ev)
		if ev.Type == until {
			return got
		}
	}
	t.Fatalf("stream ended after %+v: %v", got, sc.Err())
	return nil
}

// TestAPI_aClientRunsAParkedTurnToItsEndAndClosesTheSession: a client
// creates a session, prompts it, reads its events to the park, resumes it
// with the answer and reads the turn's end, sees the turn complete in the
// status and no jobs, cancels with nothing running, and closes the session,
// which is then gone.
func TestAPI_aClientRunsAParkedTurnToItsEndAndClosesTheSession(t *testing.T) {
	t.Parallel()
	srv, _ := server(t)
	created, body := call(t, srv, http.MethodPost, "/sessions", "")
	var out struct{ ID string }
	_ = json.Unmarshal([]byte(body), &out)
	prompted, _ := call(t, srv, http.MethodPost, "/sessions/"+out.ID+"/prompt", `{"text":"ship it"}`)
	parked := events(t, srv, out.ID, session.EventInterrupt)
	park := parked[len(parked)-1]
	resumed, _ := call(t, srv, http.MethodPost, "/sessions/"+out.ID+"/resume", `{"responses":{"`+park.MessageID+`":{"selectionIdx":0}}}`)
	ended := events(t, srv, out.ID, session.EventComplete)
	statusCode, status := call(t, srv, http.MethodGet, "/sessions/"+out.ID, "")
	jobsCode, jobs := call(t, srv, http.MethodGet, "/sessions/"+out.ID+"/jobs", "")
	cancelled, _ := call(t, srv, http.MethodPost, "/sessions/"+out.ID+"/cancel", "")
	closed, _ := call(t, srv, http.MethodDelete, "/sessions/"+out.ID, "")
	gone, _ := call(t, srv, http.MethodGet, "/sessions/"+out.ID, "")

	if created != http.StatusCreated || out.ID == "" || prompted != http.StatusAccepted || resumed != http.StatusAccepted {
		t.Fatalf("create %d %q, prompt %d, resume %d; want the session made and the wakes taken", created, body, prompted, resumed)
	}
	if park.Type != session.EventInterrupt || ended[len(ended)-1].Type != session.EventComplete {
		t.Fatalf("parked on %+v, ended with %+v; want the park, then the end of the turn", park, ended[len(ended)-1])
	}
	if statusCode != http.StatusOK || !strings.Contains(status, `"Result":"done: chose Ship"`) || jobsCode != http.StatusOK || strings.TrimSpace(jobs) != "[]" {
		t.Fatalf("status %d %s, jobs %d %s; want the answer and no jobs", statusCode, status, jobsCode, jobs)
	}
	if cancelled != http.StatusAccepted || closed != http.StatusNoContent || gone != http.StatusNotFound {
		t.Fatalf("cancel %d, close %d, then status %d; want the session closed and gone", cancelled, closed, gone)
	}
}

// TestAPI_refusesRequestsItCannotServe: a request without the token, with
// another, or to a server with no token is unauthorized; a body or a
// position it cannot read is a bad request; an id in use conflicts; an
// unknown session is not found; and storage that is down is a server
// error.
func TestAPI_refusesRequestsItCannotServe(t *testing.T) {
	t.Parallel()
	srv, p := server(t)
	open := httptest.NewServer(api.Handler(nil, ""))
	t.Cleanup(open.Close)
	unauthorized := func(srv *httptest.Server, header string) int {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/sessions/x", nil)
		if err != nil {
			t.Fatal(err)
		}
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if code, _ := call(t, srv, http.MethodPost, "/sessions", `{"id":"taken"}`); code != http.StatusCreated {
		t.Fatalf("create: %d", code)
	}
	p.FailWrites("taken", func([]durableobject.Op) bool { return true })

	got := map[string]int{
		"no token":        unauthorized(srv, ""),
		"another token":   unauthorized(srv, "Bearer other"),
		"no server token": unauthorized(open, "Bearer "),
	}
	for name, req := range map[string][3]string{
		"bad body":       {http.MethodPost, "/sessions", "{"},
		"bad prompt":     {http.MethodPost, "/sessions/taken/prompt", "{"},
		"bad resume":     {http.MethodPost, "/sessions/taken/resume", "{"},
		"bad after":      {http.MethodGet, "/sessions/taken/events?after=x", ""},
		"taken":          {http.MethodPost, "/sessions", `{"id":"taken"}`},
		"unknown events": {http.MethodGet, "/sessions/nope/events?after=0", ""},
		"outage":         {http.MethodPost, "/sessions/taken/prompt", `{"text":"hi"}`},
	} {
		got[name], _ = call(t, srv, req[0], req[1], req[2])
	}

	want := map[string]int{
		"no token": http.StatusUnauthorized, "another token": http.StatusUnauthorized, "no server token": http.StatusUnauthorized,
		"bad body": http.StatusBadRequest, "bad prompt": http.StatusBadRequest, "bad resume": http.StatusBadRequest, "bad after": http.StatusBadRequest,
		"taken": http.StatusConflict, "unknown events": http.StatusNotFound, "outage": http.StatusInternalServerError,
	}
	for name, code := range want {
		if got[name] != code {
			t.Fatalf("%s: %d; want %d", name, got[name], code)
		}
	}
}
