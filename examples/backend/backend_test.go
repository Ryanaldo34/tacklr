package backend_test

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

	"github.com/stretchr/testify/require"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/examples/backend"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
	"github.com/ryanaldo34/tacklr/session/durableobject/dotest"
)

const token = "t0ken"

// api serves the backend over in-memory session objects whose agent
// answers "hello" with a greeting, and "deploy" by calling deploy, which
// asks for permission.
func api(t *testing.T) (*httptest.Server, *dotest.Platform) {
	t.Helper()
	model := testkit.HTTPModel(t, func(_ context.Context, msgs []*message.Message, _ []string, ch chan<- tacklr.LLMResponseChunk) {
		last := msgs[len(msgs)-1]
		switch {
		case last.Role == message.RoleTool:
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "done: " + last.Content, IsComplete: true}
		case strings.Contains(last.Content, "deploy"):
			id := fmt.Sprintf("call-%d", len(msgs))
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true, ToolCalls: []message.ToolCall{{ID: id, CallID: id, Name: "deploy", Arguments: "{}"}}}
		default:
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "Hello from Tacklr", IsComplete: true}
		}
	})
	deploy := tacklr.NewTool(tacklr.ToolConfig{Name: "deploy", Description: "Deploys the change.",
		OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(context.Context) (string, error) { return "deployed", nil }})
	sealer, err := session.NewAESGCM(make([]byte, 32))
	require.NoError(t, err)
	platform := dotest.NewWire(durableobject.Config{
		Agent:        tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{deploy}},
		Sealer:       sealer,
		RetryBackoff: time.Millisecond,
	})
	server := httptest.NewServer(backend.New(durableobject.NewClient(platform), token))
	t.Cleanup(server.Close)
	return server, platform
}

// call sends a request with the token and returns its status and body.
func call(t *testing.T, server *httptest.Server, method, path, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(out)
}

// create makes a session and returns its id.
func create(t *testing.T, server *httptest.Server) string {
	t.Helper()
	status, body := call(t, server, http.MethodPost, "/sessions", "")
	require.Equal(t, http.StatusCreated, status, body)
	var created struct{ ID string }
	require.NoError(t, json.Unmarshal([]byte(body), &created))
	return created.ID
}

// follow opens the session's event stream and returns its events as they
// come.
func follow(t *testing.T, server *httptest.Server, id string) <-chan session.Event {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL+"/sessions/"+id+"/events", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))
	events := make(chan session.Event, 64)
	go func() {
		defer resp.Body.Close()
		defer close(events)
		lines := bufio.NewScanner(resp.Body)
		lines.Buffer(nil, 1<<20)
		for lines.Scan() {
			data, ok := strings.CutPrefix(lines.Text(), "data: ")
			if !ok {
				continue
			}
			var ev session.Event
			if json.Unmarshal([]byte(data), &ev) == nil {
				events <- ev
			}
		}
	}()
	return events
}

// until reads events until one of kind, and returns it.
func until(t *testing.T, events <-chan session.Event, kind session.EventType) session.Event {
	t.Helper()
	for {
		select {
		case ev, ok := <-events:
			require.True(t, ok, "the event stream ended before a %s event", kind)
			if ev.Type == kind && !ev.Delta {
				return ev
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("no %s event in 10 seconds", kind)
		}
	}
}

func TestAPromptStreamsTheAgentsAnswer(t *testing.T) {
	server, _ := api(t)
	id := create(t, server)
	events := follow(t, server, id)

	status, _ := call(t, server, http.MethodPost, "/sessions/"+id+"/prompt", `{"text":"hello"}`)
	answer := until(t, events, session.EventMessage)

	require.Equal(t, http.StatusAccepted, status)
	require.Equal(t, "Hello from Tacklr", answer.Content)
}

func TestAResumeAnswersThePermissionTheAgentAsksFor(t *testing.T) {
	server, _ := api(t)
	id := create(t, server)
	events := follow(t, server, id)
	call(t, server, http.MethodPost, "/sessions/"+id+"/prompt", `{"text":"deploy"}`)
	asked := until(t, events, session.EventInterrupt)

	status, _ := call(t, server, http.MethodPost, "/sessions/"+id+"/resume", fmt.Sprintf(`{"responses":{%q:{"optionId":"allow-once"}}}`, asked.MessageID))
	result := until(t, events, session.EventToolResult)

	require.Equal(t, http.StatusAccepted, status)
	require.Contains(t, result.Content, "deployed")
}

func TestACancelEndsATurnThatWaitsForPermission(t *testing.T) {
	server, _ := api(t)
	id := create(t, server)
	events := follow(t, server, id)
	call(t, server, http.MethodPost, "/sessions/"+id+"/prompt", `{"text":"deploy"}`)
	until(t, events, session.EventInterrupt)

	status, _ := call(t, server, http.MethodPost, "/sessions/"+id+"/cancel", "")

	require.Equal(t, http.StatusAccepted, status)
}

func TestTheBackendRefusesWhatItCannotServe(t *testing.T) {
	server, platform := api(t)
	id := create(t, server)
	platform.FailWrites(session.ID(id), func([]durableobject.Op) bool { return true })
	cases := []struct {
		name, method, path, body string
		status                   int
	}{
		{"a body that is not JSON", http.MethodPost, "/sessions/" + id + "/prompt", "{", http.StatusBadRequest},
		{"a resume that is not JSON", http.MethodPost, "/sessions/" + id + "/resume", "[", http.StatusBadRequest},
		{"an after that is not a number", http.MethodGet, "/sessions/" + id + "/events?after=x", "", http.StatusBadRequest},
		{"a session that is not there", http.MethodPost, "/sessions/missing/prompt", `{"text":"hi"}`, http.StatusNotFound},
		{"the events of a session that is not there", http.MethodGet, "/sessions/missing/events", "", http.StatusNotFound},
		{"the events after a seq of a session that is not there", http.MethodGet, "/sessions/missing/events?after=0", "", http.StatusNotFound},
		{"a session whose storage is down", http.MethodPost, "/sessions/" + id + "/prompt", `{"text":"hi"}`, http.StatusInternalServerError},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, body := call(t, server, c.method, c.path, c.body)

			require.Equal(t, c.status, status, body)
		})
	}
}

func TestTheBackendRefusesARequestWithoutTheToken(t *testing.T) {
	server, _ := api(t)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/sessions", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer wrong")

	resp, err := server.Client().Do(req)

	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}
