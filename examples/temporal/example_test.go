package example_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/examples/backend"
	"github.com/ryanaldo34/tacklr/examples/temporal/agent"
	"github.com/ryanaldo34/tacklr/internal/pgtest"
	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	"github.com/ryanaldo34/tacklr/sandbox/sandboxtest"
	"github.com/ryanaldo34/tacklr/session"
	sessionpg "github.com/ryanaldo34/tacklr/session/postgres"
	"github.com/ryanaldo34/tacklr/session/temporal"
	"github.com/ryanaldo34/tacklr/session/temporal/worker"
)

func TestMain(m *testing.M) {
	code := m.Run()
	temporaldocker.Stop()
	os.Exit(code)
}

const token = "operator-token"

// deployment is the example as it runs in production, in one test: a
// worker and a backend on the shared Temporal server, over the Postgres
// stores, wired as the programs in cmd wire them.
type deployment struct {
	api       string // the backend's URL
	workspace string // WORKSPACE_DIR
}

func deploy(t *testing.T, model tacklr.Model) deployment {
	t.Helper()
	ctx := t.Context()
	pool, err := pgxpool.New(ctx, pgtest.Conn(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	db, err := sessionpg.New(pool, sessionpg.Options{Schema: pgtest.Schema(t)})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Setup(ctx); err != nil {
		t.Fatal(err)
	}
	sealer, err := session.NewAESGCM(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	events := db.EventLog()
	t.Cleanup(events.Close)
	c, err := temporal.Dial(client.Options{HostPort: temporaldocker.HostPort(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	queue, liveKey, dir := "example-"+uuid.NewString(), []byte("live-key"), t.TempDir()

	// The worker, as cmd/worker runs it.
	var live http.Handler
	liveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { live.ServeHTTP(w, r) }))
	t.Cleanup(liveSrv.Close)
	opts := agent.Options(model)
	opts.MaxWindowSize = 100_000
	w := worker.New(c, worker.Config{
		Agent:          opts,
		Sandbox:        sandboxtest.Client(t, agent.Tree(dir)),
		TaskQueue:      queue,
		Store:          db.Store(),
		Secrets:        db.Secrets(sealer),
		Fallback:       events,
		DisableStreams: true,
		Live:           worker.Live{URL: liveSrv.URL, Key: liveKey},
	})
	live = w.LiveHandler()
	stop, done := make(chan any), make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Run(stop)
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})

	// The backend, as cmd/backend runs it.
	sessions := temporal.Open(c, temporal.Config{
		TaskQueue:      queue,
		Store:          db.Store(),
		Secrets:        db.Secrets(sealer),
		Fallback:       events,
		DisableStreams: true,
		Live:           temporal.Live{Key: liveKey},
	})
	api := httptest.NewServer(backend.New(sessions, token))
	t.Cleanup(api.Close)
	return deployment{api: api.URL, workspace: dir}
}

// operator is a program of the operator's that drives one session through
// the backend: it follows the session's events and sends its prompts.
type operator struct {
	t      *testing.T
	d      deployment
	id     string
	events chan session.Event
}

func open(t *testing.T, d deployment) *operator {
	t.Helper()
	o := &operator{t: t, d: d, events: make(chan session.Event, 256)}
	var created struct{ ID string }
	if err := json.Unmarshal([]byte(o.send(http.MethodPost, "/sessions", "", http.StatusCreated)), &created); err != nil {
		t.Fatal(err)
	}
	o.id = created.ID
	resp := o.request(http.MethodGet, "/sessions/"+o.id+"/events", "")
	go func() {
		defer resp.Body.Close()
		lines := bufio.NewScanner(resp.Body)
		lines.Buffer(nil, 1<<20)
		for lines.Scan() {
			if data, ok := strings.CutPrefix(lines.Text(), "data: "); ok {
				var ev session.Event
				if json.Unmarshal([]byte(data), &ev) == nil {
					o.events <- ev
				}
			}
		}
	}()
	return o
}

func (o *operator) request(method, path, body string) *http.Response {
	o.t.Helper()
	req, err := http.NewRequestWithContext(o.t.Context(), method, o.d.api+path, strings.NewReader(body))
	if err != nil {
		o.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		o.t.Fatal(err)
	}
	return resp
}

// send sends a request, checks its status and returns its body.
func (o *operator) send(method, path, body string, status int) string {
	o.t.Helper()
	resp := o.request(method, path, body)
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != status {
		o.t.Fatalf("%s %s = %d %s, want %d", method, path, resp.StatusCode, out, status)
	}
	return string(out)
}

func (o *operator) prompt(text string) {
	o.t.Helper()
	body, _ := json.Marshal(map[string]string{"text": text})
	o.send(http.MethodPost, "/sessions/"+o.id+"/prompt", string(body), http.StatusAccepted)
}

// turn reads events until the turn ends or waits on the operator, and
// returns that event and the text of the agent's messages before it.
func (o *operator) turn() (session.Event, string) {
	o.t.Helper()
	var said strings.Builder
	for {
		select {
		case ev := <-o.events:
			switch ev.Type {
			case session.EventComplete, session.EventError, session.EventInterrupt:
				return ev, said.String()
			case session.EventMessage:
				if !ev.Delta {
					said.WriteString(ev.Content)
				}
			}
		case <-time.After(30 * time.Second):
			o.t.Fatal("the turn did not end in 30 seconds")
		}
	}
}

// releases is the model of a releases assistant: it looks a release up
// and answers from the result; asked for a note, it makes a plan, then
// writes the note with run_command.
func releases(t *testing.T) tacklr.Model {
	return testkit.HTTPModel(t, func(_ context.Context, msgs []*message.Message, _ []string, ch chan<- tacklr.LLMResponseChunk) {
		last := msgs[len(msgs)-1]
		call := func(name, args string) {
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true,
				ToolCalls: []message.ToolCall{{ID: "fc_" + name, CallID: "call_" + name, Name: name, Arguments: args}}}
		}
		switch {
		case tacklr.IsPlanDocument(last):
			call("run_command", `{"command":"printf 'v2 shipped' > work/notes.txt"}`)
		case last.Role == message.RoleTool && strings.Contains(last.Content, "shipped"):
			// The answer streams as deltas, then the message completes.
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "v2 has "}
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "shipped."}
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, IsComplete: true}
		case last.Role == message.RoleTool:
			ch <- tacklr.LLMResponseChunk{Type: session.EventMessage, Content: "Saved.", IsComplete: true}
		case last.Content == "status of v2?":
			call("lookup", `{"release":"v2"}`)
		default:
			call("create_plan", `{"plan":"Keep a note of v2.","todos":[{"title":"Write the note","status":"pending","description":"notes.txt says v2 shipped."}]}`)
		}
	})
}

// TestExample_anOperatorAsksAboutAReleaseAndSavesANote: through the
// backend, the operator's question runs the agent's own tool on a worker
// and the answer streams back. For a note the agent plans, then its
// run_command asks the operator first; once allowed, the sandbox writes the
// note into the session's own directory of the workspace.
func TestExample_anOperatorAsksAboutAReleaseAndSavesANote(t *testing.T) {
	t.Parallel()
	// Arrange
	d := deploy(t, releases(t))
	o := open(t, d)

	// Act
	o.prompt("status of v2?")
	answered, said := o.turn()
	o.prompt("save a note")
	asked, _ := o.turn()
	o.send(http.MethodPost, "/sessions/"+o.id+"/resume", fmt.Sprintf(`{"responses":{%q:{"optionId":"allow-once"}}}`, asked.MessageID), http.StatusAccepted)
	saved, _ := o.turn()

	// Assert
	if answered.Type != session.EventComplete || !strings.Contains(said, "v2 has shipped.") {
		t.Fatalf("the question ended %q after %q; want the answer", answered.Type, said)
	}
	note, err := os.ReadFile(filepath.Join(d.workspace, url.PathEscape(o.id), "notes.txt"))
	if asked.Type != session.EventInterrupt || saved.Type != session.EventComplete || err != nil || string(note) != "v2 shipped" {
		t.Fatalf("the note asked %q, then ended %q; the file is %q (%v); want it written once allowed", asked.Type, saved.Type, note, err)
	}
}

// TestExample_anOperatorCancelsAWriteItWasAskedFor: the operator cancels
// the turn while the agent asks to write: the turn ends, the note is not
// written, and the session takes the next question.
func TestExample_anOperatorCancelsAWriteItWasAskedFor(t *testing.T) {
	t.Parallel()
	// Arrange
	d := deploy(t, releases(t))
	o := open(t, d)
	o.prompt("save a note")
	asked, _ := o.turn()

	// Act
	o.send(http.MethodPost, "/sessions/"+o.id+"/cancel", "", http.StatusAccepted)
	cancelled, _ := o.turn()
	o.prompt("status of v2?")
	answered, said := o.turn()

	// Assert
	_, noteErr := os.Stat(filepath.Join(d.workspace, url.PathEscape(o.id), "notes.txt"))
	if asked.Type != session.EventInterrupt || cancelled.Type == session.EventInterrupt || !os.IsNotExist(noteErr) {
		t.Fatalf("asked %q, then the cancel ended %q (note: %v); want the turn ended with no note", asked.Type, cancelled.Type, noteErr)
	}
	if answered.Type != session.EventComplete || !strings.Contains(said, "v2 has shipped.") {
		t.Fatalf("the next question ended %q after %q; want the answer", answered.Type, said)
	}
}
