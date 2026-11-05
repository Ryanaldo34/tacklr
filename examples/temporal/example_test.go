package example_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/examples/temporal/agent"
	"github.com/ryanaldo34/tacklr/examples/temporal/host"
	"github.com/ryanaldo34/tacklr/internal/pgtest"
	"github.com/ryanaldo34/tacklr/internal/temporaldocker"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/message"
	serverpg "github.com/ryanaldo34/tacklr/server/postgres"
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
// worker and a host on the shared Temporal server, over the Postgres
// stores, wired as the programs in cmd wire them.
type deployment struct {
	acp       string // the host's ws:// URL of /acp
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
	wire := serverpg.NewWireStore(pool, "acp")
	if err := wire.Setup(ctx); err != nil {
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
		Workspace:      agent.Workspace(dir),
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

	// The host, as cmd/host runs it.
	sessions := temporal.Open(c, temporal.Config{
		TaskQueue:      queue,
		Store:          db.Store(),
		Secrets:        db.Secrets(sealer),
		Fallback:       events,
		DisableStreams: true,
		Live:           temporal.Live{Key: liveKey},
	})
	hostSrv := httptest.NewServer(host.New(sessions, model, wire, token).HTTPMux())
	t.Cleanup(hostSrv.Close)
	return deployment{acp: "ws" + strings.TrimPrefix(hostSrv.URL, "http") + "/acp", workspace: dir}
}

// operator is an ACP client on a WebSocket, as an editor is.
type operator struct {
	t    *testing.T
	conn *websocket.Conn
	next int
}

func connect(t *testing.T, d deployment) *operator {
	t.Helper()
	return connectWith(t, d, token)
}

// connectWith connects with bearer, or with no Authorization when it is
// empty.
func connectWith(t *testing.T, d deployment, bearer string) *operator {
	t.Helper()
	header := http.Header{}
	if bearer != "" {
		header.Set("Authorization", "Bearer "+bearer)
	}
	conn, _, err := websocket.Dial(t.Context(), d.acp, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(websocket.StatusNormalClosure, "") })
	o := &operator{t: t, conn: conn}
	o.call("initialize", `{"protocolVersion":1}`, nil)
	return o
}

// call sends one request and reads frames until its response, answering
// the agent's own requests with answer. It returns the response and every
// frame before it.
func (o *operator) call(method, params string, answer func(req map[string]any) any) (map[string]any, string) {
	o.t.Helper()
	o.next++
	id := o.next
	o.write(fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":%q,"params":%s}`, id, method, params))
	var before strings.Builder
	for {
		ctx, cancel := context.WithTimeout(o.t.Context(), 30*time.Second)
		_, data, err := o.conn.Read(ctx)
		cancel()
		if err != nil {
			o.t.Fatalf("%s: read: %v", method, err)
		}
		var frame map[string]any
		if err := json.Unmarshal(data, &frame); err != nil {
			o.t.Fatal(err)
		}
		if frame["method"] != nil && frame["id"] != nil {
			reply, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": frame["id"], "result": answer(frame)})
			o.write(string(reply))
			continue
		}
		if frame["id"] == float64(id) {
			return frame, before.String()
		}
		before.Write(data)
	}
}

func (o *operator) write(body string) {
	o.t.Helper()
	if err := o.conn.Write(o.t.Context(), websocket.MessageText, []byte(body)); err != nil {
		o.t.Fatal(err)
	}
}

// prompt sends text to session sid and returns how the turn stopped and
// what streamed before.
func (o *operator) prompt(sid, text string, answer func(req map[string]any) any) (string, string) {
	o.t.Helper()
	resp, streamed := o.call("session/prompt", `{"sessionId":"`+sid+`","prompt":[{"type":"text","text":"`+text+`"}]}`, answer)
	stop, _ := resp["result"].(map[string]any)["stopReason"].(string)
	return stop, streamed
}

// releases is the model of a releases assistant: it looks a release up
// and answers from the result; asked for a note, it makes a plan (write
// tools wait for one), then writes the note.
func releases(t *testing.T) tacklr.Model {
	return testkit.HTTPModel(t, func(_ context.Context, msgs []*message.Message, _ []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
		last := msgs[len(msgs)-1]
		call := func(name, args string) {
			ch <- tacklr.LLMResponseChunk{Type: session.EventFunctionCall, IsComplete: true,
				ToolCalls: []message.ToolCall{{ID: "fc_" + name, CallID: "call_" + name, Name: name, Arguments: args}}}
		}
		switch {
		case tacklr.IsPlanDocument(last):
			call("write", `{"path":"/workspace/work/notes.txt","content":"v2 shipped"}`)
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

// TestExample_anOperatorAsksAboutAReleaseAndSavesANote: over ACP, the
// operator's question runs the agent's own tool on a worker and the answer
// streams back through the host. For a note the agent plans, then its write
// asks the operator first; once allowed, the note lands in the session's own
// directory of the workspace.
func TestExample_anOperatorAsksAboutAReleaseAndSavesANote(t *testing.T) {
	t.Parallel()
	// Arrange
	d := deploy(t, releases(t))
	o := connect(t, d)
	created, _ := o.call("session/new", `{"cwd":"/tmp"}`, nil)
	sid, _ := created["result"].(map[string]any)["sessionId"].(string)
	var asked []string
	allow := func(req map[string]any) any {
		asked = append(asked, req["method"].(string))
		return map[string]any{"outcome": map[string]any{"outcome": "selected", "optionId": "allow-once"}}
	}

	// Act
	answered, streamed := o.prompt(sid, "status of v2?", nil)
	saved, _ := o.prompt(sid, "save a note", allow)

	// Assert
	if answered != "end_turn" || !strings.Contains(streamed, "v2 has ") || !strings.Contains(streamed, "shipped.") {
		t.Fatalf("the question ended %q after %s; want the answer streamed", answered, streamed)
	}
	note, err := os.ReadFile(filepath.Join(d.workspace, url.PathEscape(sid), "notes.txt"))
	if saved != "end_turn" || len(asked) != 1 || asked[0] != "session/request_permission" || err != nil || string(note) != "v2 shipped" {
		t.Fatalf("the note ended %q after asking %v; the file is %q (%v); want it written once allowed", saved, asked, note, err)
	}
}

// TestExample_theHostRefusesAnOperatorWithoutTheToken: an upgrade with a
// token that is not the host's gets 401, and a connection with no token
// cannot open a session.
func TestExample_theHostRefusesAnOperatorWithoutTheToken(t *testing.T) {
	t.Parallel()
	// Arrange
	d := deploy(t, releases(t))

	// Act
	_, wrong, wrongErr := websocket.Dial(t.Context(), d.acp, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer not-the-token"}}})
	anonymous := connectWith(t, d, "")
	refused, _ := anonymous.call("session/new", `{"cwd":"/tmp"}`, nil)

	// Assert
	if wrongErr == nil || wrong == nil || wrong.StatusCode != http.StatusUnauthorized {
		t.Fatalf("a wrong token = %v (%v), want 401", wrong, wrongErr)
	}
	if msg, _ := refused["error"].(map[string]any)["message"].(string); msg != "authentication required" {
		t.Fatalf("session/new with no token = %v, want authentication required", refused)
	}
}
