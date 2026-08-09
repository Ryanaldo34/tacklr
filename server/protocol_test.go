package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ryanaldo34/tacklr/streaming"
)

// healthProtocol is a minimal Protocol used only to prove transport mounts
// arbitrary protocol routes without ACP/SSE knowledge.
type healthProtocol struct{}

func (healthProtocol) Name() string { return "health" }

func (healthProtocol) HandleInbound(ctx context.Context, env ProtocolEnv, body []byte) error {
	return nil
}

func (healthProtocol) HTTPRoutes() []HTTPRoute {
	return []HTTPRoute{{
		Method:  "GET",
		Pattern: "/healthz",
		Handler: func(env ProtocolEnv, w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		},
	}}
}

func (healthProtocol) OnStreamEvent(ctx context.Context, env ProtocolEnv, threadID string, stream *EventStream, ev streaming.StreamEvent, reqID json.RawMessage) StreamControl {
	return StreamControl{Finished: true}
}

func (healthProtocol) OnStreamClosed(ctx context.Context, env ProtocolEnv, threadID string, reqID json.RawMessage, cancelled bool) error {
	return nil
}

func TestServer_mountsArbitraryProtocolRoutes(t *testing.T) {
	reg := NewRegistry(testStore(t), "")
	srv := NewServer(reg, healthProtocol{})

	mux := http.NewServeMux()
	for _, p := range srv.Protocols {
		for _, route := range p.HTTPRoutes() {
			r := route
			mux.HandleFunc(r.Method+" "+r.Pattern, func(w http.ResponseWriter, req *http.Request) {
				r.Handler(ProtocolEnv{Registry: reg, Conn: &Conn{}}, w, req)
			})
		}
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Fatalf("body = %q, want ok", rec.Body.String())
	}
}

func TestACPProtocol_initializeResultShape(t *testing.T) {
	result := acpInitializeResult(nil, 1)
	if result["protocolVersion"] != 1 {
		t.Fatalf("protocolVersion = %v", result["protocolVersion"])
	}
	// Client asks for a future major; we respond with the latest we support (1).
	if v := acpInitializeResult(nil, 99)["protocolVersion"]; v != 1 {
		t.Fatalf("negotiated version for client 99 = %v, want 1", v)
	}
	caps, ok := result["agentCapabilities"].(map[string]any)
	if !ok {
		t.Fatal("missing agentCapabilities")
	}
	mcpCaps, ok := caps["mcpCapabilities"].(map[string]any)
	if !ok || mcpCaps["http"] != true {
		t.Fatalf("mcpCapabilities = %v", mcpCaps)
	}
	pc, ok := caps["promptCapabilities"].(map[string]any)
	if !ok || pc["image"] != false {
		t.Fatalf("nil registry should advertise image=false, got %v", pc)
	}
	if pc["embeddedContext"] != true || pc["audio"] != false {
		t.Fatalf("promptCapabilities = %v", pc)
	}
	info, ok := result["agentInfo"].(map[string]string)
	if !ok || info["name"] == "" {
		t.Fatalf("agentInfo = %v", result["agentInfo"])
	}
}

func (healthProtocol) CreateSession(context.Context, ProtocolEnv, json.RawMessage) (string, any, error) {
	return "", nil, ErrWireSessionUnsupported
}
func (healthProtocol) LoadSession(context.Context, ProtocolEnv, string, json.RawMessage) (any, error) {
	return nil, ErrWireSessionUnsupported
}
func (healthProtocol) BindTurn(context.Context, ProtocolEnv, string, json.RawMessage) (TurnRequest, error) {
	return TurnRequest{}, ErrWireSessionUnsupported
}
func (healthProtocol) CloseSession(context.Context, ProtocolEnv, string) error {
	return ErrWireSessionUnsupported
}
