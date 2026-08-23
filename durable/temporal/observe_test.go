package temporal

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	temporalotel "go.temporal.io/sdk/contrib/opentelemetry-v2"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/durable/inprocess"
	"github.com/ryanaldo34/tacklr/internal/testkit"
	"github.com/ryanaldo34/tacklr/telemetry"
)

func TestObservabilityPlugin_requiresReplaySafe(t *testing.T) {
	prev := otel.GetTracerProvider()
	telemetry.SetTracerProvider(nil)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		telemetry.EnsureReplaySafeProvider()
	})
	if _, err := ObservabilityPlugin(); err == nil {
		t.Fatal("want error when Init has not installed ReplaySafe")
	}
}

func TestObservabilityPlugin_usesProcessProvider(t *testing.T) {
	telemetry.EnsureReplaySafeProvider()
	p, err := ObservabilityPlugin(nil)
	if err != nil || p == nil {
		t.Fatalf("plugin: %v %v", p, err)
	}
}

func TestStartTurn_noopWithoutReplaySafe(t *testing.T) {
	prev := otel.GetTracerProvider()
	telemetry.SetTracerProvider(nil)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		telemetry.EnsureReplaySafeProvider()
	})
	if telemetry.IsReplaySafeProvider() {
		t.Fatal("setup")
	}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{
			InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "ok", IsComplete: true}
			},
		}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "hi"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 20*time.Millisecond)
	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: "s", AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
}

func TestSessionWorkflow_emitsTurnSpanAttrs(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := temporalotel.NewReplaySafeTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: &testkit.ScriptedModel{
			InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "hello-temporal", IsComplete: true}
			},
		}, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))

	id := durable.SessionID("sess-span")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "hi"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 50*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}

	var found sdktrace.ReadOnlySpan
	for _, sp := range sr.Ended() {
		if sp.Name() == telemetry.SpanTurn {
			found = sp
		}
	}
	if found == nil {
		t.Fatalf("want %s span, got %d ended", telemetry.SpanTurn, len(sr.Ended()))
	}
	attrs := map[string]string{}
	for _, a := range found.Attributes() {
		attrs[string(a.Key)] = a.Value.AsString()
	}
	if attrs[telemetry.AttrRuntime] != telemetry.RuntimeTemporal {
		t.Fatalf("runtime %q", attrs[telemetry.AttrRuntime])
	}
	if attrs[telemetry.AttrTurnKind] != telemetry.TurnKindPrompt {
		t.Fatalf("kind %q", attrs[telemetry.AttrTurnKind])
	}
	if attrs[telemetry.AttrAgentID] != "default" || attrs[telemetry.AttrSessionID] != string(id) {
		t.Fatalf("ids %+v", attrs)
	}
	if attrs[telemetry.AttrOutcome] != telemetry.OutcomeOK {
		t.Fatalf("outcome %q", attrs[telemetry.AttrOutcome])
	}
}

func TestSessionWorkflow_yieldThenResumeSpans(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := temporalotel.NewReplaySafeTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetWorkerOptions(worker.Options{EnableSessionWorker: true})
	cat := durable.NewCatalog("default")
	model := &testkit.ScriptedModel{
		InvokeFn: func(ctx context.Context, msgs []*tacklr.Message, tools []*tacklr.Tool, ch chan<- tacklr.LLMResponseChunk) {
			if last := lastMsg(msgs); last != nil && last.Role == tacklr.RoleTool {
				ch <- tacklr.LLMResponseChunk{Type: tacklr.StreamEventMessage, Content: "chose", IsComplete: true}
				return
			}
			ch <- tacklr.LLMResponseChunk{
				Type: tacklr.StreamEventFunctionCall,
				ToolCalls: []tacklr.ToolCall{{
					ID: "ask1", CallID: "ask1", Name: "ask_user_choice",
					Arguments: `{"question":"Pick?","choices":[{"title":"A"},{"title":"B"}]}`,
				}},
				IsComplete: true,
			}
		},
	}
	cat.Register("default", durable.AgentSpec{
		Options: tacklr.AgentOptions{Model: model, Config: tacklr.Config{MaxWindowSize: 8192}},
	})
	fallback := inprocess.NewMemoryEventLog()
	env.RegisterWorkflow(SessionWorkflow)
	env.RegisterActivity(newActs(cat, fallback, true))

	id := durable.SessionID("sess-yield-span")
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalPrompt, promptSignal{Text: "ask"})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalResume, resumeSignal{Responses: map[string][]byte{"ask1": []byte(`{"selectionIdx":0}`)}})
	}, 20*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(signalClose, nil)
	}, 80*time.Millisecond)

	env.ExecuteWorkflow(SessionWorkflow, WorkflowInput{SessionID: id, AgentID: "default"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}

	var kinds, outcomes []string
	for _, sp := range sr.Ended() {
		if sp.Name() != telemetry.SpanTurn {
			continue
		}
		kind, outcome := "", ""
		for _, a := range sp.Attributes() {
			switch string(a.Key) {
			case telemetry.AttrTurnKind:
				kind = a.Value.AsString()
			case telemetry.AttrOutcome:
				outcome = a.Value.AsString()
			}
		}
		kinds = append(kinds, kind)
		outcomes = append(outcomes, outcome)
	}
	if len(kinds) < 2 {
		t.Fatalf("want prompt+resume turns, kinds=%v outcomes=%v ended=%d", kinds, outcomes, len(sr.Ended()))
	}
	sawYield, sawResume := false, false
	for i := range kinds {
		if kinds[i] == telemetry.TurnKindPrompt && outcomes[i] == telemetry.OutcomeYield {
			sawYield = true
		}
		if kinds[i] == telemetry.TurnKindResume && outcomes[i] == telemetry.OutcomeOK {
			sawResume = true
		}
	}
	if !sawYield || !sawResume {
		t.Fatalf("want yield then resume ok, kinds=%v outcomes=%v", kinds, outcomes)
	}
}
