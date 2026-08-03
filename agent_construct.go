package tacklr

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/google/uuid"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/exa"
	session "github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/skills"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

// Config is harness limits and prompt settings.
type Config struct {
	MaxWindowSize    int
	SystemPrompt     string
	SkillDirectories []string
	// MaxTurnRequests limits Model.Invoke calls per Run. 0 = unlimited.
	// Exceeding the limit ends the turn with ErrMaxTurnRequests.
	MaxTurnRequests int
}

// AgentOptions configures NewAgent and NewAgentFromSession.
//
// Usual fields: Config, Model, Store, Tools, MCPConfigs, SubAgents, SessionID.
// ContextManager, ModelTasks, and ContextPolicy override the built-in ACM path;
// leave them nil unless you replace that path.
type AgentOptions struct {
	Config Config
	// SessionID is the durable thread id. Set at construction; do not change mid-turn.
	SessionID  string
	Model      InferenceStrategy
	Store      stores.BaseStore
	WatchDog   AgentWatchDog
	Tools      []*Tool
	MCPConfigs []mcp.MCPConfig
	SubAgents  []*SubAgent
	// ContextManager is the conversation window. Nil uses NewModelContextManager.
	ContextManager ContextManager
	// ModelTasks runs Turn, Absorb, and Handoff. Nil uses DefaultModelTasks.
	ModelTasks ModelTasks
	// ContextPolicy sets pressure/compress ratios when non-zero fields are set.
	ContextPolicy ContextPolicy
	// ToolInterceptors wrap each tool call (outermost first).
	// Nil: built-in planning lock and permission gate.
	// Non-nil: replaces that chain (empty slice disables interceptors).
	ToolInterceptors []ToolInterceptor
	// ToolResultHooks map tool name → post-success window effects for host tools.
	// Plan builtins use BuiltinResult instead.
	ToolResultHooks map[string]ToolResultHook
	// SkillsLoader loads skills from Config.SkillDirectories.
	// Nil uses skills.DirectoryLoader.
	SkillsLoader skills.Loader
	// ExaAPIKey enables web_search and web_fetch. Empty falls back to EXA_API_KEY.
	// When both are empty, those tools are not registered.
	ExaAPIKey string
	// Brain enables knowledge builtins when non-nil. Workers inherit the same engine.
	// Configure Store, optional QueryEmbedder, and optional GraphReader on the Engine
	// before NewAgent (e.g. brain.WithGraph(helixgraph.New(...))). The harness does
	// not construct graph backends.
	Brain *brain.Engine
	// SearchNamespace isolates brain retrieval when set (session-owned, checkpointed).
	// Nil leaves a loaded session value unchanged. Workers get a copy at spawn.
	SearchNamespace *uuid.UUID
}

// streamEventBuffer is the harness event channel size so EmitUpdate is not dropped
// when the consumer lags briefly.
const streamEventBuffer = 64

// NewAgent builds a harness. out is a non-nil sentinel so Run can detect init;
// the live event bus is installed on Run.
func NewAgent(ctx context.Context, opts AgentOptions) *AgentHarness {
	events := make(chan streaming.StreamEvent, streamEventBuffer)
	sm := session.NewSessionManager()
	runtime := session.NewRuntime(nil, opts.Store, sm)
	h := newHarnessBase(opts, runtime, sm, events)
	if opts.SessionID != "" {
		h.sessionId = opts.SessionID
	}
	h.finishInit(ctx, opts.SubAgents)
	return h
}

// newHarnessBase fills shared fields. sm and runtime must share one SessionManager.
func newHarnessBase(opts AgentOptions, runtime HarnessRuntime, sm *session.SessionManager, out chan streaming.StreamEvent) *AgentHarness {
	if sm == nil {
		sm = session.NewSessionManager()
	}
	h := &AgentHarness{
		model:                opts.Model,
		maxWindowSize:        opts.Config.MaxWindowSize,
		maxTurnRequests:      opts.Config.MaxTurnRequests,
		instructions:         opts.Config.SystemPrompt,
		store:                opts.Store,
		runtime:              runtime,
		session:              sm,
		watchDog:             opts.WatchDog,
		tools:                opts.Tools,
		mcpConfigs:           opts.MCPConfigs,
		skillDirectories:     opts.Config.SkillDirectories,
		skillsLoader:         opts.SkillsLoader,
		exaAPIKey:            resolveExaAPIKey(opts.ExaAPIKey),
		brain:                opts.Brain,
		sessionId:            "",
		subagents:            make(map[string]*SubAgent),
		interruptToRequester: make(map[string]string),
		pendingToolCalls:     make(map[string]stores.PendingToolCall),
		interruptPayloads:    make(map[string][]byte),
		parkedWorkersLive:    make(map[string]*AgentHarness),
		out:                  out,
		context:              opts.ContextManager,
		tasks:                opts.ModelTasks,
		contextPolicy:        opts.ContextPolicy,
	}
	if opts.Brain != nil {
		h.searchCtx = brain.NewSearchContext()
	}
	if opts.SearchNamespace != nil {
		sm.SetSearchNamespace(*opts.SearchNamespace)
	}
	if h.context == nil {
		h.context = NewModelContextManager()
	}
	if h.contextPolicy.PressureRatio <= 0 && h.contextPolicy.CompressFraction <= 0 {
		h.contextPolicy = DefaultContextPolicy()
	}
	if h.tasks == nil {
		h.tasks = NewDefaultModelTasks(h.model, h.context, h.contextPolicy, h.maxWindowSize)
	}
	if opts.ToolInterceptors != nil {
		h.toolRunner = newToolRunner(opts.ToolInterceptors...)
	} else {
		h.toolRunner = newToolRunner(h.planningWriteLock, toolPermissionGate)
	}
	h.toolResultHooks = newToolResultHookRegistry(opts.ToolResultHooks)
	return h
}

func (h *AgentHarness) finishInit(ctx context.Context, subAgents []*SubAgent) {
	h.initMCP(ctx)
	if err := h.initSkills(); err != nil {
		slog.Error("failed to initialize skills", "error", err)
	}
	h.initSubAgentWorkers(subAgents)
	h.injectBuiltinTools()
}

// injectBuiltinTools registers plan tools, optional web/brain tools, and spawn_worker once.
func (a *AgentHarness) injectBuiltinTools() {
	if a.builtinsInjected {
		return
	}
	if a.session == nil {
		a.session = session.NewSessionManager()
	}
	s := internalSession{
		sm: a.session,
		emitPlanTodos: func(plan []streaming.Todo) {
			session.EmitPlanUpdate(&a.runtime, plan)
		},
	}
	a.tools = append(a.tools,
		newCreatePlanTool(s),
		newEditPlanTool(s),
		newCompleteTodoTool(s),
		newListPlanTool(s),
		askUserChoiceTool,
	)
	if key := strings.TrimSpace(a.exaAPIKey); key != "" {
		client := exa.NewClient(key)
		a.tools = append(a.tools, newWebSearchTool(client), newWebFetchTool(client))
	}
	if a.brain != nil && a.searchCtx != nil {
		a.tools = append(a.tools, newBrainTools(a.brain, a.session, a.searchCtx)...)
	}
	if len(a.subagents) > 0 {
		a.tools = append(a.tools, a.spawnTool())
	}
	a.builtinsInjected = true
}

// SetSearchNamespace sets session retrieval isolation for knowledge tools.
func (a *AgentHarness) SetSearchNamespace(id uuid.UUID) {
	if a.session == nil {
		a.session = session.NewSessionManager()
	}
	a.session.SetSearchNamespace(id)
}

// ClearSearchNamespace clears session retrieval isolation.
func (a *AgentHarness) ClearSearchNamespace() {
	if a.session == nil {
		return
	}
	a.session.ClearSearchNamespace()
}

// SearchNamespace returns the host-set search namespace, if any.
func (a *AgentHarness) SearchNamespace() (uuid.UUID, bool) {
	if a.session == nil {
		return uuid.UUID{}, false
	}
	return a.session.SearchNamespace()
}

// planningWriteLock blocks write tools until create_plan has set a plan.
func (a *AgentHarness) planningWriteLock(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
	if inv.Tool != nil && inv.Tool.Access != nil && inv.Tool.Access.Contains(WritePermission) &&
		(a.session == nil || !a.session.HasActivePlan()) {
		return "", fmt.Errorf("%w: write tools are locked until create_plan establishes a todo list", ErrToolPermissionDenied)
	}
	return next(ctx, inv)
}

func (a *AgentHarness) initSkills() error {
	if a.skillsInitialized {
		return nil
	}
	loader := a.skillsLoader
	if loader == nil {
		loader = skills.DirectoryLoader{}
	}
	loaded, err := loader.Load(a.skillDirectories)
	if err != nil {
		return err
	}
	a.skillByName = make(map[string]skills.Skill, len(loaded))
	for _, skill := range loaded {
		a.skillByName[skill.Name] = skill
	}
	if len(loaded) > 0 {
		a.tools = append(a.tools, a.skillTool())
	}
	a.skillsInitialized = true
	return nil
}

func (a *AgentHarness) skillTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "read_skill",
		Description: "Load the full instructions for an available skill.",
		Handler: func(ctx context.Context, args struct {
			Name string `json:"name" desc:"Skill name from the available skills catalog"`
		}) (string, error) {
			skill, ok := a.skillByName[args.Name]
			if !ok {
				return "", fmt.Errorf("unknown skill %q", args.Name)
			}
			return skill.Instructions, nil
		},
	})
}

// NewAgentFromSession loads a harness from a session checkpoint.
// opts.Store is required. Uses the same AgentOptions shape as NewAgent.
func NewAgentFromSession(ctx context.Context, sessionId string, opts AgentOptions) (*AgentHarness, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("agent harness: store is required to load session %q", sessionId)
	}
	events := make(chan StreamEvent, streamEventBuffer)
	checkpoint, err := opts.Store.LoadSession(ctx, sessionId)
	if err != nil {
		return nil, err
	}
	sm := session.NewSessionManager()
	applied, err := session.NewCheckpointer().Apply(checkpoint, sm)
	if err != nil {
		return nil, err
	}
	runtime := session.NewRuntime(nil, opts.Store, sm)
	h := newHarnessBase(opts, runtime, sm, events)
	h.sessionId = sessionId
	h.context.Restore(applied.Window)
	h.interruptToRequester = applied.InterruptToRequester
	h.pendingToolCalls = applied.PendingToolCalls
	if h.searchCtx != nil && len(checkpoint.State.SearchContext) > 0 {
		if err := h.searchCtx.Restore(checkpoint.State.SearchContext); err != nil {
			return nil, fmt.Errorf("agent harness: restore search context: %w", err)
		}
	}
	h.finishInit(ctx, opts.SubAgents)
	return h, nil
}
