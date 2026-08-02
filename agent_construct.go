package tacklr

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ryanaldo34/tacklr/internal/exa"
	session "github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/skills"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
)

type Config struct {
	MaxWindowSize    int
	SystemPrompt     string
	SkillDirectories []string
	// MaxTurnRequests caps Model.Invoke calls within a single Run turn.
	// 0 means unlimited. When exceeded, the turn ends with ErrMaxTurnRequests.
	MaxTurnRequests int
}

// AgentOptions configures a new agent harness via NewAgent or NewAgentFromSession.
type AgentOptions struct {
	Config Config
	// SessionID binds this harness to a durable thread/session id (registry sets this).
	SessionID  string
	Model      InferenceStrategy
	Store      stores.BaseStore
	WatchDog   AgentWatchDog
	Tools      []*Tool
	MCPConfigs []mcp.MCPConfig
	SubAgents  []*SubAgent
	// ContextManager owns the conversation window (nil → NewModelContextManager).
	ContextManager ContextManager
	// ModelTasks runs Turn/Absorb/Handoff (nil → DefaultModelTasks from Model + context).
	ModelTasks ModelTasks
	// ContextPolicy overrides default pressure/compress ratios when non-zero fields are set.
	ContextPolicy ContextPolicy
	// ToolInterceptors wrap every harness tool call (outermost first).
	// nil uses the built-in chain (planning write lock, permission gate).
	// A non-nil slice replaces that chain entirely; empty disables interceptors.
	ToolInterceptors []ToolInterceptor
	// ToolResultHooks map tool names to post-success context effects for host tools.
	// Plan builtins return BuiltinResult instead of relying on name-keyed hooks.
	// nil (or empty) means no host hooks.
	ToolResultHooks map[string]ToolResultHook
	// SkillsLoader discovers skills from Config.SkillDirectories.
	// nil uses skills.DirectoryLoader (filesystem SKILL.md trees).
	SkillsLoader skills.Loader
	// ExaAPIKey enables the built-in web_search tool. When empty, EXA_API_KEY
	// from the process environment is used. When both are empty, web_search
	// is not registered.
	ExaAPIKey string
}

// streamEventBuffer sizes the harness event bus so non-blocking EmitUpdate
// (progress, worker status) is not dropped when the consumer is briefly behind.
const streamEventBuffer = 64

func NewAgent(ctx context.Context, opts AgentOptions) *AgentHarness {
	// Runtime output channel is nil until Run; plan mutations before Run only update SessionManager.
	// a.out is a non-nil sentinel so Run can detect an initialized harness.
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

// newHarnessBase builds the shared AgentHarness fields for NewAgent and
// NewAgentFromSession. sm and runtime must share the same SessionManager backend.
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

// finishInit runs one-time skill/MCP/subagent/builtin setup shared by constructors.
func (h *AgentHarness) finishInit(ctx context.Context, subAgents []*SubAgent) {
	h.initMCP(ctx)
	if err := h.initSkills(); err != nil {
		slog.Error("failed to initialize skills", "error", err)
	}
	h.initSubAgentWorkers(subAgents)
	h.injectBuiltinTools()
}

// injectBuiltinTools appends plan tools and spawn_worker (when subagents are
// registered) exactly once. Safe to call from NewAgent and Run.
// Plan tools close over SessionManager; they do not use Runtime for plan state.
// EmitPlanUpdate is only for streaming todo-list updates to clients.
func (a *AgentHarness) injectBuiltinTools() {
	if a.builtinsInjected {
		return
	}
	if a.session == nil {
		a.session = session.NewSessionManager()
	}
	s := internalSession{
		sm:            a.session,
		emitPlanTodos: a.runtime.EmitPlanUpdate,
	}
	a.tools = append(a.tools,
		newCreatePlanTool(s),
		newEditPlanTool(s),
		newCompleteTodoTool(s),
		newListPlanTool(s),
		askUserChoiceTool,
	)
	if key := strings.TrimSpace(a.exaAPIKey); key != "" {
		a.tools = append(a.tools, newWebSearchTool(exa.NewClient(key)))
	}
	if len(a.subagents) > 0 {
		a.tools = append(a.tools, a.spawnTool())
	}
	a.builtinsInjected = true
}

// planningWriteLock denies tools that require Write access while no plan exists.
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

// NewAgentFromSession restores a harness from a stored session checkpoint using
// the same AgentOptions shape as NewAgent.
func NewAgentFromSession(ctx context.Context, sessionId string, opts AgentOptions) (*AgentHarness, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("agent harness: store is required to load session %q", sessionId)
	}
	events := make(chan StreamEvent)
	checkpoint, err := opts.Store.LoadSession(ctx, sessionId)
	if err != nil {
		return nil, err
	}
	sm := session.NewSessionManager()
	applied, err := session.NewCheckpointer().Apply(checkpoint, sm)
	if err != nil {
		return nil, err
	}
	// Same as NewAgent: output channel is nil until Run.
	runtime := session.NewRuntime(nil, opts.Store, sm)
	h := newHarnessBase(opts, runtime, sm, events)
	h.sessionId = sessionId
	h.context.Restore(applied.Window)
	h.interruptToRequester = applied.InterruptToRequester
	h.pendingToolCalls = applied.PendingToolCalls
	h.finishInit(ctx, opts.SubAgents)
	return h, nil
}
