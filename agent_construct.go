package tacklr

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/internal/exa"
	session "github.com/ryanaldo34/tacklr/internal/session"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/skills"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// Config is harness limits and prompt settings.
type Config struct {
	MaxWindowSize int
	SystemPrompt  string
	// MaxTurnRequests limits Model.Invoke calls per Run. 0 = unlimited.
	// Exceeding the limit ends the turn with ErrMaxTurnRequests.
	MaxTurnRequests int
}

// Validate checks host configuration that does not depend on a model.
func (c Config) Validate() error {
	if c.MaxWindowSize < 0 {
		return fmt.Errorf("tacklr: Config.MaxWindowSize must be positive")
	}
	if c.MaxTurnRequests < 0 {
		return fmt.Errorf("tacklr: Config.MaxTurnRequests must not be negative")
	}
	return nil
}

// AgentOptions configures NewAgent.
//
// Usual fields: Config, Model, Tools, MCPConfigs, Specialists, SessionID.
// ContextPolicy knobs (ratios, stream-summary) stay host-settable. Adaptive
// Case Management itself is harness-owned and cannot be replaced.
//
// Conversation for durable.Runtime sessions lives on SnapshotStore.
// Wire session envelopes (server.ProtocolWireStore) are a separate protocol
// contract.
type AgentOptions struct {
	Config Config
	// SessionID is the durable thread id. Set at construction; do not change mid-turn.
	SessionID  string
	Model      InferenceStrategy
	WatchDog   AgentWatchDog
	Tools      []*Tool
	MCPConfigs []mcp.MCPConfig
	// MCPCredentialResolver resolves durable references immediately before
	// connection. Inline client credentials remain session-scoped.
	MCPCredentialResolver mcp.CredentialResolver
	Specialists           []*Specialist
	// ContextPolicy sets pressure/compress ratios when non-zero fields are set.
	ContextPolicy ContextPolicy
	// ToolInterceptors wrap each tool call (outermost first). Built-in
	// planning lock and OnCall middleware are installed after these.
	ToolInterceptors []ToolInterceptor
	// disablePlanningLock omits planningWriteLock (workers and tests).
	// The permission gate is still always installed.
	disablePlanningLock bool
	// writeUnattended injects write without ToolPermissionOnCall.
	// Write-mechanic tests use this so persist/index paths do not park.
	writeUnattended bool
	// ToolResultHooks map tool name → post-success window effects for host tools.
	// Plan builtins use ToolOutcome instead.
	ToolResultHooks map[string]ToolResultHook
	// SkillsLoader loads skills. Nil uses skills.Loader on the /skills mount
	// when MountSession has one (from SkillSource factories).
	SkillsLoader skills.SkillLoader
	// ExaAPIKey enables web_search and web_fetch. Empty falls back to EXA_API_KEY.
	// When both are empty, those tools are not registered.
	ExaAPIKey string
	// Brain enables knowledge builtins when non-nil. Workers inherit the same engine.
	// Configure Store, optional QueryEmbedder, and optional GraphReader/GraphWriter on the Engine
	// before NewAgent (e.g. brain.WithGraph(helixgraph.New(...))). The harness does
	// not construct graph backends.
	Brain *brain.Engine
	// BrainWriteKinds maps save_discovery / save_fact / save_memory to host kind names.
	// Empty fields skip that tool. Kinds should be registered via brain.ApplyKinds / WithKinds.
	// Ignored when Brain is nil.
	BrainWriteKinds brain.WriteKinds
	// SearchNamespace isolates brain retrieval when set (session-owned, checkpointed).
	// Nil leaves a loaded session value unchanged. Workers get a copy at spawn.
	SearchNamespace *uuid.UUID
	// MountSession is the VFS tree injected for this turn, or nil (no VFS tools).
	// Runtime builds one from FSBootstrap plus Prompt.Auth bindings when a
	// projection is available. Embedders pass their own. The injector Closes
	// it after the turn; the harness never does (workers inherit the pointer).
	MountSession *vfs.MountSession
	// runCommandUnattended injects run_command without ToolPermissionOnCall.
	// Zero value parks run_command for permission. Unexported so hosts cannot
	// disable the permission park from AgentOptions.
	runCommandUnattended bool
	// shareIndexBridge is the parent index bridge. Nil means Start a new bridge.
	shareIndexBridge *vfsindex.Bridge
}

// streamEventBuffer is the harness event channel size so EmitUpdate is not dropped
// when the consumer lags briefly.
const streamEventBuffer = 64

// NewAgent builds a session-scoped harness. Turn-scoped Runtime is created in Run.
func NewAgent(ctx context.Context, opts AgentOptions) (*AgentHarness, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	sm := session.NewSessionManager()
	h := &AgentHarness{
		model:                 opts.Model,
		maxWindowSize:         opts.Config.MaxWindowSize,
		maxTurnRequests:       opts.Config.MaxTurnRequests,
		instructions:          opts.Config.SystemPrompt,
		session:               sm,
		watchDog:              opts.WatchDog,
		tools:                 opts.Tools,
		mcpConfigs:            opts.MCPConfigs,
		mcpCredentialResolver: opts.MCPCredentialResolver,
		skillsLoader:          opts.SkillsLoader,
		hostInterceptors:      slices.Clone(opts.ToolInterceptors),
		hostResultHooks:       maps.Clone(opts.ToolResultHooks),
		exaAPIKey:             resolveExaAPIKey(opts.ExaAPIKey),
		brain:                 opts.Brain,
		brainWriteKinds:       opts.BrainWriteKinds,
		sessionId:             opts.SessionID,
		specialists:           make(map[string]*Specialist),
		pendingToolCalls:      make(map[string]stores.PendingToolCall),
		interruptPayloads:     make(map[string][]byte),
		parkedWorkersLive:     make(map[string]*AgentHarness),
		jobs:                  make(map[string]*workerRun),
		context:               newModelContextManager(),
		contextPolicy:         opts.ContextPolicy,
		runCommandUnattended:  opts.runCommandUnattended,
		writeUnattended:       opts.writeUnattended,
		vfsBridge:             opts.shareIndexBridge,
	}
	h.jobsCtx, h.jobsCancel = context.WithCancel(context.Background())
	if opts.MountSession != nil {
		sm.VFS = opts.MountSession
	}
	if opts.SearchNamespace != nil {
		sm.Search.SetNamespace(*opts.SearchNamespace)
	}
	def := DefaultContextPolicy()
	if h.contextPolicy.PressureRatio <= 0 {
		h.contextPolicy.PressureRatio = def.PressureRatio
	}
	if h.contextPolicy.CompressFraction <= 0 {
		h.contextPolicy.CompressFraction = def.CompressFraction
	}
	h.tasks = newDefaultModelTasks(h.model, h.context, h.contextPolicy, h.maxWindowSize)
	chain := append([]ToolInterceptor{}, opts.ToolInterceptors...)
	if !opts.disablePlanningLock {
		chain = append(chain, h.planningWriteLock)
	}
	chain = append(chain, onCallMiddleware(sm))
	h.toolRunner = newToolRunner(chain...)
	h.toolResultHooks = newToolResultHookRegistry(opts.ToolResultHooks)
	if err := h.finishInit(ctx, opts.Specialists); err != nil {
		return nil, err
	}
	return h, nil
}

// Validate checks the construction contract and fills MaxWindowSize from the
// model when the host left it at zero.
func (opts *AgentOptions) Validate() error {
	if opts.Model == nil {
		return fmt.Errorf("tacklr: AgentOptions.Model is required")
	}
	if err := opts.Config.Validate(); err != nil {
		return err
	}
	if opts.Config.MaxWindowSize == 0 {
		size, err := opts.Model.MaxContextWindow()
		if err != nil {
			return fmt.Errorf("tacklr: resolve model context window: %w", err)
		}
		if size <= 0 {
			return fmt.Errorf("tacklr: Config.MaxWindowSize is required when the model does not report a context window")
		}
		opts.Config.MaxWindowSize = size
	}
	if err := opts.ContextPolicy.Validate(); err != nil {
		return err
	}
	for i, tool := range opts.Tools {
		if tool == nil {
			return fmt.Errorf("tacklr: AgentOptions.Tools[%d] is nil", i)
		}
	}
	seenMCP := make(map[string]struct{}, len(opts.MCPConfigs))
	for i := range opts.MCPConfigs {
		config := opts.MCPConfigs[i]
		if err := config.Validate(); err != nil {
			return err
		}
		if _, ok := seenMCP[config.Name]; ok {
			return fmt.Errorf("tacklr: duplicate MCP server name %q", config.Name)
		}
		seenMCP[config.Name] = struct{}{}
		if config.CredentialRef != "" && opts.MCPCredentialResolver == nil {
			return fmt.Errorf("tacklr: MCP credential resolver is required for server %q", config.Name)
		}
	}
	return nil
}

func (h *AgentHarness) finishInit(ctx context.Context, subAgents []*Specialist) error {
	if err := h.initSkills(ctx); err != nil {
		return fmt.Errorf("initialize skills: %w", err)
	}
	h.initMCP(ctx)
	if err := h.initSpecialists(subAgents); err != nil {
		return err
	}
	if h.vfsBridge == nil {
		if err := h.initVFSIndexBridge(); err != nil {
			return err
		}
	}
	h.injectBuiltinTools()
	return nil
}

func (a *AgentHarness) ensureReady(ctx context.Context) error {
	if err := a.initSkills(ctx); err != nil {
		return fmt.Errorf("load skills: %w", err)
	}
	a.initMCP(ctx)
	a.injectBuiltinTools()
	return nil
}

// injectBuiltinTools registers plan tools, optional web/brain/VFS/index tools, and spawn_specialist once.
func (a *AgentHarness) injectBuiltinTools() {
	if a.builtinsInjected {
		return
	}
	a.tools = append(a.tools,
		newCreatePlanTool(a.session),
		newEditPlanTool(a.session),
		newCompleteTodoTool(a.session),
		newListPlanTool(a.session),
		askUserChoiceTool,
	)
	if key := strings.TrimSpace(a.exaAPIKey); key != "" {
		client := exa.NewClient(key)
		a.tools = append(a.tools, newWebSearchTool(client), newWebFetchTool(client))
	}
	br := a.vfsBridge
	if ms := a.VFS(); ms != nil {
		a.tools = append(a.tools, newVFSTools(ms, !a.writeUnattended)...)
		a.tools = append(a.tools, newRunCommand(ms, !a.runCommandUnattended))
	}
	if a.brain != nil {
		var idx *vfsindex.MountIndexer
		if br != nil {
			idx = br.Indexer
		}
		a.tools = append(a.tools, newBrainTools(a.brain, a.session.Search, a.brainWriteKinds, brainToolDeps{
			VFS:     a.VFS(),
			Indexer: idx,
		})...)
	}
	if br != nil {
		a.tools = append(a.tools, newVFSIndexTools(br)...)
	}
	if len(a.specialists) > 0 {
		a.tools = append(a.tools, a.spawnTool(), a.listJobsTool(), a.getJobTool(), a.cancelJobTool())
	}
	a.builtinsInjected = true
}

// initVFSIndexBridge starts a new vfsindex.Bridge when Brain + VFS + namespace
// are set. Call only when vfsBridge is nil (this harness owns the lifecycle).
// Hosts with a non-empty kind catalog should register vfsindex.MountIndexKinds().
func (a *AgentHarness) initVFSIndexBridge() error {
	if a.brain == nil || a.VFS() == nil {
		return nil
	}
	ns, ok := a.session.Search.Namespace()
	if !ok {
		return nil
	}
	nsCopy := ns
	attachMemory := true
	for _, s := range a.VFS().Specs() {
		if s.Profile == brain.DefaultProfile {
			attachMemory = false
			break
		}
	}
	br, err := vfsindex.Start(a.VFS(), a.brain, brain.Scope{Namespace: &nsCopy}, attachMemory)
	if err != nil {
		return fmt.Errorf("vfsindex: start bridge: %w", err)
	}
	a.vfsBridge = br
	a.ownsVFSBridge = true
	return nil
}

// SetSearchNamespace sets retrieval isolation for knowledge tools.
func (a *AgentHarness) SetSearchNamespace(id uuid.UUID) {
	a.session.Search.SetNamespace(id)
}

// ClearSearchNamespace clears retrieval isolation for knowledge tools.
func (a *AgentHarness) ClearSearchNamespace() {
	a.session.Search.ClearNamespace()
}

// SearchNamespace returns the host-set search namespace, if any.
func (a *AgentHarness) SearchNamespace() (uuid.UUID, bool) {
	return a.session.Search.Namespace()
}

// planningWriteLock blocks write tools until create_plan has set a plan.
func (a *AgentHarness) planningWriteLock(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
	if inv.Tool != nil && inv.Tool.access.Allows(WritePermission) &&
		!a.session.Plan.HasActive() {
		return "", fmt.Errorf("%w: write tools are locked until create_plan establishes a todo list", ErrToolPermissionDenied)
	}
	return next(ctx, inv)
}

func (a *AgentHarness) initSkills(ctx context.Context) error {
	if a.skillsInitialized {
		return nil
	}
	loader := a.skillsLoader
	if loader == nil {
		loader = skills.Loader{Session: a.VFS()}
	}
	loaded, err := loader.Load(ctx)
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
