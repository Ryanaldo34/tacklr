package tacklr

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/mcp"
	"github.com/ryanaldo34/tacklr/skills"
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

// AgentOptions configures NewTurnManager.
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
	SessionID string
	Model     InferenceStrategy
	WatchDog  AgentWatchDog
	// Tools are host tools, including optional builtins from package
	// builtins (email, Exa web). Give each tool its clients by closing
	// over them in the constructor (see NewTool). Session-world tools
	// (VFS, brain, index) still inject from the fields below.
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
	// Hosts cannot omit the planning lock; specialists skip it via WithSpecialist.
	ToolInterceptors []ToolInterceptor
	// UnattendedWrite injects write without ToolPermissionOnCall.
	// Default false: write parks for permission.
	UnattendedWrite bool
	// ToolResultHooks map tool name → post-success window effects for host tools.
	// Plan builtins use ToolOutcome instead.
	ToolResultHooks map[string]ToolResultHook
	// SkillsLoader loads skills. When nil, SkillsSession is walked with
	// skills.Loader. MountSession is never used for skills.
	SkillsLoader skills.SkillLoader
	// SkillsSession is the host-only skills tree for this turn. Runtime
	// builds it from AgentSpec.OpenSkills. It is not session.VFS; VFS tools
	// do not see it. Nil and a nil SkillsLoader means no skills.
	SkillsSession *vfs.MountSession
	// SkillsRoot is the virtual directory skills.Loader walks on
	// SkillsSession. Empty means skills.DefaultRoot (/workspace/skills).
	SkillsRoot string
	// Brain enables knowledge builtins when non-nil. Workers inherit the same engine.
	// Configure Store, optional QueryEmbedder, and optional GraphReader/GraphWriter on the Engine
	// before NewTurnManager (e.g. brain.WithGraph(g) after helixgraph.New). The harness
	// does not construct store or graph backends.
	Brain *brain.Engine
	// BrainWriteKinds maps save_discovery / save_fact / save_memory to host kind names.
	// Empty fields skip that tool. Kinds should be registered via brain.ApplyKinds / WithKinds.
	// Ignored when Brain is nil.
	BrainWriteKinds brain.WriteKinds
	// SearchNamespace is the host ceiling for brain tools (session-owned, checkpointed).
	// Each tool call may add attrs to narrow the search; it cannot change ceiling values.
	// Empty means no ceiling. Workers get a copy at spawn.
	SearchNamespace brain.Namespace
	// MountSession is the agent /workspace tree for this turn, or nil (no VFS tools).
	// Runtime builds one from OpenVFS plus Prompt.Auth bindings when a
	// projection is available. Embedders pass their own. The injector Closes
	// it after the turn; the harness never does (workers inherit the pointer).
	// Do not mount skills here; use SkillsSession.
	MountSession *vfs.MountSession
	// UnattendedRunCommand injects run_command without ToolPermissionOnCall.
	// Default false: run_command parks for permission.
	UnattendedRunCommand bool
	// shareIndexBridge is the parent index bridge. Nil means Start a new bridge.
	shareIndexBridge *vfsindex.Bridge
	// skipPlanningLock is set only by WithSpecialist. The parent session always
	// gates write tools on an active plan.
	skipPlanningLock bool
}

// NewTurnManager builds a TurnManager for one turn slice.
// Durable runtimes call this; hosts use durable.Runtime.
func NewTurnManager(ctx context.Context, opts AgentOptions) (*TurnManager, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	sm := newSessionManager()
	h := &TurnManager{
		model:                 opts.Model,
		maxWindowSize:         opts.Config.MaxWindowSize,
		maxTurnRequests:       opts.Config.MaxTurnRequests,
		instructions:          opts.Config.SystemPrompt,
		session:               sm,
		watchDog:              opts.WatchDog,
		tools:                 opts.Tools,
		mcpConfigs:            opts.MCPConfigs,
		mcpCredentialResolver: opts.MCPCredentialResolver,
		skillsLoader:          skillsSource(opts),
		hostInterceptors:      slices.Clone(opts.ToolInterceptors),
		hostResultHooks:       maps.Clone(opts.ToolResultHooks),
		brain:                 opts.Brain,
		brainWriteKinds:       opts.BrainWriteKinds,
		sessionId:             opts.SessionID,
		specialists:           make(map[string]*Specialist),
		pendingToolCalls:      make(map[string]PendingToolCall),
		context:               newModelContextManager(),
		contextPolicy:         opts.ContextPolicy,
		runCommandUnattended:  opts.UnattendedRunCommand,
		writeUnattended:       opts.UnattendedWrite,
		vfsBridge:             opts.shareIndexBridge,
	}
	if opts.MountSession != nil {
		sm.VFS = opts.MountSession
	}
	if !opts.SearchNamespace.Empty() {
		sm.Search.SetNamespace(opts.SearchNamespace)
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
	if !opts.skipPlanningLock {
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

func (h *TurnManager) finishInit(ctx context.Context, specialists []*Specialist) error {
	if err := h.initSkills(ctx); err != nil {
		return fmt.Errorf("initialize skills: %w", err)
	}
	h.initMCP(ctx)
	if err := h.initSpecialists(specialists); err != nil {
		return err
	}
	if h.vfsBridge == nil {
		h.initVFSIndexBridge()
	}
	h.injectBuiltinTools()
	return nil
}

// injectBuiltinTools registers plan tools, session-world VFS/brain/index tools, and spawn_specialist once.
func (a *TurnManager) injectBuiltinTools() {
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
	br := a.vfsBridge
	if ms := a.session.VFS; ms != nil {
		a.tools = append(a.tools, newVFSTools(ms, !a.writeUnattended)...)
		a.tools = append(a.tools, newRunCommand(ms, !a.runCommandUnattended))
	}
	if a.brain != nil {
		var idx *vfsindex.MountIndexer
		if br != nil {
			idx = br.Indexer
		}
		a.tools = append(a.tools, newBrainTools(a.brain, a.session.Search, a.brainWriteKinds, brainToolDeps{
			VFS:     a.session.VFS,
			Indexer: idx,
		})...)
	}
	if br != nil {
		a.tools = append(a.tools, newVFSIndexTools(br)...)
	}
	if len(a.specialists) > 0 {
		a.tools = append(a.tools, a.spawnTool(), a.listChildrenTool(), a.getChildTool(), a.cancelChildTool())
	}
	a.builtinsInjected = true
}

// initVFSIndexBridge starts a new vfsindex.Bridge when Brain + VFS + namespace
// are set. Call only when vfsBridge is nil (this harness owns the lifecycle).
// Hosts with a non-empty kind catalog should register vfsindex.MountIndexKinds().
func (a *TurnManager) initVFSIndexBridge() {
	if a.brain == nil || a.session.VFS == nil {
		return
	}
	ns, ok := a.session.Search.Namespace()
	if !ok {
		return
	}
	br, err := vfsindex.Start(a.session.VFS, a.brain, brain.Scope{Namespace: ns})
	if err != nil {
		return
	}
	a.vfsBridge = br
	a.ownsVFSBridge = true
}

// planningWriteLock blocks write tools until create_plan has set a plan.
func (a *TurnManager) planningWriteLock(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
	if inv.Tool != nil && inv.Tool.access.Allows(WritePermission) &&
		!a.session.Plan.HasActive() {
		return "", fmt.Errorf("%w: write tools are locked until create_plan establishes a todo list", ErrToolPermissionDenied)
	}
	return next(ctx, inv)
}

func skillsSource(opts AgentOptions) skills.SkillLoader {
	if opts.SkillsLoader != nil {
		return opts.SkillsLoader
	}
	if opts.SkillsSession == nil {
		return nil
	}
	return skills.Loader{Session: opts.SkillsSession, Root: opts.SkillsRoot}
}

func (a *TurnManager) initSkills(ctx context.Context) error {
	if a.skillsLoader == nil {
		return nil
	}
	loaded, err := a.skillsLoader.Load(ctx)
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
	return nil
}

func (a *TurnManager) skillTool() *Tool {
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
