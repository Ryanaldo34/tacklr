package tacklr

import (
	"context"
	"encoding/json"
	"errors"
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
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
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
	// SkillsLoader loads skills. Nil uses DirectoryLoader with Config.SkillDirectories.
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
	// MountSession is the session-owned VFS mount table. Hosts attach and detach
	// mounts on this object (vfs.MountSession.Mount / Unmount) — not on the harness.
	// If nil and FSRegistry is set, the harness creates one and stores it on the
	// session manager for checkpointing.
	MountSession *vfs.MountSession
	// FSRegistry resolves MountSpec.Profile to providers (process-scoped pools).
	// Used when MountSession is nil and mounts must be materialized, or when
	// creating a MountSession at construct. Prefer creating MountSession yourself.
	FSRegistry *vfs.BackendRegistry
	// FSBootstrap mounts applied when materializing a new or loaded session.
	// Merged before durable checkpoint mounts; duplicate points error.
	FSBootstrap []vfs.MountSpec
}

// streamEventBuffer is the harness event channel size so EmitUpdate is not dropped
// when the consumer lags briefly.
const streamEventBuffer = 64

// NewAgent builds a session-scoped harness. Turn-scoped Runtime is created in Run.
func NewAgent(ctx context.Context, opts AgentOptions) *AgentHarness {
	sm := session.NewSessionManager()
	h := newHarnessBase(opts, sm)
	if opts.SessionID != "" {
		h.sessionId = opts.SessionID
	}
	if err := h.initSessionMounts(ctx, nil); err != nil {
		slog.Error("failed to initialize virtual filesystem", "error", err)
	}
	h.finishInit(ctx, opts.SubAgents)
	return h
}

// newHarnessBase fills shared fields. Session state lives on sm across turns.
// sm must be non-nil.
func newHarnessBase(opts AgentOptions, sm *session.SessionManager) *AgentHarness {
	h := &AgentHarness{
		model:                opts.Model,
		maxWindowSize:        opts.Config.MaxWindowSize,
		maxTurnRequests:      opts.Config.MaxTurnRequests,
		instructions:         opts.Config.SystemPrompt,
		store:                opts.Store,
		session:              sm,
		watchDog:             opts.WatchDog,
		tools:                opts.Tools,
		mcpConfigs:           opts.MCPConfigs,
		skillDirectories:     opts.Config.SkillDirectories,
		skillsLoader:         opts.SkillsLoader,
		exaAPIKey:            resolveExaAPIKey(opts.ExaAPIKey),
		brain:                opts.Brain,
		brainWriteKinds:      opts.BrainWriteKinds,
		sessionId:            "",
		subagents:            make(map[string]*SubAgent),
		interruptToRequester: make(map[string]string),
		pendingToolCalls:     make(map[string]stores.PendingToolCall),
		interruptPayloads:    make(map[string][]byte),
		parkedWorkersLive:    make(map[string]*AgentHarness),
		context:              opts.ContextManager,
		tasks:                opts.ModelTasks,
		contextPolicy:        opts.ContextPolicy,
		fsRegistry:           opts.FSRegistry,
		fsBootstrap:          opts.FSBootstrap,
	}
	if opts.MountSession != nil {
		sm.VFS = opts.MountSession
	}
	if opts.Brain != nil {
		h.searchCtx = brain.NewSearchContext()
		if opts.SearchNamespace != nil {
			h.searchCtx.SetNamespace(*opts.SearchNamespace)
		}
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
	if err := h.initSkills(ctx); err != nil {
		slog.Error("failed to initialize skills", "error", err)
	}
	h.initSubAgentWorkers(subAgents)
	h.injectBuiltinTools()
}

// injectBuiltinTools registers plan tools, optional web/brain/VFS/index tools, and spawn_worker once.
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
	// Index bridge after brain factory/mounts so save_* can write Engram files.
	br := a.initVFSIndexBridge()
	if a.session != nil && a.session.VFS != nil {
		a.tools = append(a.tools, newVFSTools(a.session.VFS)...)
	}
	if a.brain != nil && a.searchCtx != nil {
		var ms *vfs.MountSession
		var idx *vfsindex.MountIndexer
		if a.session != nil {
			ms = a.session.VFS
		}
		if br != nil {
			idx = br.Indexer
		}
		a.tools = append(a.tools, newBrainTools(a.brain, a.searchCtx, a.brainWriteKinds, brainToolDeps{
			VFS:     ms,
			Indexer: idx,
		})...)
	}
	if br != nil {
		a.tools = append(a.tools, newVFSIndexTools(br)...)
	}
	if len(a.subagents) > 0 {
		a.tools = append(a.tools, a.spawnTool())
	}
	a.builtinsInjected = true
}

// initVFSIndexBridge starts vfsindex.Bridge when Brain + VFS + namespace are set.
// Hosts with a non-empty kind catalog should register vfsindex.MountIndexKinds().
func (a *AgentHarness) initVFSIndexBridge() *vfsindex.Bridge {
	if a.vfsBridge != nil {
		return a.vfsBridge
	}
	if a.brain == nil || a.searchCtx == nil || a.session == nil || a.session.VFS == nil {
		return nil
	}
	ns, ok := a.searchCtx.Namespace()
	if !ok {
		return nil
	}
	nsCopy := ns
	scratch := a.fsRegistry != nil && a.fsRegistry.HasProfile("scratch") &&
		!sessionHasProfile(a.session.VFS, brain.DefaultProfile)
	br, err := vfsindex.Start(a.session.VFS, a.brain, brain.Scope{Namespace: &nsCopy}, scratch)
	if err != nil {
		slog.Error("vfsindex: failed to start bridge", "error", err)
		return nil
	}
	a.vfsBridge = br
	return br
}

// SetSearchNamespace sets retrieval isolation for knowledge tools.
func (a *AgentHarness) SetSearchNamespace(id uuid.UUID) {
	if a.searchCtx == nil {
		a.searchCtx = brain.NewSearchContext()
	}
	a.searchCtx.SetNamespace(id)
}

// ClearSearchNamespace clears retrieval isolation for knowledge tools.
func (a *AgentHarness) ClearSearchNamespace() {
	if a.searchCtx == nil {
		return
	}
	a.searchCtx.ClearNamespace()
}

// SearchNamespace returns the host-set search namespace, if any.
func (a *AgentHarness) SearchNamespace() (uuid.UUID, bool) {
	if a.searchCtx == nil {
		return uuid.UUID{}, false
	}
	return a.searchCtx.Namespace()
}

// planningWriteLock blocks write tools until create_plan has set a plan.
func (a *AgentHarness) planningWriteLock(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (string, error) {
	if inv.Tool != nil && inv.Tool.Access != nil && inv.Tool.Access.Contains(WritePermission) &&
		!a.session.HasActivePlan() {
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
		loader = skills.DirectoryLoader{Directories: a.skillDirectories}
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

// NewAgentFromSession loads a harness from a session checkpoint.
// opts.Store is required. Uses the same AgentOptions shape as NewAgent.
func NewAgentFromSession(ctx context.Context, sessionId string, opts AgentOptions) (*AgentHarness, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("agent harness: store is required to load session %q", sessionId)
	}
	checkpoint, err := opts.Store.LoadSession(ctx, sessionId)
	if err != nil {
		return nil, err
	}
	sm := session.NewSessionManager()
	applied, err := session.NewCheckpointer().Apply(checkpoint, sm)
	if err != nil {
		return nil, err
	}
	h := newHarnessBase(opts, sm)
	h.sessionId = sessionId
	h.context.Restore(applied.Window)
	h.interruptToRequester = applied.InterruptToRequester
	h.pendingToolCalls = applied.PendingToolCalls
	if h.searchCtx != nil {
		if len(checkpoint.State.SearchContext) > 0 {
			if err := h.searchCtx.Restore(checkpoint.State.SearchContext); err != nil {
				return nil, fmt.Errorf("agent harness: restore search context: %w", err)
			}
		}
		// Legacy checkpoints stored namespace only under runtime _search_namespace.
		if _, ok := h.searchCtx.Namespace(); !ok {
			if raw, ok := checkpoint.State.RuntimeState["_search_namespace"]; ok {
				if s, ok := raw.(string); ok {
					if id, err := uuid.Parse(s); err == nil {
						h.searchCtx.SetNamespace(id)
					}
				}
			}
		}
	}
	var durableMounts []vfs.MountSpec
	if len(checkpoint.State.Mounts) > 0 {
		if err := json.Unmarshal(checkpoint.State.Mounts, &durableMounts); err != nil {
			return nil, fmt.Errorf("invalid session mounts: %w", err)
		}
	}
	if err := h.initSessionMounts(ctx, durableMounts); err != nil {
		return nil, err
	}
	h.finishInit(ctx, opts.SubAgents)
	return h, nil
}

// initSessionMounts materializes bootstrap+durable specs onto session.VFS.
// Attach/detach after construct uses MountSession, not the harness.
func (a *AgentHarness) initSessionMounts(ctx context.Context, durable []vfs.MountSpec) error {
	if a.session == nil {
		return fmt.Errorf("vfs: no session")
	}
	a.registerBrainFactory()
	specs, err := vfs.MergeSpecs(a.fsBootstrap, durable)
	if err != nil {
		return err
	}
	for i := range specs {
		if specs[i].Profile == brain.DefaultProfile {
			specs[i].IndexPolicy = vfsindex.PolicyNone
		}
	}
	hasBrain := specsHaveProfile(specs, brain.DefaultProfile) ||
		sessionHasProfile(a.session.VFS, brain.DefaultProfile)
	auto := a.shouldAutoEngram() && !hasBrain

	if len(specs) == 0 && !auto {
		return nil
	}
	if a.session.VFS == nil {
		if a.fsRegistry == nil {
			return fmt.Errorf("vfs: registry required to restore mounts")
		}
		a.session.VFS = vfs.NewMountSession(a.sessionId, a.fsRegistry)
	}
	if len(specs) > 0 {
		if err := a.session.VFS.Materialize(ctx, specs); err != nil {
			return err
		}
	}
	if auto {
		if err := a.session.VFS.Mount(ctx, a.defaultEngramSpec()); err != nil && !errors.Is(err, vfs.ErrAlreadyMounted) {
			return err
		}
	}
	return nil
}

func (a *AgentHarness) registerBrainFactory() {
	if a.brain == nil || a.fsRegistry == nil || a.searchCtx == nil {
		return
	}
	ns, ok := a.searchCtx.Namespace()
	if !ok {
		return
	}
	nsCopy := ns
	_ = a.fsRegistry.Register(brain.BrainFactory{
		ID:     brain.DefaultProfile,
		Engine: a.brain,
		Scope:  brain.Scope{Namespace: &nsCopy},
	})
}

func (a *AgentHarness) shouldAutoEngram() bool {
	if a.brain == nil || a.searchCtx == nil || a.fsRegistry == nil {
		return false
	}
	if _, ok := a.searchCtx.Namespace(); !ok {
		return false
	}
	return a.fsRegistry.HasProfile(brain.DefaultProfile)
}

func (a *AgentHarness) defaultEngramSpec() vfs.MountSpec {
	params := map[string]string{"mode": brain.ModePrefix}
	if a.brain != nil && a.brain.Catalog() != nil && !a.brain.Catalog().Empty() {
		var names []string
		for _, spec := range a.brain.Catalog().All() {
			if brain.IsParentKind(spec) {
				names = append(names, spec.Kind)
			}
		}
		if len(names) > 0 {
			params["kinds"] = strings.Join(names, ",")
		}
	}
	return vfs.MountSpec{
		Point:       brain.DefaultMountPoint,
		Profile:     brain.DefaultProfile,
		IndexPolicy: vfsindex.PolicyNone,
		Params:      params,
	}
}

func specsHaveProfile(specs []vfs.MountSpec, profile string) bool {
	for _, s := range specs {
		if s.Profile == profile {
			return true
		}
	}
	return false
}

func sessionHasProfile(ms *vfs.MountSession, profile string) bool {
	if ms == nil {
		return false
	}
	for _, s := range ms.Specs() {
		if s.Profile == profile {
			return true
		}
	}
	return false
}
