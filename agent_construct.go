package tacklr

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/ryanaldo34/tacklr/session"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/skills"
)

// AgentOptions is the one agent, and the argument to NewHarness. A session
// runtime calls BindTurn to attach the session id for each turn. The agent's
// workspace (package workspace) adds its tools, skills and Overflow to these
// options for the turns that have one.
type AgentOptions struct {
	// MaxWindowSize is the context window in tokens. Zero uses the model's
	// reported window.
	MaxWindowSize uint
	SystemPrompt  string
	Model         Model
	// sessionID is the thread id for this turn. BindTurn sets it.
	sessionID string
	// Name is the label used in logs. It does not select behavior.
	Name string
	// Tools are host tools, including optional tools from web, shell and
	// workspace. Give each tool its clients by closing over them in the
	// constructor (see NewTool). The plan, brain and specialist tools inject
	// from the fields below.
	Tools []*Tool
	// MCP connects the harness to MCPConfigs (package mcp). It is
	// required when there are MCP servers.
	MCP        session.MCPClient
	MCPConfigs []session.MCPServer
	// MCPCredentialResolver resolves durable references immediately before
	// connection. Inline client credentials remain session-scoped.
	MCPCredentialResolver session.MCPCredentialResolver
	Specialists           []*Specialist
	// Overflow keeps the full text of results the window trims. When a round
	// is too large for the window even with the history fully summarized,
	// its largest results are trimmed; each one's full text is first kept
	// here and the trimmed result says where. Nil: the full text is not kept.
	Overflow Overflow
	// ToolInterceptors wrap each tool call (outermost first). Built-in
	// planning lock and OnCall middleware are installed after these.
	// Hosts cannot omit the planning lock; specialists skip it via WithSpecialist.
	ToolInterceptors []ToolInterceptor
	// AllowTools are tools (by the name the model calls them, namespace__name
	// for a namespaced tool) that run without a permission prompt. A user's
	// reject-always still wins. An unknown name fails construction.
	AllowTools []string
	// SkillsLoader loads the skills the agent can read: skills.Loader for a
	// local directory, or the workspace's skills directory (package
	// workspace). Nil: no skills.
	SkillsLoader skills.SkillLoader
	// Brain enables the knowledge tools (search, open, related, save, and
	// link when the engine has a graph) when non-nil. Workers inherit the
	// same engine. The host builds the store, and the graph with
	// brain.WithGraph, each with its own embedding, and passes the engine
	// before NewHarness. The harness does not construct backends.
	Brain *brain.Engine
	// BrainWriteKinds are the kinds the save tool may write; it lists them
	// as its kind choices. Empty: no save tool. Register them with
	// brain.WithKinds or ApplyKinds. Ignored when Brain is nil.
	BrainWriteKinds []string
	// SearchNamespace is the host ceiling for brain tools (session-owned, checkpointed):
	// the widest namespace this session may read or write. Each tool call may add
	// attrs to narrow it; it cannot change ceiling values. Required when Brain is
	// set. Workers get a copy at spawn.
	SearchNamespace brain.Namespace
	// SearchPrincipals are the caller's identities (user and group ids) checked
	// against object Readers, so search only returns source files the caller can
	// open at the source. Set them from the host's identity provider each turn.
	SearchPrincipals []string
	// skipPlanningLock is set only by WithSpecialist. The parent session always
	// gates write tools on an active plan.
	skipPlanningLock bool
	// jobs are the Runtime job workers the runtime registered. BindTurn
	// sets them.
	jobs []string
}

// Overflow keeps the full text of results trimmed from the window, where the
// agent can read it again. Package workspace keeps it on the workspace.
type Overflow interface {
	// Keep stores text, the full result of the session's call callID, and
	// returns where the agent reads it.
	Keep(ctx context.Context, sessionID, callID, text string) (string, error)
}

// BindTurn attaches the per-turn session id and the names of the Runtime
// job workers the runtime registered. The session runtime calls BindTurn.
func BindTurn(opts AgentOptions, id string, jobs []string) AgentOptions {
	opts.sessionID = id
	opts.jobs = jobs
	return opts
}

// NewHarness builds a Harness for one turn slice.
// Session runtimes call this after BindTurn. Hosts use a session.Client.
func NewHarness(ctx context.Context, opts AgentOptions) (*Harness, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	sm := newSessionManager()
	if !opts.SearchNamespace.Empty() {
		sm.Search.SetNamespace(opts.SearchNamespace)
	}
	sm.Search.SetPrincipals(opts.SearchPrincipals)
	h := &Harness{
		model:                 opts.Model,
		maxWindowSize:         int(opts.MaxWindowSize),
		instructions:          opts.SystemPrompt,
		session:               sm,
		tools:                 opts.Tools,
		mcp:                   opts.MCP,
		mcpConfigs:            opts.MCPConfigs,
		mcpCredentialResolver: opts.MCPCredentialResolver,
		skillsLoader:          opts.SkillsLoader,
		brain:                 opts.Brain,
		brainWriteKinds:       opts.BrainWriteKinds,
		sessionId:             opts.sessionID,
		jobs:                  opts.jobs,
		specialists:           make(map[string]*Specialist),
		context:               newModelContextManager(),
		overflow:              opts.Overflow,
	}
	chain := slices.Clone(opts.ToolInterceptors)
	if !opts.skipPlanningLock {
		chain = append(chain, h.planningWriteLock)
	}
	h.toolRunner = newToolRunner(append(chain, onCallMiddleware)...)
	if err := h.finishInit(ctx, opts); err != nil {
		h.Close()
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
	if opts.MaxWindowSize == 0 {
		size, err := opts.Model.MaxContextWindow()
		if err != nil {
			return fmt.Errorf("tacklr: resolve model context window: %w", err)
		}
		if size <= 0 {
			return fmt.Errorf("tacklr: MaxWindowSize is required when the model does not report a context window")
		}
		opts.MaxWindowSize = uint(size)
	}
	if opts.Brain != nil {
		if err := opts.SearchNamespace.Validate(); err != nil {
			return fmt.Errorf("tacklr: AgentOptions.SearchNamespace is required with Brain: %w", err)
		}
	}
	for i, tool := range opts.Tools {
		if tool == nil {
			return fmt.Errorf("tacklr: AgentOptions.Tools[%d] is nil", i)
		}
	}
	if len(opts.MCPConfigs) > 0 && opts.MCP == nil {
		return errors.New("tacklr: AgentOptions.MCP is required with MCP servers")
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

func (h *Harness) finishInit(ctx context.Context, opts AgentOptions) error {
	if err := h.initSkills(ctx); err != nil {
		return fmt.Errorf("initialize skills: %w", err)
	}
	h.initMCP(ctx)
	if err := h.initSpecialists(opts.Specialists); err != nil {
		return err
	}
	h.injectBuiltinTools()
	return h.grantTools(opts.AllowTools)
}

// injectBuiltinTools registers the plan, brain and specialist tools.
func (a *Harness) injectBuiltinTools() {
	a.tools = append(a.tools,
		newCreatePlanTool(),
		newEditPlanTool(),
		newCompleteTodoTool(),
		newListPlanTool(),
		askUserChoiceTool,
	)
	if a.brain != nil {
		a.tools = append(a.tools, newBrainTools(a.brain, a.session.Search, a.sessionId, a.brainWriteKinds)...)
	}
	if len(a.specialists) > 0 {
		a.tools = append(a.tools, a.spawnTool(), a.listChildrenTool(), a.cancelChildTool())
	}
}

// grantTools lets the named tools run without a permission prompt. Each
// name must be a registered tool's QualifiedName.
func (a *Harness) grantTools(names []string) error {
	if len(names) == 0 {
		return nil
	}
	granted := make(map[string]bool, len(names))
	for _, name := range names {
		if !slices.ContainsFunc(a.tools, func(t *Tool) bool { return t.QualifiedName() == name }) {
			return fmt.Errorf("tacklr: AgentOptions.AllowTools: %q is not a registered tool", name)
		}
		granted[name] = true
	}
	a.session.Permissions.granted = granted
	return nil
}

// planningWriteLock blocks write tools until create_plan has set a plan.
func (a *Harness) planningWriteLock(ctx context.Context, inv ToolInvocation, next ToolCallFunc) (ToolOutcome, error) {
	if inv.Tool.access.Allows(WritePermission) && !a.session.Plan.HasActive() {
		return ToolOutcome{}, fmt.Errorf("%w: write tools are locked until create_plan establishes a todo list", ErrToolPermissionDenied)
	}
	return next(ctx, inv)
}

func (a *Harness) initSkills(ctx context.Context) error {
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

func (a *Harness) skillTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "read_skill",
		Description: "Load the full instructions for a skill listed in the skills catalog. Call when that method should shape the current work. Returns the skill body. Fails if the name is not in the catalog.",
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
