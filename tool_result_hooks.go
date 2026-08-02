package tacklr

import (
	"context"
	"sync/atomic"
)

// ToolResultEffect is applied once after a successful tool batch (no pending interrupts).
type ToolResultEffect int

const (
	EffectNone ToolResultEffect = iota
	// EffectInstallPlanDocument prunes the window to [user, plan document].
	EffectInstallPlanDocument
	// EffectHandoff rebuilds to [user, plan?, handoff, nudge?].
	EffectHandoff
)

// BuiltinResult is the success value for framework builtins that drive ACM
// context effects. Prefer returning this from plan tools instead of relying on
// name-keyed ToolResultHooks. Invoke still surfaces Output as the tool string.
type BuiltinResult struct {
	Output string
	// Effect is merged across the batch and applied once at batch end.
	Effect ToolResultEffect
	// SuppressWindowMessage skips adding the tool-result Message to the window;
	// the client still receives StreamEventToolResult.
	SuppressWindowMessage bool
}

// disposition converts a BuiltinResult into the mergeable tool disposition.
func (r BuiltinResult) disposition() ToolResultDisposition {
	return ToolResultDisposition{
		Effect:                r.Effect,
		SuppressWindowMessage: r.SuppressWindowMessage,
	}
}

// ToolResultObservation is a successful tool invocation observed by a hook.
type ToolResultObservation struct {
	Name     string
	ArgsJSON string
	Output   string
	Runtime  HarnessRuntime
}

// ToolResultDisposition is returned by a tool (via BuiltinResult) or a
// ToolResultHook after a tool finishes successfully.
type ToolResultDisposition struct {
	// Effect is merged across the batch and applied once at batch end.
	Effect ToolResultEffect
	// SuppressWindowMessage skips adding the tool-result Message to the window;
	// the client still receives StreamEventToolResult.
	SuppressWindowMessage bool
}

// ToolResultHook observes a finished successful tool and may queue window effects.
// Hooks run after the tool returns and before the tool result is emitted.
// Window rebuilds are deferred to end of batch via Effect.
// Plan builtins return BuiltinResult instead; hooks remain for host tools.
type ToolResultHook func(ctx context.Context, obs ToolResultObservation) ToolResultDisposition

type toolResultHookRegistry struct {
	byName map[string]ToolResultHook
}

func newToolResultHookRegistry(hooks map[string]ToolResultHook) *toolResultHookRegistry {
	cp := make(map[string]ToolResultHook, len(hooks))
	for k, v := range hooks {
		cp[k] = v
	}
	return &toolResultHookRegistry{byName: cp}
}

func (r *toolResultHookRegistry) observe(ctx context.Context, obs ToolResultObservation) ToolResultDisposition {
	if r == nil {
		return ToolResultDisposition{}
	}
	hook := r.byName[obs.Name]
	if hook == nil {
		return ToolResultDisposition{}
	}
	return hook(ctx, obs)
}

type batchToolResultEffects struct {
	installPlan atomic.Bool
	handoff     atomic.Bool
	suppress    atomic.Bool
}

func (b *batchToolResultEffects) merge(d ToolResultDisposition) {
	switch d.Effect {
	case EffectInstallPlanDocument:
		b.installPlan.Store(true)
	case EffectHandoff:
		b.handoff.Store(true)
	}
	if d.SuppressWindowMessage {
		b.suppress.Store(true)
	}
}

// resolved prefers install over handoff when both appear in one batch.
func (b *batchToolResultEffects) resolved() ToolResultEffect {
	if b.installPlan.Load() {
		return EffectInstallPlanDocument
	}
	if b.handoff.Load() {
		return EffectHandoff
	}
	return EffectNone
}
