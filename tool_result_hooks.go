package tacklr

import (
	"context"
	"sync/atomic"

	"github.com/ryanaldo34/tacklr/control"
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

// ToolResultObservation is a successful tool invocation observed by a hook.
type ToolResultObservation struct {
	Name     string
	ArgsJSON string
	Output   string
	Runtime  control.HarnessRuntime
}

// ToolResultDisposition is returned by a ToolResultHook after a tool finishes.
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

func defaultToolResultHooks() map[string]ToolResultHook {
	return map[string]ToolResultHook{
		"create_plan":   createPlanResultHook,
		"complete_todo": completeTodoResultHook,
		"edit_plan":     editPlanResultHook,
	}
}

func (r *toolResultHookRegistry) observe(ctx context.Context, obs ToolResultObservation) ToolResultDisposition {
	hook := r.byName[obs.Name]
	if hook == nil {
		return ToolResultDisposition{}
	}
	return hook(ctx, obs)
}

func createPlanResultHook(_ context.Context, _ ToolResultObservation) ToolResultDisposition {
	return ToolResultDisposition{
		Effect:                EffectInstallPlanDocument,
		SuppressWindowMessage: true,
	}
}

func completeTodoResultHook(_ context.Context, _ ToolResultObservation) ToolResultDisposition {
	return ToolResultDisposition{Effect: EffectHandoff}
}

func editPlanResultHook(_ context.Context, obs ToolResultObservation) ToolResultDisposition {
	if obs.Runtime.ConsumePlanDocumentUpdated() {
		return ToolResultDisposition{Effect: EffectHandoff}
	}
	return ToolResultDisposition{}
}

type batchToolResultEffects struct {
	installPlan atomic.Bool
	handoff     atomic.Bool
}

func (b *batchToolResultEffects) merge(d ToolResultDisposition) {
	switch d.Effect {
	case EffectInstallPlanDocument:
		b.installPlan.Store(true)
	case EffectHandoff:
		b.handoff.Store(true)
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
