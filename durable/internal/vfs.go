package adapter

import (
	"context"
	"os"

	"go.opentelemetry.io/otel/log"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

// CloseTurnVFS unmounts a turn-scoped MountSession (FUSE telemetry, Close, host dir).
func CloseTurnVFS(ms *vfs.MountSession) {
	if ms == nil {
		return
	}
	dir := ms.HostDir()
	if dir != "" {
		telemetry.EmitEvent(context.Background(), telemetry.EventFuseUnmount)
	}
	_ = ms.Close()
	if dir != "" {
		_ = os.Remove(dir)
	}
}

// CloseTurnTrees closes the agent workspace tree and the host-only skills tree.
func CloseTurnTrees(workspace, skills *vfs.MountSession) {
	CloseTurnVFS(workspace)
	CloseTurnVFS(skills)
}

// OpenSkillsVFS builds the host-only skills MountSession from AgentSpec.OpenSkills.
// It does not attach a FUSE projection. The agent never receives this session.
func OpenSkillsVFS(ctx context.Context, threadID string, spec durable.AgentSpec) (*vfs.MountSession, error) {
	if spec.OpenSkills == nil {
		return nil, nil
	}
	return spec.OpenSkills(ctx, threadID, vfs.Request{})
}

// OpenTurnSessions opens the agent workspace and the host-only skills tree.
// On OpenSkills failure the workspace session is closed.
func OpenTurnSessions(ctx context.Context, threadID string, spec durable.AgentSpec, bindings []vfs.Binding, proj vfs.Projection) (workspace, skills *vfs.MountSession, err error) {
	workspace, err = OpenTurnVFS(ctx, threadID, spec, bindings, proj)
	if err != nil {
		return nil, nil, err
	}
	skills, err = OpenSkillsVFS(ctx, threadID, spec)
	if err != nil {
		CloseTurnVFS(workspace)
		return nil, nil, err
	}
	return workspace, skills, nil
}

// OpenTurnVFS builds the turn-scoped MountSession from AgentSpec.OpenVFS.
// Nil when OpenVFS is nil or the projection is unavailable.
func OpenTurnVFS(ctx context.Context, threadID string, spec durable.AgentSpec, bindings []vfs.Binding, proj vfs.Projection) (*vfs.MountSession, error) {
	if spec.OpenVFS == nil {
		return nil, nil
	}
	if proj == nil || !proj.Available() {
		telemetry.InstrumentsFromContext(ctx).RecordFuseMount(ctx, telemetry.FuseMountOutcomeUnavailable)
		return nil, nil
	}
	ms, err := spec.OpenVFS(ctx, threadID, vfs.Request{Bindings: bindings})
	if err != nil {
		return nil, err
	}
	if ms.HostDir() == "" {
		if err := proj.Attach(ms, threadID); err != nil {
			telemetry.InstrumentsFromContext(ctx).RecordFuseMount(ctx, telemetry.FuseMountOutcomeError)
			CloseTurnVFS(ms)
			return nil, err
		}
		telemetry.EmitEvent(ctx, telemetry.EventFuseMount,
			log.String(telemetry.AttrSessionID, threadID),
		)
		telemetry.InstrumentsFromContext(ctx).RecordFuseMount(ctx, telemetry.FuseMountOutcomeOK)
	}
	return ms, nil
}
