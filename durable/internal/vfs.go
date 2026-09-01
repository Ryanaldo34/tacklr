package adapter

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/log"

	"github.com/ryanaldo34/tacklr/durable"
	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
)

// CloseTurnVFS unmounts a turn-scoped MountSession (FUSE telemetry, Close, host dir).
func CloseTurnVFS(ms *vfs.MountSession, sessionID, reason string) {
	if ms == nil {
		return
	}
	dir := ms.HostDir()
	if dir != "" {
		telemetry.EmitEvent(context.Background(), telemetry.EventFuseUnmount)
	}
	if err := ms.Close(); err != nil {
		slog.Warn("turn vfs close failed", "session_id", sessionID, "reason", reason, "error", err)
	}
	if dir != "" {
		if err := os.Remove(dir); err != nil {
			slog.Warn("turn vfs host dir remove failed", "session_id", sessionID, "dir", dir, "reason", reason, "error", err)
		}
	}
}

// CloseTurnTrees closes the agent workspace tree and the host-only skills tree.
func CloseTurnTrees(workspace, skills *vfs.MountSession, sessionID, reason string) {
	CloseTurnVFS(workspace, sessionID, reason)
	CloseTurnVFS(skills, sessionID, reason+"-skills")
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
		CloseTurnVFS(workspace, threadID, "skills_open")
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
	if ms == nil {
		return nil, nil
	}
	for _, mount := range ms.Specs() {
		name := strings.TrimPrefix(mount.Point, "/")
		if name == "" || strings.Contains(name, "/") {
			err := fmt.Errorf("vfs: fuse requires a single-segment mount (got %q); Tree uses /workspace", mount.Point)
			CloseTurnVFS(ms, threadID, "fuse_point")
			return nil, err
		}
	}
	if ms.HostDir() == "" {
		if err := proj.Attach(ms, threadID); err != nil {
			telemetry.InstrumentsFromContext(ctx).RecordFuseMount(ctx, telemetry.FuseMountOutcomeError)
			CloseTurnVFS(ms, threadID, "fuse_attach")
			return nil, err
		}
		telemetry.EmitEvent(ctx, telemetry.EventFuseMount,
			log.String(telemetry.AttrSessionID, threadID),
		)
		telemetry.InstrumentsFromContext(ctx).RecordFuseMount(ctx, telemetry.FuseMountOutcomeOK)
	}
	return ms, nil
}
