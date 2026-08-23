package durable

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/log"

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
			slog.Warn("turn vfs host dir remove failed", "session_id", sessionID, "dir", dir, "error", err)
		}
	}
}

func workspaceMembers(binds []vfs.Binding) []vfs.MountSpec {
	members := make([]vfs.MountSpec, 0, len(binds))
	for _, b := range binds {
		members = append(members, vfs.BindingMember(b))
	}
	return members
}

// OpenTurnVFS builds the turn-scoped MountSession from catalog bootstrap plus
// this work item's bindings. Nil when the agent has no VFS or the projection
// is unavailable. Tokens on bindings are applied to factories for this turn.
func OpenTurnVFS(ctx context.Context, threadID string, spec AgentSpec, bindings []vfs.Binding, proj vfs.Projection) (*vfs.MountSession, error) {
	hasBindings := len(bindings) > 0
	if spec.FSRegistry == nil || (len(spec.FSBootstrap) == 0 && !hasBindings) {
		return nil, nil
	}
	if proj == nil || !proj.Available() {
		telemetry.InstrumentsFromContext(ctx).RecordFuseMount(ctx, telemetry.FuseMountOutcomeUnavailable)
		return nil, nil
	}
	if hasBindings {
		if err := spec.FSRegistry.BindSession(threadID, bindings); err != nil {
			return nil, err
		}
	}
	ms, err := vfs.NewMountSession(threadID, spec.FSRegistry)
	if err != nil {
		return nil, err
	}
	if err := ms.Materialize(ctx, spec.FSBootstrap); err != nil {
		CloseTurnVFS(ms, threadID, "materialize")
		return nil, err
	}
	if hasBindings {
		if err := ms.Mount(ctx, vfs.Workspace(workspaceMembers(bindings)...)); err != nil {
			CloseTurnVFS(ms, threadID, "workspace")
			return nil, err
		}
		if err := ms.AttachSkills(ctx); err != nil {
			CloseTurnVFS(ms, threadID, "attach_skills")
			return nil, err
		}
	}
	for _, mount := range ms.Specs() {
		name := strings.TrimPrefix(mount.Point, "/")
		if name == "" || strings.Contains(name, "/") {
			err := fmt.Errorf("vfs: fuse requires single-segment mount points (got %q); use /work and /engram", mount.Point)
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

// ClearSessionVFS drops turn-local tokens from catalog factories.
func ClearSessionVFS(cat Catalog, sessionID SessionID) {
	if cat == nil || sessionID == "" {
		return
	}
	for _, id := range cat.IDs() {
		spec, ok := cat.Lookup(id)
		if !ok || spec.FSRegistry == nil {
			continue
		}
		spec.FSRegistry.ClearSession(string(sessionID))
	}
}
