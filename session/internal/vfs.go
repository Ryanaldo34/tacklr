package adapter

import (
	"context"
	"os"

	"go.opentelemetry.io/otel/log"

	"github.com/ryanaldo34/tacklr"
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

// OpenTurnVFS builds the turn-scoped MountSession from AgentOptions.OpenVFS.
// Nil OpenVFS returns nil. A nil projection leaves the session in-process.
func OpenTurnVFS(ctx context.Context, threadID string, agent tacklr.AgentOptions, bindings []vfs.Binding, proj vfs.Projection) (*vfs.MountSession, error) {
	if agent.OpenVFS == nil {
		return nil, nil
	}
	ms, err := agent.OpenVFS(ctx, threadID, vfs.Request{Bindings: bindings})
	if err != nil {
		return nil, err
	}
	if proj == nil || ms.HostDir() != "" {
		return ms, nil
	}
	if err := proj.Attach(ms, threadID); err != nil {
		telemetry.InstrumentsFromContext(ctx).RecordFuseMount(ctx, telemetry.FuseMountOutcomeError)
		CloseTurnVFS(ms)
		return nil, err
	}
	telemetry.EmitEvent(ctx, telemetry.EventFuseMount,
		log.String(telemetry.AttrSessionID, threadID),
	)
	telemetry.InstrumentsFromContext(ctx).RecordFuseMount(ctx, telemetry.FuseMountOutcomeOK)
	return ms, nil
}
