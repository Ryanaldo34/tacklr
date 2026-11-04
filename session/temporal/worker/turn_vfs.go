package worker

import (
	"context"
	"path/filepath"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"github.com/ryanaldo34/tacklr/telemetry"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/fuse"
	"github.com/ryanaldo34/tacklr/vfs/mount"
	"github.com/ryanaldo34/tacklr/workspace"
)

// openWorkspace builds the turn's tree with ws.Open. With a fuseRoot it
// also shows the tree on the host at fuseRoot/<session>, so run_command can
// run there; the returned server unmounts it. Nil ws.Open is no workspace.
func openWorkspace(ctx context.Context, threadID string, ws workspace.Config, bindings []mount.Binding, fuseRoot string) (*vfs.MountSession, *fuse.Server, error) {
	if ws.Open == nil {
		return nil, nil, nil
	}
	ms, err := ws.Open(ctx, threadID, vfs.Request{Bindings: bindings})
	if err != nil {
		return nil, nil, err
	}
	if fuseRoot == "" {
		return ms, nil, nil
	}
	instruments := telemetry.InstrumentsFromContext(ctx)
	srv, err := fuse.Mount(ms, filepath.Join(fuseRoot, strings.ReplaceAll(threadID, "/", "_")))
	if err != nil {
		instruments.RecordFuseMount(ctx, telemetry.FuseMountOutcomeError)
		return nil, nil, err
	}
	telemetry.EmitEvent(ctx, telemetry.EventFuseMount, attribute.String(telemetry.AttrSessionID, threadID))
	instruments.RecordFuseMount(ctx, telemetry.FuseMountOutcomeOK)
	return ms, srv, nil
}

// closeWorkspace unmounts a turn's host directory, if it has one.
func closeWorkspace(srv *fuse.Server) {
	if srv == nil {
		return
	}
	telemetry.EmitEvent(context.Background(), telemetry.EventFuseUnmount)
	_ = srv.Close()
}
