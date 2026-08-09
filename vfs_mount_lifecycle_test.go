package tacklr

import (
	"context"
	"testing"

	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestVFS_sessionMountsSurviveCheckpointReload is the harness integration:
// bootstrap + mid-session mount changes persist through NewAgentFromSession.
func TestVFS_sessionMountsSurviveCheckpointReload(t *testing.T) {
	ctx := context.Background()
	store := stores.NewInMemoryStore()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}

	const sessionID = "sess-vfs-lifecycle"
	ms := vfs.NewMountSession(sessionID, reg)

	h := NewAgent(ctx, AgentOptions{
		SessionID:    sessionID,
		Store:        store,
		MountSession: ms,
		FSRegistry:   reg,
		FSBootstrap: []vfs.MountSpec{
			{Point: "/scratch", Profile: "scratch", Params: map[string]string{"subpath": "boot"}},
		},
		Model: &mockStrategy{},
	})
	if h.session.VFS == nil {
		t.Fatal("session VFS not bound")
	}
	if err := h.session.VFS.Mount(ctx, vfs.MountSpec{
		Point: "/work", Profile: "scratch", ReadOnly: true,
		Params: map[string]string{"subpath": "work"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.session.VFS.Unmount("/scratch"); err != nil {
		t.Fatal(err)
	}
	if err := h.checkpointSession(ctx); err != nil {
		t.Fatal(err)
	}

	loaded, err := NewAgentFromSession(ctx, sessionID, AgentOptions{
		Store:      store,
		FSRegistry: reg,
		Model:      &mockStrategy{},
	})
	if err != nil {
		t.Fatal(err)
	}
	specs := loaded.session.VFS.Specs()
	if len(specs) != 1 || specs[0].Point != "/work" || !specs[0].ReadOnly || specs[0].Params["subpath"] != "work" {
		t.Fatalf("restored Specs = %+v", specs)
	}
}
