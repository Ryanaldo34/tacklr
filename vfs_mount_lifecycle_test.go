package tacklr

import (
	"context"
	"testing"

	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestVFS_mountLifecycle_sessionOwnedMountsPersistsAcrossReload: mounts are managed
// on vfs.MountSession (session-level), not AgentHarness. Changes persist via checkpoint.
func TestVFS_mountLifecycle_sessionOwnedMountsPersistsAcrossReload(t *testing.T) {
	ctx := context.Background()
	store := stores.NewInMemoryStore()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}

	const sessionID = "sess-vfs-lifecycle"
	// Host owns the mount session — attach/detach without harness APIs.
	ms := vfs.NewMountSession(sessionID, reg)

	h := NewAgent(ctx, AgentOptions{
		SessionID:    sessionID,
		Store:        store,
		MountSession: ms,
		FSBootstrap: []vfs.MountSpec{
			{Point: "/scratch", Profile: "scratch", Params: map[string]string{"subpath": "boot"}},
		},
		FSRegistry: reg,
		Model:      &mockStrategy{},
	})
	// Bootstrap applied onto the same MountSession held by the session manager.
	if len(ms.Specs()) != 1 {
		// init materializes bootstrap onto session.VFS which is ms
		if h.session.VFS == nil || len(h.session.VFS.Specs()) != 1 {
			t.Fatalf("bootstrap mounts = %+v session=%+v", ms.Specs(), h.session.VFS)
		}
	}

	// Mid-session attach / detach on the session object (not the harness).
	mounts := h.session.VFS
	if err := mounts.Mount(ctx, vfs.MountSpec{
		Point:    "/work",
		Profile:  "scratch",
		ReadOnly: true,
		Params:   map[string]string{"subpath": "work"},
	}); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	if err := mounts.Unmount("/scratch"); err != nil {
		t.Fatalf("Unmount: %v", err)
	}
	if len(mounts.Specs()) != 1 || mounts.Specs()[0].Point != "/work" {
		t.Fatalf("after lifecycle Specs = %+v", mounts.Specs())
	}

	// Persist without a full Run.
	if err := h.checkpointSession(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// Restart: new MountSession materializes durable specs from checkpoint.
	loaded, err := NewAgentFromSession(ctx, sessionID, AgentOptions{
		Store:      store,
		FSRegistry: reg,
		Model:      &mockStrategy{},
	})
	if err != nil {
		t.Fatalf("NewAgentFromSession: %v", err)
	}
	specs := loaded.session.VFS.Specs()
	if len(specs) != 1 || specs[0].Point != "/work" || !specs[0].ReadOnly || specs[0].Params["subpath"] != "work" {
		t.Fatalf("restored Specs = %+v", specs)
	}
}
