package server

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestRegistry_bindRefreshUnbindDrive(t *testing.T) {
	ctx := t.Context()
	auth := vfs.NewSessionAuth()
	api := newACPMemDrive()
	spec, _ := driveAgent(t, auth, api)
	r := NewRegistry(testStore(t), "default", WithVFSProjection(DirectProjection{}), WithVFSAuth(auth))
	r.Register("default", spec)

	bContracts := vfs.Binding{
		Provider: "gdrive", Point: "/contracts",
		Auth: vfs.Credential{Token: "tok"}, Params: map[string]string{vfs.ParamFolderID: "root-a"},
	}
	if err := r.BindVFS(ctx, "sess-1", "", bContracts); err != nil {
		t.Fatal(err)
	}
	if !auth.HasBindings("sess-1") {
		t.Fatal("bind must record credentials before the session tree exists")
	}

	stream, err := r.RunTurn(ctx, TurnRequest{
		SessionID: "sess-1", AgentID: "default", ThreadID: "sess-1", Prompt: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Cancel(); stream.Close() })
	ms := stream.VFS()
	if ms == nil {
		t.Fatal("want VFS after bind + turn")
	}
	got, err := ms.ReadFile(ctx, "/workspace/contracts/a.txt")
	if err != nil || string(got) != "one" {
		t.Fatalf("read contracts = %q err=%v", got, err)
	}

	bNotes := vfs.Binding{
		Provider: "gdrive", Point: "/notes",
		Auth: vfs.Credential{Token: "tok"}, Params: map[string]string{vfs.ParamFolderID: "root-b"},
	}
	drainTurn(t, stream)
	if err := r.BindVFS(ctx, "sess-1", "default", bNotes); err != nil {
		t.Fatal(err)
	}
	stream, err = r.RunTurn(ctx, TurnRequest{
		SessionID: "sess-1", AgentID: "default", ThreadID: "sess-1", Prompt: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Cancel(); stream.Close() })
	ms = stream.VFS()
	got, err = ms.ReadFile(ctx, "/workspace/notes/b.txt")
	if err != nil || string(got) != "two" {
		t.Fatalf("next turn notes = %q err=%v", got, err)
	}
	got, err = ms.ReadFile(ctx, "/workspace/contracts/a.txt")
	if err != nil || string(got) != "one" {
		t.Fatalf("next turn contracts = %q err=%v", got, err)
	}

	if err := r.RefreshVFS("sess-1", "gdrive", vfs.Credential{Token: "rotated"}); err != nil {
		t.Fatal(err)
	}
	if tok, ok := auth.Credential("sess-1", "gdrive"); !ok || tok.Token != "rotated" {
		t.Fatalf("refresh = %+v ok=%v", tok, ok)
	}

	r.SetVFSTokenRefresh("sess-1", "gdrive", func(context.Context) (vfs.Credential, error) {
		return vfs.Credential{Token: "from-cb"}, nil
	})
	if err := auth.Holder("sess-1", "gdrive").RefreshOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if tok, _ := auth.Credential("sess-1", "gdrive"); tok.Token != "from-cb" {
		t.Fatalf("holder refresh = %q", tok.Token)
	}

	// Unbind drops recipes only. The live tree stays until this turn Close.
	if err := r.UnbindVFS(ctx, "sess-1", "", "gdrive"); err != nil {
		t.Fatal(err)
	}
	if auth.HasBindings("sess-1") {
		t.Fatal("unbind provider must drop credentials")
	}
	got, err = ms.ReadFile(ctx, "/workspace/contracts/a.txt")
	if err != nil || string(got) != "one" {
		t.Fatalf("live tree after unbind = %q err=%v", got, err)
	}

	drainTurn(t, stream)
	stream, err = r.RunTurn(ctx, TurnRequest{
		SessionID: "sess-1", AgentID: "default", ThreadID: "sess-1", Prompt: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Cancel(); stream.Close() })
	if stream.VFS() != nil {
		t.Fatal("next turn has no tree when the client bound nothing")
	}
}

func TestRegistry_vfsAuthRejects(t *testing.T) {
	ctx := t.Context()
	auth := vfs.NewSessionAuth()
	api := newACPMemDrive()
	spec, _ := driveAgent(t, auth, api)
	r := NewRegistry(testStore(t), "default", WithVFSProjection(DirectProjection{}), WithVFSAuth(auth))
	r.Register("default", spec)

	if err := r.BindVFS(ctx, "live", "default", vfs.Binding{
		Provider: "gdrive", Point: "/ok", Auth: vfs.Credential{Token: "t"},
		Params: map[string]string{vfs.ParamFolderID: "root-a"},
	}); err != nil {
		t.Fatal(err)
	}
	stream, err := r.RunTurn(ctx, TurnRequest{
		SessionID: "live", AgentID: "default", ThreadID: "live", Prompt: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Cancel(); stream.Close() })
	ms := stream.VFS()
	if ms == nil {
		t.Fatal("want VFS after bind")
	}
	got, err := ms.ReadFile(ctx, "/workspace/ok/a.txt")
	if err != nil || string(got) != "one" {
		t.Fatalf("read ok = %q err=%v", got, err)
	}

	if err := r.BindVFS(ctx, "live", "default", vfs.Binding{
		Provider: "gdrive", Point: "/bad", Auth: vfs.Credential{Token: "t"},
		Params: map[string]string{vfs.ParamFolderID: "nope"},
	}); err == nil {
		t.Fatal("CheckMount must reject a missing folder before the next turn")
	}
	got, err = ms.ReadFile(ctx, "/workspace/ok/a.txt")
	if err != nil || string(got) != "one" {
		t.Fatalf("live tree after failed bind = %q err=%v", got, err)
	}

	if err := r.BindVFS(ctx, "live", "default", vfs.Binding{
		Provider: "gdrive", Point: "/ok", Auth: vfs.Credential{Token: "t"},
		Params: map[string]string{vfs.ParamFolderID: "nope"},
	}); err == nil {
		t.Fatal("replace same alias with missing folder")
	}
	got, err = ms.ReadFile(ctx, "/workspace/ok/a.txt")
	if err != nil || string(got) != "one" {
		t.Fatalf("live tree after failed replace = %q err=%v", got, err)
	}
	binds := auth.Bindings("live")
	if len(binds) != 1 || binds[0].Params[vfs.ParamFolderID] != "root-a" {
		t.Fatalf("restored bind = %+v", binds)
	}

	drainTurn(t, stream)
	stream, err = r.RunTurn(ctx, TurnRequest{
		SessionID: "live", AgentID: "default", ThreadID: "live", Prompt: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Cancel(); stream.Close() })
	ms = stream.VFS()
	if ms == nil {
		t.Fatal("want VFS on next turn from remaining recipes")
	}
	got, err = ms.ReadFile(ctx, "/workspace/ok/a.txt")
	if err != nil || string(got) != "one" {
		t.Fatalf("next turn uses remaining recipes = %q err=%v", got, err)
	}
}

func TestACP_vfsRefreshUnbindRejects(t *testing.T) {
	auth := vfs.NewSessionAuth()
	spec, _ := driveAgent(t, auth, newACPMemDrive())
	r := NewRegistry(testStore(t), "default", WithVFSProjection(DirectProjection{}), WithVFSAuth(auth))
	r.Register("default", spec)
	s := newACPTestServer(t, r)
	sessionID, _ := acpRPCResult(t, s.rpc(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{}}`))["sessionId"].(string)

	if acpRPCError(t, s.rpc(`{"jsonrpc":"2.0","id":2,"method":"_tacklr/vfs/bind","params":{}}`)) == nil {
		t.Fatal("bind missing sessionId")
	}
	if acpRPCError(t, s.rpc(`{"jsonrpc":"2.0","id":3,"method":"_tacklr/vfs/bind","params":{"sessionId":"`+sessionID+`"}}`)) == nil {
		t.Fatal("bind missing backends")
	}
	if acpRPCError(t, s.rpc(`{"jsonrpc":"2.0","id":4,"method":"_tacklr/vfs/bind","params":"nope"}`)) == nil {
		t.Fatal("bind bad params")
	}
	if acpRPCError(t, s.rpc(`{"jsonrpc":"2.0","id":5,"method":"_tacklr/vfs/refresh","params":{"sessionId":"`+sessionID+`"}}`)) == nil {
		t.Fatal("refresh missing provider")
	}
	if acpRPCError(t, s.rpc(`{"jsonrpc":"2.0","id":6,"method":"_tacklr/vfs/refresh","params":"nope"}`)) == nil {
		t.Fatal("refresh bad params")
	}
	if acpRPCError(t, s.rpc(`{"jsonrpc":"2.0","id":7,"method":"_tacklr/vfs/unbind","params":{}}`)) == nil {
		t.Fatal("unbind missing sessionId")
	}
	if acpRPCError(t, s.rpc(`{"jsonrpc":"2.0","id":8,"method":"_tacklr/vfs/unbind","params":"nope"}`)) == nil {
		t.Fatal("unbind bad params")
	}

	bind, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 9, "method": methodVFSBind,
		"params": map[string]any{
			"sessionId": sessionID,
			"backends": []map[string]any{
				{"profile": "gdrive", "point": "/ok", "auth": map[string]string{"token": "t"}, "params": map[string]string{"folderId": "root-a"}},
			},
		},
	})
	res := acpRPCResult(t, s.rpc(string(bind)))
	mounted, _ := res["mounted"].([]any)
	if len(mounted) != 1 {
		t.Fatalf("profile alias bind = %#v", res)
	}

	if acpRPCError(t, s.rpc(`{"jsonrpc":"2.0","id":10,"method":"_tacklr/vfs/refresh","params":{"sessionId":"`+sessionID+`","provider":"dropbox","auth":{"token":"x"}}}`)) == nil {
		t.Fatal("refresh unknown provider")
	}
	if acpRPCError(t, s.rpc(`{"jsonrpc":"2.0","id":11,"method":"_tacklr/vfs/unbind","params":{"sessionId":"`+sessionID+`"}}`)) == nil {
		t.Fatal("unbind missing point and provider")
	}
	if acpRPCError(t, s.rpc(`{"jsonrpc":"2.0","id":12,"method":"_tacklr/vfs/refresh","params":{"sessionId":"nope","provider":"gdrive","auth":{"token":"x"}}}`)) == nil {
		t.Fatal("refresh unknown session")
	}
	if acpRPCError(t, s.rpc(`{"jsonrpc":"2.0","id":13,"method":"_tacklr/vfs/unbind","params":{"sessionId":"nope","point":"/ok"}}`)) == nil {
		t.Fatal("unbind unknown session")
	}
}
