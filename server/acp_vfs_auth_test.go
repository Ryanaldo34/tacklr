package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/vfs"
)

type acpMemDrive struct {
	mu    sync.Mutex
	nodes map[string]acpMemNode
}

type acpMemNode struct {
	meta   vfs.DriveMeta
	parent string
	body   []byte
}

func newACPMemDrive() *acpMemDrive {
	d := &acpMemDrive{nodes: map[string]acpMemNode{}}
	d.nodes["root-a"] = acpMemNode{meta: vfs.DriveMeta{ID: "root-a", Name: "A", MimeType: "application/vnd.google-apps.folder", IsDir: true}}
	d.nodes["root-b"] = acpMemNode{meta: vfs.DriveMeta{ID: "root-b", Name: "B", MimeType: "application/vnd.google-apps.folder", IsDir: true}}
	d.nodes["f1"] = acpMemNode{parent: "root-a", meta: vfs.DriveMeta{ID: "f1", Name: "a.txt", MimeType: "text/plain", Size: 3}, body: []byte("one")}
	d.nodes["f2"] = acpMemNode{parent: "root-b", meta: vfs.DriveMeta{ID: "f2", Name: "b.txt", MimeType: "text/plain", Size: 3}, body: []byte("two")}
	return d
}

func (d *acpMemDrive) GetMeta(ctx context.Context, fileID string) (vfs.DriveMeta, error) {
	if err := ctx.Err(); err != nil {
		return vfs.DriveMeta{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	n, ok := d.nodes[fileID]
	if !ok {
		return vfs.DriveMeta{}, vfs.ErrNotExist
	}
	return n.meta, nil
}

func (d *acpMemDrive) GetMedia(ctx context.Context, fileID string) (io.ReadCloser, int64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	n, ok := d.nodes[fileID]
	if !ok {
		return nil, 0, vfs.ErrNotExist
	}
	return io.NopCloser(strings.NewReader(string(n.body))), int64(len(n.body)), nil
}

func (d *acpMemDrive) Export(ctx context.Context, fileID, mimeType string) (io.ReadCloser, int64, error) {
	_ = mimeType
	return nil, 0, vfs.ErrNotSupported
}

func (d *acpMemDrive) PutMedia(ctx context.Context, fileID, mediaMIME string, r io.Reader, size int64) (vfs.DriveMeta, error) {
	if err := ctx.Err(); err != nil {
		return vfs.DriveMeta{}, err
	}
	data, err := io.ReadAll(io.LimitReader(r, size+1))
	if err != nil {
		return vfs.DriveMeta{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	n, ok := d.nodes[fileID]
	if !ok {
		return vfs.DriveMeta{}, vfs.ErrNotExist
	}
	n.body = data
	n.meta.Size = int64(len(data))
	if mediaMIME != "" {
		n.meta.MimeType = mediaMIME
	}
	d.nodes[fileID] = n
	return n.meta, nil
}

func (d *acpMemDrive) Create(ctx context.Context, parentID, name, metadataMIME, mediaMIME string, r io.Reader, size int64) (vfs.DriveMeta, error) {
	_ = parentID
	_ = name
	_ = metadataMIME
	_ = mediaMIME
	_ = r
	_ = size
	return vfs.DriveMeta{}, vfs.ErrNotSupported
}

func (d *acpMemDrive) Trash(ctx context.Context, fileID string) error {
	_ = fileID
	return vfs.ErrNotSupported
}

func (d *acpMemDrive) Mkdir(ctx context.Context, parentID, name string) (vfs.DriveMeta, error) {
	_ = parentID
	_ = name
	return vfs.DriveMeta{}, vfs.ErrNotSupported
}

func (d *acpMemDrive) List(ctx context.Context, folderID string) ([]vfs.DriveMeta, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.nodes[folderID]; !ok {
		return nil, vfs.ErrNotExist
	}
	var out []vfs.DriveMeta
	for _, n := range d.nodes {
		if n.parent == folderID {
			out = append(out, n.meta)
		}
	}
	return out, nil
}

func driveAgent(t *testing.T, auth *vfs.SessionAuth, api vfs.DriveAPI) (AgentSpec, *vfs.BackendRegistry) {
	t.Helper()
	fsReg := vfs.NewBackendRegistry()
	if err := fsReg.Register(vfs.DriveFactory{ID: "gdrive", Auth: auth, API: api}); err != nil {
		t.Fatal(err)
	}
	if err := fsReg.Register(vfs.GraphFactory{ID: vfs.ProviderMicrosoft, Auth: auth}); err != nil {
		t.Fatal(err)
	}
	return AgentSpec{
		Options: tacklr.AgentOptions{
			Config: tacklr.Config{MaxWindowSize: 8192},
			Model:  okModel(),
		},
		FSRegistry: fsReg,
	}, fsReg
}

func TestACP_vfsBindRefreshUnbind(t *testing.T) {
	auth := vfs.NewSessionAuth()
	api := newACPMemDrive()
	spec, _ := driveAgent(t, auth, api)
	r := NewRegistry(testStore(t), "default", WithVFSProjection(DirectProjection{}), WithVFSAuth(auth))
	r.Register("default", spec)
	s := newACPTestServer(t, r)

	initRec := s.rpc(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}`)
	initRes := acpRPCResult(t, initRec)
	caps, _ := initRes["agentCapabilities"].(map[string]any)
	meta, _ := caps["_meta"].(map[string]any)
	tacklrMeta, _ := meta["tacklr"].(map[string]any)
	vfsCap, _ := tacklrMeta["vfs"].(map[string]any)
	if vfsCap["credentials"] != true || vfsCap["tokenRefresh"] != true || vfsCap["writable"] != true {
		t.Fatalf("vfs cap = %#v", vfsCap)
	}
	provs, _ := vfsCap["providers"].([]any)
	have := map[string]bool{}
	for _, p := range provs {
		s, _ := p.(string)
		have[s] = true
	}
	if !have["gdrive"] || !have[vfs.ProviderMicrosoft] {
		t.Fatalf("providers = %#v", vfsCap["providers"])
	}

	newRec := s.rpc(`{"jsonrpc":"2.0","id":2,"method":"session/new","params":{"cwd":"/tmp"}}`)
	sessionID, _ := acpRPCResult(t, newRec)["sessionId"].(string)
	if sessionID == "" {
		t.Fatal("missing sessionId")
	}

	secret := "never-persist-this-token"
	expiresAt := time.Now().UTC().Add(30 * time.Minute).Truncate(time.Second)
	bindBody, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 3, "method": methodVFSBind,
		"params": map[string]any{
			"sessionId": sessionID,
			"backends": []map[string]any{
				{"provider": "gdrive", "point": "/contracts", "auth": map[string]any{"scheme": "bearer", "token": secret, "expiresAt": expiresAt}, "params": map[string]string{"folderId": "root-a"}},
				{"provider": "gdrive", "point": "/workspace", "auth": map[string]any{"token": secret, "expiresAt": expiresAt}, "params": map[string]string{"name": "legal", "folderId": "root-b"}},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	bindRes := acpRPCResult(t, s.rpc(string(bindBody)))
	mounted, _ := bindRes["mounted"].([]any)
	if len(mounted) != 2 {
		t.Fatalf("mounted = %#v errors=%v", bindRes["mounted"], bindRes["errors"])
	}
	for _, item := range mounted {
		m, _ := item.(map[string]any)
		if m["point"] != vfs.WorkspacePoint {
			t.Fatalf("mounted.point = %#v", item)
		}
	}

	raw, err := s.wire.Get(t.Context(), sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), secret) {
		t.Fatalf("wire envelope stored token: %s", raw)
	}
	if credential, ok := auth.Credential(sessionID, "gdrive"); !ok || !credential.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("credential expiry = %v ok=%v", credential.ExpiresAt, ok)
	}

	stream, err := r.RunTurn(t.Context(), TurnRequest{
		SessionID: sessionID, AgentID: "default", ThreadID: sessionID, Prompt: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Cancel(); stream.Close() })
	ms := stream.VFS()
	if ms == nil {
		t.Fatal("want VFS after bind")
	}
	ents, err := ms.ReadDir(t.Context(), vfs.WorkspacePoint)
	if err != nil || len(ents) != 2 || ents[0].Name != "contracts" || ents[1].Name != "legal" {
		t.Fatalf("ReadDir /workspace = %+v err=%v", ents, err)
	}
	got, err := ms.ReadFile(t.Context(), "/workspace/contracts/a.txt")
	if err != nil || string(got) != "one" {
		t.Fatalf("read contracts = %q err=%v", got, err)
	}
	if _, err := ms.ReadFile(t.Context(), "/contracts/a.txt"); !errors.Is(err, vfs.ErrNotMounted) {
		t.Fatalf("old /contracts path: %v", err)
	}
	if err := ms.WriteFile(t.Context(), "/workspace/contracts/a.txt", []byte("nope")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("omit readOnly must deny write: %v", err)
	}
	got, err = ms.ReadFile(t.Context(), "/workspace/contracts/a.txt")
	if err != nil || string(got) != "one" {
		t.Fatalf("body after denied write = %q err=%v", got, err)
	}
	got, err = ms.ReadFile(t.Context(), "/workspace/legal/b.txt")
	if err != nil || string(got) != "two" {
		t.Fatalf("read legal = %q err=%v", got, err)
	}

	refreshBody, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 4, "method": methodVFSRefresh,
		"params": map[string]any{"sessionId": sessionID, "provider": "gdrive", "auth": map[string]string{"token": "rotated"}},
	})
	_ = acpRPCResult(t, s.rpc(string(refreshBody)))
	if tok, ok := auth.Credential(sessionID, "gdrive"); !ok || tok.Token != "rotated" {
		t.Fatalf("refresh credential = %+v ok=%v", tok, ok)
	}

	unbindBody, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 5, "method": methodVFSUnbind,
		"params": map[string]any{"sessionId": sessionID, "point": vfs.WorkspacePoint, "name": "legal"},
	})
	_ = acpRPCResult(t, s.rpc(string(unbindBody)))
	drainTurn(t, stream)
	stream, err = r.RunTurn(t.Context(), TurnRequest{
		SessionID: sessionID, AgentID: "default", ThreadID: sessionID, Prompt: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Cancel(); stream.Close() })
	ms = stream.VFS()
	if _, err := ms.ReadFile(t.Context(), "/workspace/legal/b.txt"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("unbound legal: %v", err)
	}
	if _, err := ms.ReadFile(t.Context(), "/workspace/contracts/a.txt"); err != nil {
		t.Fatalf("contracts after unbind legal: %v", err)
	}

	_ = acpRPCResult(t, s.rpc(`{"jsonrpc":"2.0","id":6,"method":"session/close","params":{"sessionId":"`+sessionID+`"}}`))
	if auth.HasBindings(sessionID) {
		t.Fatal("close must clear tokens")
	}
}

func TestACP_vfsBindWritable(t *testing.T) {
	auth := vfs.NewSessionAuth()
	api := newACPMemDrive()
	spec, _ := driveAgent(t, auth, api)
	r := NewRegistry(testStore(t), "default", WithVFSProjection(DirectProjection{}), WithVFSAuth(auth))
	r.Register("default", spec)
	s := newACPTestServer(t, r)
	sessionID, _ := acpRPCResult(t, s.rpc(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{}}`))["sessionId"].(string)
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 2, "method": methodVFSBind,
		"params": map[string]any{
			"sessionId": sessionID,
			"backends": []map[string]any{
				{"provider": "gdrive", "point": "/w", "readOnly": false, "auth": map[string]string{"token": "t"}, "params": map[string]string{"folderId": "root-a"}},
			},
		},
	})
	res := acpRPCResult(t, s.rpc(string(body)))
	mounted, _ := res["mounted"].([]any)
	if len(mounted) != 1 {
		t.Fatalf("mounted=%v errors=%v", res["mounted"], res["errors"])
	}
	binds := auth.Bindings(sessionID)
	if len(binds) != 1 || !binds[0].Writable {
		t.Fatalf("writable bind = %+v", binds)
	}
	stream, err := r.RunTurn(t.Context(), TurnRequest{
		SessionID: sessionID, AgentID: "default", ThreadID: sessionID, Prompt: "hi",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stream.Cancel(); stream.Close() })
	ms := stream.VFS()
	if ms == nil {
		t.Fatal("want VFS after writable bind")
	}
	if err := ms.WriteFile(t.Context(), "/workspace/w/a.txt", []byte("two")); err != nil {
		t.Fatal(err)
	}
	got, err := ms.ReadFile(t.Context(), "/workspace/w/a.txt")
	if err != nil || string(got) != "two" {
		t.Fatalf("writable write = %q err=%v", got, err)
	}
}

func TestACP_vfsBindRejects(t *testing.T) {
	auth := vfs.NewSessionAuth()
	spec, _ := driveAgent(t, auth, newACPMemDrive())
	r := NewRegistry(testStore(t), "default", WithVFSProjection(DirectProjection{}), WithVFSAuth(auth))
	r.Register("default", spec)
	s := newACPTestServer(t, r)
	sessionID, _ := acpRPCResult(t, s.rpc(`{"jsonrpc":"2.0","id":1,"method":"session/new","params":{}}`))["sessionId"].(string)

	// Unknown session.
	errObj := acpRPCError(t, s.rpc(`{"jsonrpc":"2.0","id":2,"method":"_tacklr/vfs/bind","params":{"sessionId":"nope","backends":[{"provider":"gdrive","point":"/a","auth":{"token":"t"},"params":{"folderId":"root-a"}}]}}`))
	if errObj == nil {
		t.Fatal("want session error")
	}

	// Per-item errors: missing folder, multi-segment, unknown profile.
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0", "id": 3, "method": methodVFSBind,
		"params": map[string]any{
			"sessionId": sessionID,
			"backends": []map[string]any{
				{"provider": "gdrive", "point": "/ok", "auth": map[string]string{"token": "t"}, "params": map[string]string{"folderId": "root-a"}},
				{"provider": "gdrive", "point": "/a/b", "auth": map[string]string{"token": "t"}, "params": map[string]string{"folderId": "root-a"}},
				{"provider": "dropbox", "point": "/drop", "auth": map[string]string{"token": "t"}},
				{"provider": "gdrive", "point": "/missing", "auth": map[string]string{"token": "t"}, "params": map[string]string{"folderId": "nope"}},
			},
		},
	})
	res := acpRPCResult(t, s.rpc(string(body)))
	mounted, _ := res["mounted"].([]any)
	errs, _ := res["errors"].([]any)
	if len(mounted) != 1 || len(errs) != 3 {
		t.Fatalf("mounted=%v errors=%v", res["mounted"], res["errors"])
	}
}

func TestACP_vfsTokenRefreshCall(t *testing.T) {
	w := &recordingMessageWriter{}
	bridge := NewClientBridge(w)
	bridge.SetCaps(ClientCapabilities{VFSTokenRefresh: true})
	fn := vfsTokenRefresh(bridge, "sess", "gdrive")
	expiresAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)

	done := make(chan error, 1)
	go func() {
		cred, err := fn(t.Context())
		if err != nil {
			done <- err
			return
		}
		if cred.Token != "from-client" || !cred.ExpiresAt.Equal(expiresAt) {
			done <- errors.New("unexpected token")
			return
		}
		done <- nil
	}()

	deadline := time.Now().Add(2 * time.Second)
	replied := false
	for time.Now().Before(deadline) && !replied {
		for _, f := range w.SnapshotFrames() {
			var msg map[string]any
			if json.Unmarshal(f, &msg) != nil || msg["method"] != methodVFSToken {
				continue
			}
			reply, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0", "id": msg["id"],
				"result": map[string]any{"token": "from-client", "expiresAt": expiresAt},
			})
			if !bridge.TryCompleteResponse(reply) {
				t.Fatal("did not complete token response")
			}
			replied = true
			break
		}
		if !replied {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if !replied {
		t.Fatal("client never received _tacklr/vfs/token")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	bridge.SetCaps(ClientCapabilities{})
	if _, err := vfsTokenRefresh(bridge, "sess", "gdrive")(t.Context()); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("no cap: %v", err)
	}

	bridge.SetCaps(ClientCapabilities{VFSTokenRefresh: true})
	emptyDone := make(chan error, 1)
	go func() {
		_, err := vfsTokenRefresh(bridge, "sess", "gdrive")(t.Context())
		emptyDone <- err
	}()
	replied = false
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !replied {
		for _, f := range w.SnapshotFrames() {
			var msg map[string]any
			if json.Unmarshal(f, &msg) != nil || msg["method"] != methodVFSToken {
				continue
			}
			reply, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": msg["id"], "result": map[string]string{"token": ""}})
			if bridge.TryCompleteResponse(reply) {
				replied = true
				break
			}
		}
		if !replied {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if err := <-emptyDone; !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("empty token: %v", err)
	}
}
