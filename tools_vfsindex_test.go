package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

func vfsIndexHarness(t *testing.T, withNS bool) (*AgentHarness, *vfs.MountSession, *brain.Engine, uuid.UUID) {
	t.Helper()
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("vfs-idx-tools", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	opts := AgentOptions{
		SessionID:    "vfs-idx-tools",
		Store:        stores.NewInMemoryStore(),
		MountSession: ms,
		FSRegistry:   reg,
		Model:        &mockStrategy{},
		Brain:        eng,
	}
	if withNS {
		opts.SearchNamespace = &ns
	}
	h := NewAgent(ctx, opts)
	t.Cleanup(h.Close)
	return h, ms, eng, ns
}

func activatePlan(t *testing.T, h *AgentHarness) {
	t.Helper()
	h.session.Plan().Set([]Todo{{Title: "t", Description: "d", Status: streaming.TodoStatusPending}})
	if !h.session.HasActivePlan() {
		t.Fatal("plan not active")
	}
}

func runWriteTool(t *testing.T, h *AgentHarness, tool *Tool, argsJSON string) (string, error) {
	t.Helper()
	out, _, err := h.toolRunner.Run(context.Background(), ToolInvocation{
		Tool:     tool,
		ArgsJSON: argsJSON,
		Runtime:  turnRuntime(h),
	})
	return out, err
}

func waitSearchHit(t *testing.T, eng *brain.Engine, scope brain.Scope, query string, timeout time.Duration) brain.RichObject {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: query}, brain.NewSearchContext())
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Objects) > 0 {
			return page.Objects[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("search %q: no hit within %v", query, timeout)
	return brain.RichObject{}
}

// TestVFSIndexTools_indexSearchUnindex: index_file → searchable vfs_path;
// hash skip; unindex soft-deletes mirror; VFS file remains.
// WriteFile may race with AfterPersist async index, so index_file may return
// indexed or skipped; search + unindex outcomes are the contract.
func TestVFSIndexTools_indexSearchUnindex(t *testing.T) {
	h, ms, eng, ns := vfsIndexHarness(t, true)
	activatePlan(t, h)
	ctx := context.Background()
	scope := brain.Scope{Namespace: &ns}

	indexTool := h.findTool("index_file", "")
	unindexTool := h.findTool("unindex", "")
	if indexTool == nil || unindexTool == nil {
		t.Fatal("index_file and unindex required when Brain+VFS+ns")
	}

	body := "alpha line\nbeta TODO findme-xyz\ngamma\n"
	if err := ms.WriteFile(ctx, "/work/note.txt", []byte(body)); err != nil {
		t.Fatal(err)
	}

	out, err := runWriteTool(t, h, indexTool, `{"path":"/work/note.txt"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "indexed path=/work/note.txt") && !strings.Contains(out, "skipped path=/work/note.txt") {
		t.Fatalf("index: %q", out)
	}

	hit := waitSearchHit(t, eng, scope, "findme-xyz", 3*time.Second)
	if hit.Properties[vfsindex.PropVFSPath] != "/work/note.txt" {
		t.Fatalf("vfs_path: %+v", hit.Properties)
	}

	// Second index same hash → skipped (async or explicit already wrote).
	out, err = runWriteTool(t, h, indexTool, `{"path":"/work/note.txt"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "skipped path=/work/note.txt") {
		t.Fatalf("skip: %q", out)
	}

	out, err = runWriteTool(t, h, unindexTool, `{"path":"/work/note.txt"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "unindexed path=/work/note.txt") {
		t.Fatalf("unindex: %q", out)
	}
	if h.vfsIndexSched == nil || h.vfsIndexSched.Indexer == nil {
		t.Fatal("harness should own AsyncScheduler+Indexer")
	}
	parentID := h.vfsIndexSched.Indexer.DocumentID("/work/note.txt")
	if _, err := eng.Read(ctx, scope, parentID); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("want soft-deleted, got %v", err)
	}
	if _, err := ms.Stat(ctx, "/work/note.txt"); err != nil {
		t.Fatal("VFS file must remain after unindex")
	}

	out, err = runWriteTool(t, h, unindexTool, `{"path":"/work/note.txt"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "noop path=/work/note.txt") {
		t.Fatalf("noop: %q", out)
	}
}

// TestVFSIndexTools_batchPathsAndGuards: multi-path success (≤8), max-8 no-partial,
// directory rejected before any IndexPath.
func TestVFSIndexTools_batchPathsAndGuards(t *testing.T) {
	h, ms, eng, ns := vfsIndexHarness(t, true)
	activatePlan(t, h)
	ctx := context.Background()
	scope := brain.Scope{Namespace: &ns}
	indexTool := h.findTool("index_file", "")
	if indexTool == nil {
		t.Fatal("index_file required")
	}

	if err := ms.WriteFile(ctx, "/work/a.txt", []byte("token-alpha-batch\n")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/work/b.txt", []byte("token-beta-batch\n")); err != nil {
		t.Fatal(err)
	}

	args, err := json.Marshal(map[string]any{"paths": []string{"/work/a.txt", "/work/b.txt"}})
	if err != nil {
		t.Fatal(err)
	}
	out, err := runWriteTool(t, h, indexTool, string(args))
	if err != nil {
		t.Fatal(err)
	}
	// AfterPersist may skip either path; both must appear with compact status.
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 status lines, got %q", out)
	}
	for _, p := range []string{"/work/a.txt", "/work/b.txt"} {
		var ok bool
		for _, line := range lines {
			if strings.Contains(line, "path="+p) &&
				(strings.HasPrefix(line, "indexed ") || strings.HasPrefix(line, "skipped ")) {
				ok = true
				break
			}
		}
		if !ok {
			t.Fatalf("missing status for %s in %q", p, out)
		}
	}
	hitA := waitSearchHit(t, eng, scope, "token-alpha-batch", 3*time.Second)
	if hitA.Properties[vfsindex.PropVFSPath] != "/work/a.txt" {
		t.Fatalf("a vfs_path: %+v", hitA.Properties)
	}
	hitB := waitSearchHit(t, eng, scope, "token-beta-batch", 3*time.Second)
	if hitB.Properties[vfsindex.PropVFSPath] != "/work/b.txt" {
		t.Fatalf("b vfs_path: %+v", hitB.Properties)
	}

	// Oversize batch: error, no partial (paths length 9).
	paths := make([]string, 9)
	for i := range paths {
		p := fmt.Sprintf("/work/f%d.txt", i)
		paths[i] = p
		if err := ms.WriteFile(ctx, p, []byte(fmt.Sprintf("body %d\n", i))); err != nil {
			t.Fatal(err)
		}
	}
	args9, _ := json.Marshal(map[string]any{"paths": paths})
	_, err = runWriteTool(t, h, indexTool, string(args9))
	if err == nil || !strings.Contains(err.Error(), "at most 8") || !strings.Contains(err.Error(), "no files indexed") {
		t.Fatalf("want max-8 no-partial error, got %v", err)
	}

	_, err = runWriteTool(t, h, indexTool, `{"path":"/work"}`)
	if err == nil || !strings.Contains(err.Error(), "directory") {
		t.Fatalf("want directory error, got %v", err)
	}

	// Empty args / missing path
	_, err = runWriteTool(t, h, indexTool, `{}`)
	if err == nil || !strings.Contains(err.Error(), "path or paths is required") {
		t.Fatalf("want required path error, got %v", err)
	}
	// Relative path rejected
	_, err = runWriteTool(t, h, indexTool, `{"path":"work/a.txt"}`)
	if err == nil {
		t.Fatal("want relative path error")
	}
	// Missing file
	_, err = runWriteTool(t, h, indexTool, `{"path":"/work/missing.txt"}`)
	if err == nil {
		t.Fatal("want missing file error")
	}
	// path + paths combined (still under max)
	if err := ms.WriteFile(ctx, "/work/c.txt", []byte("token-gamma-batch\n")); err != nil {
		t.Fatal(err)
	}
	argsCombo, _ := json.Marshal(map[string]any{
		"path":  "/work/c.txt",
		"paths": []string{"/work/a.txt"},
	})
	out, err = runWriteTool(t, h, indexTool, string(argsCombo))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "path=/work/c.txt") || !strings.Contains(out, "path=/work/a.txt") {
		t.Fatalf("path+paths: %q", out)
	}
	_ = waitSearchHit(t, eng, scope, "token-gamma-batch", 3*time.Second)
}

// TestVFSIndexTools_planGate: write tools locked until create_plan.
func TestVFSIndexTools_planGate(t *testing.T) {
	h, ms, _, _ := vfsIndexHarness(t, true)
	ctx := context.Background()
	if err := ms.WriteFile(ctx, "/work/a.txt", []byte("x\n")); err != nil {
		t.Fatal(err)
	}
	_, err := runWriteTool(t, h, h.findTool("index_file", ""), `{"path":"/work/a.txt"}`)
	if err == nil || !errors.Is(err, ErrToolPermissionDenied) {
		t.Fatalf("want plan lock, got %v", err)
	}
	_, err = runWriteTool(t, h, h.findTool("unindex", ""), `{"path":"/work/a.txt"}`)
	if err == nil || !errors.Is(err, ErrToolPermissionDenied) {
		t.Fatalf("want plan lock unindex, got %v", err)
	}
}

// TestVFSIndexTools_asyncReindexAfterWriteFile: persist triggers async reindex.
func TestVFSIndexTools_asyncReindexAfterWriteFile(t *testing.T) {
	h, ms, eng, ns := vfsIndexHarness(t, true)
	activatePlan(t, h)
	ctx := context.Background()
	scope := brain.Scope{Namespace: &ns}

	if err := ms.WriteFile(ctx, "/work/live.txt", []byte("version one phrase\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := runWriteTool(t, h, h.findTool("index_file", ""), `{"path":"/work/live.txt"}`); err != nil {
		t.Fatal(err)
	}

	if err := ms.WriteFile(ctx, "/work/live.txt", []byte("version two asyncphrase42\n")); err != nil {
		t.Fatal(err)
	}

	_ = waitSearchHit(t, eng, scope, "asyncphrase42", 3*time.Second)
}

// TestVFSIndexTools_hostAfterPersistComposed: existing host hook still runs.
func TestVFSIndexTools_hostAfterPersistComposed(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("compose-hook", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	var hostSaw string
	ms.SetAfterPersist(func(ctx context.Context, path string) error {
		hostSaw = path
		return nil
	})

	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx, vfsindex.MountIndexKinds()...); err != nil {
		t.Fatal(err)
	}
	ns := uuid.New()
	h := NewAgent(ctx, AgentOptions{
		SessionID: "compose-hook", Store: stores.NewInMemoryStore(),
		MountSession: ms, FSRegistry: reg, Model: &mockStrategy{},
		Brain: eng, SearchNamespace: &ns,
	})
	t.Cleanup(h.Close)

	if err := ms.WriteFile(ctx, "/work/z.txt", []byte("z\n")); err != nil {
		t.Fatal(err)
	}
	if hostSaw != "/work/z.txt" {
		t.Fatalf("host AfterPersist not composed: saw %q", hostSaw)
	}
}
