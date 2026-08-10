package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestVFSTools_readWriteRev: agent tools (no vfs_ prefix) + rev gate + path ops.
func TestVFSTools_readWriteRev(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("tools-vfs", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/work/a.go", []byte("package a\n// old\nfunc A() {}\n")); err != nil {
		t.Fatal(err)
	}

	h := NewAgent(ctx, AgentOptions{
		SessionID:    "tools-vfs",
		Store:        stores.NewInMemoryStore(),
		MountSession: ms,
		FSRegistry:   reg,
		Model:        &mockStrategy{},
	})
	tools := map[string]*Tool{}
	for _, tool := range h.tools {
		switch tool.Name {
		case "list", "stat", "read_lines", "replace_lines", "replace_text", "write", "mkdir", "remove":
			tools[tool.Name] = tool
		case "vfs_list", "vfs_read_lines", "vfs_write":
			t.Fatalf("legacy vfs_ tool still registered: %s", tool.Name)
		}
	}
	for _, name := range []string{"list", "stat", "read_lines", "replace_lines", "replace_text", "write", "mkdir", "remove"} {
		if tools[name] == nil {
			t.Fatalf("missing tool %q", name)
		}
	}

	rt := turnRuntime(h)

	// list / stat
	res, err := tools["list"].invoke(ctx, `{"path":"/work"}`, rt)
	if err != nil || !strings.Contains(res.output, "a.go") {
		t.Fatalf("list: %q err=%v", res.output, err)
	}
	res, err = tools["stat"].invoke(ctx, `{"path":"/work/a.go"}`, rt)
	if err != nil || !strings.Contains(res.output, "is_dir=false") {
		t.Fatalf("stat: %q err=%v", res.output, err)
	}

	// read_lines + rev
	res, err = tools["read_lines"].invoke(ctx, `{"path":"/work/a.go","start":1,"end":10}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	rev := fieldKV(res.output, "rev")
	if rev == "" || !strings.Contains(res.output, "1|package a") {
		t.Fatalf("read_lines: %s", res.output)
	}

	// stale
	_, err = tools["replace_lines"].invoke(ctx, `{"path":"/work/a.go","rev":"dead","start":2,"end":3,"lines":["// x"]}`, rt)
	if err == nil || !errors.Is(err, vfs.ErrStaleContent) {
		t.Fatalf("stale: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"path": "/work/a.go", "rev": rev, "start": 2, "end": 3, "lines": []string{"// new"},
	})
	res, err = tools["replace_lines"].invoke(ctx, string(body), rt)
	if err != nil {
		t.Fatal(err)
	}
	rev2 := fieldKV(res.output, "rev")
	if rev2 == "" || rev2 == rev {
		t.Fatalf("replace_lines rev: %s", res.output)
	}

	body, _ = json.Marshal(map[string]any{
		"path": "/work/a.go", "rev": rev2, "old": "func A() {}", "new": "func A() { return }",
	})
	res, err = tools["replace_text"].invoke(ctx, string(body), rt)
	if err != nil {
		t.Fatal(err)
	}
	rev3 := fieldKV(res.output, "rev")
	if !strings.Contains(res.output, "replacements=1") {
		t.Fatalf("replace_text: %s", res.output)
	}

	// replace_all path
	if err := ms.WriteFile(ctx, "/work/dup.txt", []byte("aa aa\n")); err != nil {
		t.Fatal(err)
	}
	r, err := ms.ContentRev(ctx, "/work/dup.txt")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = json.Marshal(map[string]any{
		"path": "/work/dup.txt", "rev": r.Hash, "old": "aa", "new": "bb", "replace_all": true,
	})
	res, err = tools["replace_text"].invoke(ctx, string(body), rt)
	if err != nil || !strings.Contains(res.output, "replacements=2") {
		t.Fatalf("replace_all: %q err=%v", res.output, err)
	}

	// write requires rev when exists
	_, err = tools["write"].invoke(ctx, `{"path":"/work/a.go","content":"x\n"}`, rt)
	if err == nil {
		t.Fatal("write without rev")
	}
	body, _ = json.Marshal(map[string]any{"path": "/work/a.go", "rev": rev3, "content": "package a\n"})
	res, err = tools["write"].invoke(ctx, string(body), rt)
	if err != nil || fieldKV(res.output, "rev") == "" {
		t.Fatalf("write: %q err=%v", res.output, err)
	}

	// create, mkdir, remove
	res, err = tools["write"].invoke(ctx, `{"path":"/work/b.go","content":"package b\n"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tools["mkdir"].invoke(ctx, `{"path":"/work/sub"}`, rt); err != nil {
		t.Fatal(err)
	}
	if _, err := tools["remove"].invoke(ctx, `{"path":"/work/b.go"}`, rt); err != nil {
		t.Fatal(err)
	}

	// bad path
	if _, err := tools["list"].invoke(ctx, `{"path":"rel"}`, rt); err == nil {
		t.Fatal("relative path")
	}
	if _, err := tools["read_lines"].invoke(ctx, `{"path":"/work/a.go","start":0,"end":1}`, rt); err == nil {
		t.Fatal("bad range")
	}
	if _, err := tools["replace_text"].invoke(ctx, `{"path":"/work/a.go","rev":"x","old":"","new":"y"}`, rt); err == nil {
		t.Fatal("empty old")
	}

	doc, err := ms.ReadText(ctx, "/work/a.go")
	if err != nil || doc.Text() != "package a\n" {
		t.Fatalf("body=%q err=%v", doc.Text(), err)
	}
}

func fieldKV(s, key string) string {
	prefix := key + "="
	line, _, _ := strings.Cut(s, "\n")
	for _, part := range strings.Fields(line) {
		if strings.HasPrefix(part, prefix) {
			return strings.TrimPrefix(part, prefix)
		}
	}
	return ""
}
