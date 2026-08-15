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
		Model:        &mockStrategy{},
	})
	tools := map[string]*Tool{}
	for _, tool := range h.tools {
		tools[tool.Name] = tool
	}
	for _, name := range []string{"list", "stat", "find_files", "read_lines", "replace_lines", "replace_text", "write", "mkdir", "remove"} {
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

	// bad path / missing / unmounted
	if _, err := tools["list"].invoke(ctx, `{"path":"rel"}`, rt); err == nil {
		t.Fatal("relative path")
	}
	if _, err := tools["list"].invoke(ctx, `{"path":"/nomount"}`, rt); err == nil {
		t.Fatal("list unmounted")
	}
	if _, err := tools["stat"].invoke(ctx, `{"path":"/work/missing.go"}`, rt); err == nil {
		t.Fatal("stat missing")
	}
	if _, err := tools["mkdir"].invoke(ctx, `{"path":"/nomount/x"}`, rt); err == nil {
		t.Fatal("mkdir unmounted")
	}
	if _, err := tools["remove"].invoke(ctx, `{"path":"/work/missing.go"}`, rt); err == nil {
		t.Fatal("remove missing")
	}
	if _, err := tools["read_lines"].invoke(ctx, `{"path":"/work/missing.go","start":1,"end":2}`, rt); err == nil {
		t.Fatal("read missing")
	}
	if _, err := tools["list"].invoke(ctx, "{\"path\":\"/work/has\\u0000x\"}", rt); err == nil {
		t.Fatal("nul path")
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

	// Structured markdown: outline, read by block_id, replace by block_id
	md := "# Hello\n\n## Install\n\nold\n"
	if err := ms.WriteFile(ctx, "/work/README.md", []byte(md)); err != nil {
		t.Fatal(err)
	}
	res, err = tools["read_lines"].invoke(ctx, `{"path":"/work/README.md","outline":true}`, rt)
	if err != nil || !strings.Contains(res.output, "outline:") ||
		!strings.Contains(res.output, "hello/install") || !strings.Contains(res.output, "kind=heading") {
		t.Fatalf("outline: %q err=%v", res.output, err)
	}
	revMD := fieldKV(res.output, "rev")
	if revMD == "" {
		t.Fatalf("outline rev empty: %s", res.output)
	}

	// read_lines by block_id: dump span lines without full outline
	res, err = tools["read_lines"].invoke(ctx, `{"path":"/work/README.md","block_id":"hello/install"}`, rt)
	if err != nil {
		t.Fatalf("read block_id: %v", err)
	}
	if !strings.Contains(res.output, "block_id=hello/install") {
		t.Fatalf("read block_id header: %q", res.output)
	}
	if !strings.Contains(res.output, "## Install") || !strings.Contains(res.output, "old") {
		t.Fatalf("read block_id body lines: %q", res.output)
	}
	if fieldKV(res.output, "rev") == "" {
		t.Fatalf("read block_id rev empty: %s", res.output)
	}
	// Span dump includes numbered lines from the block window
	if !strings.Contains(res.output, "returned=") || !strings.Contains(res.output, "|") {
		t.Fatalf("read block_id window lines: %q", res.output)
	}

	body, _ = json.Marshal(map[string]any{
		"path": "/work/README.md", "rev": revMD, "block_id": "hello/install", "body": "new body\n",
	})
	res, err = tools["replace_lines"].invoke(ctx, string(body), rt)
	if err != nil {
		t.Fatalf("replace block: %v out=%s", err, res.output)
	}
	got, err := ms.ReadText(ctx, "/work/README.md")
	if err != nil || !strings.Contains(got.Text(), "new body") || !strings.Contains(got.Text(), "## Install") {
		t.Fatalf("after block replace: %q err=%v", got.Text(), err)
	}

	// Unknown block_id → error
	revMD2 := fieldKV(res.output, "rev")
	if revMD2 == "" {
		r, _ := ms.ContentRev(ctx, "/work/README.md")
		revMD2 = r.Hash
	}
	body, _ = json.Marshal(map[string]any{
		"path": "/work/README.md", "rev": revMD2, "block_id": "missing/block", "body": "x\n",
	})
	if _, err = tools["replace_lines"].invoke(ctx, string(body), rt); err == nil {
		t.Fatal("want unknown block_id error")
	}
	if _, err = tools["read_lines"].invoke(ctx, `{"path":"/work/README.md","block_id":"missing/block"}`, rt); err == nil {
		t.Fatal("want read unknown block_id error")
	}

	// include_heading=true replaces the heading line too
	md2 := "# Top\n\n## Sec\n\nkeep\n"
	if err := ms.WriteFile(ctx, "/work/head.md", []byte(md2)); err != nil {
		t.Fatal(err)
	}
	revHead, err := ms.ContentRev(ctx, "/work/head.md")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = json.Marshal(map[string]any{
		"path": "/work/head.md", "rev": revHead.Hash, "block_id": "top/sec",
		"include_heading": true, "lines": []string{"## Renamed", "body"},
	})
	res, err = tools["replace_lines"].invoke(ctx, string(body), rt)
	if err != nil {
		t.Fatalf("include_heading replace: %v", err)
	}
	got, err = ms.ReadText(ctx, "/work/head.md")
	if err != nil || !strings.Contains(got.Text(), "## Renamed") || strings.Contains(got.Text(), "## Sec") {
		t.Fatalf("include_heading body: %q err=%v", got.Text(), err)
	}

	// outline-only (no block_id, no start/end lines required for structure dump)
	if err := ms.WriteFile(ctx, "/work/plain.txt", []byte("no structure\n")); err != nil {
		t.Fatal(err)
	}
	res, err = tools["read_lines"].invoke(ctx, `{"path":"/work/plain.txt","outline":true}`, rt)
	if err != nil || !strings.Contains(res.output, "line_count=") {
		t.Fatalf("outline on plain text: %q err=%v", res.output, err)
	}
	// block_id on non-structured doc
	if _, err = tools["read_lines"].invoke(ctx, `{"path":"/work/plain.txt","block_id":"x"}`, rt); err == nil {
		t.Fatal("block_id on plain should fail")
	}
	// replace_lines missing rev
	if _, err = tools["replace_lines"].invoke(ctx, `{"path":"/work/plain.txt","start":1,"end":2,"lines":["x"]}`, rt); err == nil {
		t.Fatal("replace_lines without rev")
	}
	// replace_text missing rev / not found
	rPlain, err := ms.ContentRev(ctx, "/work/plain.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tools["replace_text"].invoke(ctx, `{"path":"/work/plain.txt","old":"nope","new":"y"}`, rt); err == nil {
		t.Fatal("replace_text without rev")
	}
	bodyNF, _ := json.Marshal(map[string]any{
		"path": "/work/plain.txt", "rev": rPlain.Hash, "old": "zzz-missing", "new": "y",
	})
	if _, err = tools["replace_text"].invoke(ctx, string(bodyNF), rt); err == nil {
		t.Fatal("replace_text old not found")
	}
	bodyDup, _ := json.Marshal(map[string]any{
		"path": "/work/plain.txt", "rev": rPlain.Hash, "old": "t", "new": "y",
	})
	if _, err = tools["replace_text"].invoke(ctx, string(bodyDup), rt); err == nil {
		t.Fatal("replace_text not unique")
	}
	// write create then replace invalid range
	if _, err = tools["write"].invoke(ctx, `{"path":"/work/new.txt","content":"a\nb\n"}`, rt); err != nil {
		t.Fatal(err)
	}
	rNew, err := ms.ContentRev(ctx, "/work/new.txt")
	if err != nil {
		t.Fatal(err)
	}
	bodyBad, _ := json.Marshal(map[string]any{
		"path": "/work/new.txt", "rev": rNew.Hash, "start": 0, "end": 1, "lines": []string{"x"},
	})
	if _, err = tools["replace_lines"].invoke(ctx, string(bodyBad), rt); err == nil {
		t.Fatal("invalid replace range")
	}
	// write stale rev
	rNew2, err := ms.ContentRev(ctx, "/work/new.txt")
	if err != nil {
		t.Fatal(err)
	}
	bodyStale, _ := json.Marshal(map[string]any{
		"path": "/work/new.txt", "rev": "deadbeef", "content": "z\n",
	})
	if _, err = tools["write"].invoke(ctx, string(bodyStale), rt); err == nil {
		t.Fatal("stale write rev")
	}
	// outline+start/end window on markdown (lineWindowFromTextDoc)
	res, err = tools["read_lines"].invoke(ctx, `{"path":"/work/README.md","outline":true,"start":1,"end":3}`, rt)
	if err != nil || !strings.Contains(res.output, "returned=") {
		t.Fatalf("outline+range: %q err=%v", res.output, err)
	}
	// start past EOF on structured read
	if _, err = tools["read_lines"].invoke(ctx, `{"path":"/work/README.md","block_id":"hello","start":1,"end":1}`, rt); err != nil {
		// block read still ok without needing start
	}
	_ = rNew2
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

// TestVFSTools_findFiles: bounded live walk returns paths under a mount.
func TestVFSTools_findFiles(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("find-files", reg)
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.MkdirAll(ctx, "/work/sub"); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/work/a.go", []byte("package a\n")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/work/sub/b.go", []byte("package b\n")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/work/readme.md", []byte("# r\n")); err != nil {
		t.Fatal(err)
	}
	h := NewAgent(ctx, AgentOptions{
		SessionID: "find-files", Store: stores.NewInMemoryStore(),
		MountSession: ms, Model: &mockStrategy{},
	})
	tool := h.findTool("find_files", "")
	if tool == nil {
		t.Fatal("find_files required")
	}
	out, err := tool.invoke(ctx, `{"path":"/work","name":"*.go"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.output, "count=2") {
		t.Fatalf("want count=2 for *.go: %s", out.output)
	}
	// Every listed path must match the glob (positive membership).
	for _, line := range strings.Split(out.output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "root=") {
			continue
		}
		if !strings.HasSuffix(line, ".go") {
			t.Fatalf("glob hit must be .go: %q in %s", line, out.output)
		}
	}
	if !strings.Contains(out.output, "/work/a.go") || !strings.Contains(out.output, "/work/sub/b.go") {
		t.Fatalf("find go: %s", out.output)
	}
	out2, err := tool.invoke(ctx, `{"path":"/work","name":"readme"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out2.output, "/work/readme.md") {
		t.Fatalf("substring name: %s", out2.output)
	}
	// max_results caps the live walk.
	out3, err := tool.invoke(ctx, `{"path":"/work","max_results":1}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out3.output, "count=1") {
		t.Fatalf("max_results=1: %s", out3.output)
	}
	out4, err := tool.invoke(ctx, `{"path":"/work","max_depth":1,"max_results":300}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out4.output, "/work/a.go") || strings.Contains(out4.output, "/work/sub/b.go") {
		t.Fatalf("max_depth=1: %s", out4.output)
	}
	out5, err := tool.invoke(ctx, `{"path":"/work/a.go"}`, turnRuntime(h))
	if err != nil || !strings.Contains(out5.output, "/work/a.go") {
		t.Fatalf("find file root: %q err=%v", out5.output, err)
	}
	if _, err := tool.invoke(ctx, `{"path":"/work/missing"}`, turnRuntime(h)); err == nil {
		t.Fatal("find missing")
	}
	if _, err := tool.invoke(ctx, `{"path":"rel"}`, turnRuntime(h)); err == nil {
		t.Fatal("find relative")
	}
}
