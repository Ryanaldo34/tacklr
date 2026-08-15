package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestVFSTools_readWriteRev: list/stat/read/write outcomes over a DirectProjection mount.
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
	for _, name := range []string{"list", "stat", "read", "write", "mkdir", "remove", "run_command"} {
		if tools[name] == nil {
			t.Fatalf("missing tool %q", name)
		}
	}
	rt := turnRuntime(h)

	res, err := tools["list"].invoke(ctx, `{"path":"/work"}`, rt)
	if err != nil || !strings.Contains(res.output, "file\ta.go") {
		t.Fatalf("list: %q err=%v", res.output, err)
	}
	res, err = tools["stat"].invoke(ctx, `{"path":"/work/a.go"}`, rt)
	if err != nil || !strings.Contains(res.output, "is_dir=false") {
		t.Fatalf("stat: %q err=%v", res.output, err)
	}
	if _, err = tools["run_command"].invoke(ctx, `{"command":"ls work"}`, rt); !errors.Is(err, vfs.ErrFuseNotMounted) {
		t.Fatalf("run_command without HostDir: %v", err)
	}

	var page strings.Builder
	for i := 1; i <= vfs.MaxLinesPerWindow+1; i++ {
		fmt.Fprintf(&page, "%d\n", i)
	}
	if err := ms.WriteFile(ctx, "/work/page.txt", []byte(page.String())); err != nil {
		t.Fatal(err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/work/page.txt"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if fieldKV(res.output, "rev") == "" ||
		fieldKV(res.output, "start") != "1" ||
		fieldKV(res.output, "end") != fmt.Sprintf("%d", 1+vfs.MaxLinesPerWindow) ||
		fieldKV(res.output, "returned") != fmt.Sprintf("%d", vfs.MaxLinesPerWindow) ||
		fieldKV(res.output, "eof") != "false" ||
		fieldKV(res.output, "next_start") != "501" ||
		!strings.Contains(res.output, "   500|500") {
		t.Fatalf("path-only first page: %s", res.output)
	}

	_, err = tools["read"].invoke(ctx, `{"path":"/work/a.go","rev":"dead"}`, rt)
	if !errors.Is(err, vfs.ErrStaleContent) {
		t.Fatalf("stale read: %v", err)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/work/a.go","start":1,"end":10}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	rev := fieldKV(res.output, "rev")
	if rev == "" || !strings.Contains(res.output, "1|package a") {
		t.Fatalf("read window: %s", res.output)
	}

	_, err = tools["write"].invoke(ctx, `{"path":"/work/a.go","rev":"dead","start":2,"end":3,"lines":["// x"]}`, rt)
	if !errors.Is(err, vfs.ErrStaleContent) {
		t.Fatalf("stale write: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"path": "/work/a.go", "rev": rev, "start": 2, "end": 3, "lines": []string{"// new"},
	})
	res, err = tools["write"].invoke(ctx, string(body), rt)
	if err != nil {
		t.Fatal(err)
	}
	rev2 := fieldKV(res.output, "rev")
	if rev2 == "" || rev2 == rev {
		t.Fatalf("write lines rev: %s", res.output)
	}
	got, err := ms.ReadText(ctx, "/work/a.go")
	if err != nil || !strings.Contains(got.Text(), "// new") || !strings.Contains(got.Text(), "func A()") {
		t.Fatalf("lines-mode body: %q err=%v", got.Text(), err)
	}

	body, _ = json.Marshal(map[string]any{
		"path": "/work/a.go", "rev": rev2, "old": "func A() {}", "new": "func A() { return }",
	})
	res, err = tools["write"].invoke(ctx, string(body), rt)
	if err != nil || !strings.Contains(res.output, "replacements=1") {
		t.Fatalf("write substring: %q err=%v", res.output, err)
	}
	rev3 := fieldKV(res.output, "rev")
	got, err = ms.ReadText(ctx, "/work/a.go")
	if err != nil || !strings.Contains(got.Text(), "func A() { return }") {
		t.Fatalf("substring body: %q err=%v", got.Text(), err)
	}

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
	res, err = tools["write"].invoke(ctx, string(body), rt)
	if err != nil || !strings.Contains(res.output, "replacements=2") {
		t.Fatalf("replace_all: %q err=%v", res.output, err)
	}
	got, err = ms.ReadText(ctx, "/work/dup.txt")
	if err != nil || got.Text() != "bb bb\n" {
		t.Fatalf("replace_all body: %q err=%v", got.Text(), err)
	}

	beforeA := gotText(t, ms, "/work/a.go")
	_, err = tools["write"].invoke(ctx, `{"path":"/work/a.go","content":"x\n"}`, rt)
	if err == nil || !strings.Contains(err.Error(), "rev required when path exists") {
		t.Fatalf("write without rev: %v", err)
	}
	if gotText(t, ms, "/work/a.go") != beforeA {
		t.Fatalf("body changed without rev: %q", gotText(t, ms, "/work/a.go"))
	}

	body, _ = json.Marshal(map[string]any{"path": "/work/a.go", "rev": rev3, "content": "package a\n"})
	res, err = tools["write"].invoke(ctx, string(body), rt)
	if err != nil || fieldKV(res.output, "rev") == "" {
		t.Fatalf("write full: %q err=%v", res.output, err)
	}
	if gotText(t, ms, "/work/a.go") != "package a\n" {
		t.Fatalf("full write body: %q", gotText(t, ms, "/work/a.go"))
	}

	if _, err = tools["write"].invoke(ctx, `{"path":"/work/b.go","content":"package b\n"}`, rt); err != nil {
		t.Fatal(err)
	}
	if _, err := tools["mkdir"].invoke(ctx, `{"path":"/work/sub"}`, rt); err != nil {
		t.Fatal(err)
	}
	res, err = tools["list"].invoke(ctx, `{"path":"/work"}`, rt)
	if err != nil || !strings.Contains(res.output, "dir\tsub") {
		t.Fatalf("list dir: %q err=%v", res.output, err)
	}
	if _, err := tools["remove"].invoke(ctx, `{"path":"/work/b.go"}`, rt); err != nil {
		t.Fatal(err)
	}

	_, err = tools["list"].invoke(ctx, "{\"path\":\"/work/has\\u0000x\"}", rt)
	if !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("nul path: %v", err)
	}

	_, err = tools["write"].invoke(ctx, `{"path":"/work/missing-span.txt","start":1,"end":2,"lines":["x"]}`, rt)
	if !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("non-full missing: %v", err)
	}
	if _, err = tools["write"].invoke(ctx, `{"path":"/work/missing-span.txt","content":"created\n"}`, rt); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/work/missing-span.txt") != "created\n" {
		t.Fatalf("create after missing-span: %q", gotText(t, ms, "/work/missing-span.txt"))
	}

	requireWriteUnchanged(t, ms, tools["write"], rt, "/work/a.go",
		`{"path":"/work/a.go","rev":"x","old":"","new":"y"}`, "old is required")

	md := "# Hello\n\n## Install\n\nold\n"
	if err := ms.WriteFile(ctx, "/work/README.md", []byte(md)); err != nil {
		t.Fatal(err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/work/README.md","outline":true}`, rt)
	if err != nil || !strings.Contains(res.output, "outline:") ||
		!strings.Contains(res.output, "hello/install") || !strings.Contains(res.output, "kind=heading") {
		t.Fatalf("outline: %q err=%v", res.output, err)
	}
	revMD := fieldKV(res.output, "rev")
	if revMD == "" {
		t.Fatalf("outline rev empty: %s", res.output)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/work/README.md","outline":true,"start":1,"end":4}`, rt)
	if err != nil || fieldKV(res.output, "rev") == "" ||
		!strings.Contains(res.output, "# Hello") ||
		!strings.Contains(res.output, "## Install") {
		t.Fatalf("outline+range: %q err=%v", res.output, err)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/work/README.md","outline":true,"start":1,"end":999}`, rt)
	if err != nil || !strings.Contains(res.output, "eof=true") || !strings.Contains(res.output, "# Hello") {
		t.Fatalf("outline clamp: %q err=%v", res.output, err)
	}
	_, err = tools["read"].invoke(ctx, `{"path":"/work/README.md","outline":true,"start":999,"end":1000}`, rt)
	if !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("outline past EOF: %v", err)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/work/README.md","block_id":"hello/install"}`, rt)
	if err != nil {
		t.Fatalf("read block_id: %v", err)
	}
	if !strings.Contains(res.output, "block_id=hello/install") ||
		!strings.Contains(res.output, "## Install") || !strings.Contains(res.output, "old") ||
		fieldKV(res.output, "rev") == "" {
		t.Fatalf("read block_id: %q", res.output)
	}

	body, _ = json.Marshal(map[string]any{
		"path": "/work/README.md", "rev": revMD, "block_id": "hello/install", "body": "new body\n",
	})
	res, err = tools["write"].invoke(ctx, string(body), rt)
	if err != nil {
		t.Fatalf("replace block: %v out=%s", err, res.output)
	}
	got, err = ms.ReadText(ctx, "/work/README.md")
	if err != nil || !strings.Contains(got.Text(), "new body") || !strings.Contains(got.Text(), "## Install") {
		t.Fatalf("after block replace: %q err=%v", got.Text(), err)
	}
	revMD2 := fieldKV(res.output, "rev")
	requireWriteUnchanged(t, ms, tools["write"], rt, "/work/README.md",
		fmt.Sprintf(`{"path":"/work/README.md","rev":%q,"block_id":"missing/block","body":"x\n"}`, revMD2),
		"unknown block_id")

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
	if _, err = tools["write"].invoke(ctx, string(body), rt); err != nil {
		t.Fatalf("include_heading replace: %v", err)
	}
	got, err = ms.ReadText(ctx, "/work/head.md")
	if err != nil || !strings.Contains(got.Text(), "## Renamed") || strings.Contains(got.Text(), "## Sec") {
		t.Fatalf("include_heading body: %q err=%v", got.Text(), err)
	}

	if err := ms.WriteFile(ctx, "/work/plain.txt", []byte("no structure\n")); err != nil {
		t.Fatal(err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/work/plain.txt","outline":true}`, rt)
	if err != nil || !strings.Contains(res.output, "line_count=") {
		t.Fatalf("outline on plain text: %q err=%v", res.output, err)
	}
	rPlain, err := ms.ContentRev(ctx, "/work/plain.txt")
	if err != nil {
		t.Fatal(err)
	}
	requireWriteUnchanged(t, ms, tools["write"], rt, "/work/plain.txt",
		fmt.Sprintf(`{"path":"/work/plain.txt","rev":%q,"old":"zzz-missing","new":"y"}`, rPlain.Hash),
		"old text not found")
	requireWriteUnchanged(t, ms, tools["write"], rt, "/work/plain.txt",
		fmt.Sprintf(`{"path":"/work/plain.txt","rev":%q,"old":"t","new":"y"}`, rPlain.Hash),
		"occurs")

	if _, err = tools["write"].invoke(ctx, `{"path":"/work/new.txt","content":"a\nb\n"}`, rt); err != nil {
		t.Fatal(err)
	}
	rNew, err := ms.ContentRev(ctx, "/work/new.txt")
	if err != nil {
		t.Fatal(err)
	}
	requireWriteUnchanged(t, ms, tools["write"], rt, "/work/new.txt",
		fmt.Sprintf(`{"path":"/work/new.txt","rev":%q,"start":1,"lines":["x"]}`, rNew.Hash),
		"invalid range")
	_, err = tools["write"].invoke(ctx, fmt.Sprintf(
		`{"path":"/work/new.txt","rev":%q,"start":99,"end":100,"lines":["x"]}`, rNew.Hash), rt)
	if !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("lines past EOF: %v", err)
	}
	if gotText(t, ms, "/work/new.txt") != "a\nb\n" {
		t.Fatalf("body after past-EOF write: %q", gotText(t, ms, "/work/new.txt"))
	}

	if _, err = tools["write"].invoke(ctx, `{"path":"/work/empty.txt","content":""}`, rt); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/work/empty.txt") != "" {
		t.Fatalf("empty create: %q", gotText(t, ms, "/work/empty.txt"))
	}
	if err := ms.WriteFile(ctx, "/work/empty.txt", []byte("keep\n")); err != nil {
		t.Fatal(err)
	}
	rEmpty, err := ms.ContentRev(ctx, "/work/empty.txt")
	if err != nil {
		t.Fatal(err)
	}
	bodyEmpty, _ := json.Marshal(map[string]any{"path": "/work/empty.txt", "rev": rEmpty.Hash, "content": ""})
	if _, err = tools["write"].invoke(ctx, string(bodyEmpty), rt); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/work/empty.txt") != "" {
		t.Fatalf("empty overwrite: %q", gotText(t, ms, "/work/empty.txt"))
	}

	if err := ms.WriteFile(ctx, "/work/cut.txt", []byte("keep UNIQUE-CUT rest\n")); err != nil {
		t.Fatal(err)
	}
	rCut, err := ms.ContentRev(ctx, "/work/cut.txt")
	if err != nil {
		t.Fatal(err)
	}
	bodyNilNew, _ := json.Marshal(map[string]any{
		"path": "/work/cut.txt", "rev": rCut.Hash, "old": " UNIQUE-CUT",
	})
	if _, err = tools["write"].invoke(ctx, string(bodyNilNew), rt); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/work/cut.txt") != "keep rest\n" {
		t.Fatalf("nil new: %q", gotText(t, ms, "/work/cut.txt"))
	}

	if _, err = tools["write"].invoke(ctx, `{"path":"/work/new.txt"}`, rt); err == nil || !strings.Contains(err.Error(), "no mutation") {
		t.Fatalf("no mutation: %v", err)
	}
	_, err = tools["write"].invoke(ctx, `{"path":"/work/new.txt","content":"x","old":"a"}`, rt)
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("mixed mode: %v", err)
	}
	_, err = tools["write"].invoke(ctx, `{"path":"/work/new.txt","content":"a","ir_text":"b"}`, rt)
	if err == nil || !strings.Contains(err.Error(), "disagree") {
		t.Fatalf("content/ir_text: %v", err)
	}

	if _, err = tools["write"].invoke(ctx, `{"path":"/work/ir.txt","ir_text":"hello\n"}`, rt); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/work/ir.txt") != "hello\n" {
		t.Fatalf("ir_text-only: %q", gotText(t, ms, "/work/ir.txt"))
	}
	if _, err = tools["write"].invoke(ctx, `{"path":"/work/same.txt","content":"same\n","ir_text":"same\n"}`, rt); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/work/same.txt") != "same\n" {
		t.Fatalf("content+ir_text equal: %q", gotText(t, ms, "/work/same.txt"))
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/work/plain.txt","outline":true,"ir":true}`, rt)
	if err != nil || !strings.Contains(res.output, "media_type=") ||
		!strings.Contains(res.output, "encoding=") || !strings.Contains(res.output, "line_count=") ||
		!strings.Contains(res.output, "text=") {
		t.Fatalf("read ir: %q err=%v", res.output, err)
	}
}

func gotText(t *testing.T, ms *vfs.MountSession, path string) string {
	t.Helper()
	doc, err := ms.ReadText(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	return doc.Text()
}

func requireWriteUnchanged(t *testing.T, ms *vfs.MountSession, write *Tool, rt HarnessRuntime, path, argsJSON, wantSubstr string) {
	t.Helper()
	before := gotText(t, ms, path)
	_, err := write.invoke(context.Background(), argsJSON, rt)
	if err == nil || !strings.Contains(err.Error(), wantSubstr) {
		t.Fatalf("write %s: want %q, got %v", argsJSON, wantSubstr, err)
	}
	if gotText(t, ms, path) != before {
		t.Fatalf("body changed after %q: %q", wantSubstr, gotText(t, ms, path))
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

// TestVFSTools_runCommandLiveNames: host ls/find on a FUSE tree match session ReadDir.
func TestVFSTools_runCommandLiveNames(t *testing.T) {
	if !vfs.FuseAvailable() {
		t.Skip("no /dev/fuse or /dev/macfuse*")
	}
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms := vfs.NewMountSession("live-names", reg)
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
	if err := ms.FuseMount(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	h := NewAgent(ctx, AgentOptions{
		SessionID: "live-names", Store: stores.NewInMemoryStore(),
		MountSession: ms, Model: &mockStrategy{},
	})
	tool := h.findTool("run_command", "")
	if tool == nil {
		t.Fatal("run_command required")
	}
	ents, err := ms.ReadDir(ctx, "/work")
	if err != nil {
		t.Fatal(err)
	}
	ls, err := tool.invoke(ctx, `{"command":"ls work"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if !strings.Contains(ls.output, e.Name) {
			t.Fatalf("ls missing %q: %s", e.Name, ls.output)
		}
	}
	found, err := tool.invoke(ctx, `{"command":"find work -name '*.go'"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(found.output, "work/a.go") || !strings.Contains(found.output, "work/sub/b.go") {
		t.Fatalf("find *.go: %s", found.output)
	}
}
