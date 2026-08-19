package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/stores"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestVFSTools_readWriteRev: read/write outcomes over a DirectProjection mount.
func TestVFSTools_readWriteRev(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("tools-vfs", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/work/a.go", []byte("package a\n// old\nfunc A() {}\n")); err != nil {
		t.Fatal(err)
	}

	h := mustNewAgent(t, AgentOptions{
		SessionID:    "tools-vfs",
		Store:        stores.NewInMemoryStore(),
		MountSession: ms,
		Model:        &mockStrategy{},
	})
	tools := map[string]*Tool{}
	for _, tool := range h.tools {
		tools[tool.Name] = tool
	}
	for _, name := range []string{"read", "write", "run_command"} {
		if tools[name] == nil {
			t.Fatalf("missing tool %q", name)
		}
	}
	rt := turnRuntime(h)

	if _, err := tools["run_command"].invoke(ctx, `{"command":"ls work"}`, rt); !errors.Is(err, vfs.ErrFuseNotMounted) {
		t.Fatalf("run_command without HostDir: %v", err)
	}

	var page strings.Builder
	for i := 1; i <= vfs.MaxLinesPerWindow+1; i++ {
		fmt.Fprintf(&page, "%d\n", i)
	}
	if err := ms.WriteFile(ctx, "/work/page.txt", []byte(page.String())); err != nil {
		t.Fatal(err)
	}
	res, err := tools["read"].invoke(ctx, `{"path":"/work/page.txt"}`, rt)
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
	if gotText(t, ms, "/work/b.go") != "package b\n" {
		t.Fatalf("create b.go: %q", gotText(t, ms, "/work/b.go"))
	}

	_, err = tools["read"].invoke(ctx, "{\"path\":\"/work/has\\u0000x\"}", rt)
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

	_, err = tools["read"].invoke(ctx, `{"path":"/work/plain.txt","start":5,"end":3}`, rt)
	if err == nil || !strings.Contains(err.Error(), "invalid range") {
		t.Fatalf("inverted range: %v", err)
	}
	_, err = tools["read"].invoke(ctx, `{"path":"work/plain.txt"}`, rt)
	if !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("relative path: %v", err)
	}
	_, err = tools["read"].invoke(ctx, `{"path":"/work/plain.txt","block_id":"nope"}`, rt)
	if err == nil || !strings.Contains(err.Error(), "no structured blocks") {
		t.Fatalf("block on plain: %v", err)
	}
	if _, err := ms.ContentRev(ctx, "/work/pic.bin"); err == nil {
		// pic may not exist in this mount; seed and hash raw bytes
	}
	if err := ms.WriteFile(ctx, "/work/pic.bin", []byte{0x89, 'P', 'N', 'G'}); err != nil {
		t.Fatal(err)
	}
	revBin, err := ms.ContentRev(ctx, "/work/pic.bin")
	if err != nil || revBin.Hash == "" {
		t.Fatalf("binary ContentRev: %+v err=%v", revBin, err)
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

func TestVFSTools_projectedDocOutlineAndBlocks(t *testing.T) {
	ctx := context.Background()
	api := newToolMemDrive()
	docsAPI := newToolMemDocs("doc1", "R0", []vfs.DocsSpan{
		{TabID: "t.a", StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{TabID: "t.a", StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, Text: "Spec"},
		{TabID: "t.a", StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
		{TabID: "t.b", StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{TabID: "t.b", StartIndex: 2, EndIndex: 8, Kind: "paragraph", Text: "Other"},
	}, []vfs.DocTab{{ID: "t.a", Title: "Intro", Index: 0}, {ID: "t.b", Title: "Appendix", Index: 1}})
	auth := vfs.NewSessionAuth()
	_ = auth.Bind("tools-docs", vfs.Binding{
		Provider: "gdrive", Point: "/contracts", Writable: true,
		Auth: vfs.Credential{Token: "t"}, Params: map[string]string{vfs.ParamFolderID: "root"},
	})
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.DriveFactory{ID: "gdrive", Auth: auth, API: api, Docs: docsAPI}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("tools-docs", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.BindingSpec(vfs.Binding{
		Provider: "gdrive", Point: "/contracts", Writable: true,
		Params: map[string]string{vfs.ParamFolderID: "root"},
	})); err != nil {
		t.Fatal(err)
	}
	h := mustNewAgent(t, AgentOptions{
		SessionID: "tools-docs", Store: stores.NewInMemoryStore(), MountSession: ms, Model: &mockStrategy{},
	})
	tools := map[string]*Tool{}
	for _, tool := range h.tools {
		tools[tool.Name] = tool
	}
	rt := turnRuntime(h)

	res, err := tools["read"].invoke(ctx, `{"path":"/contracts/Spec"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "outline:") || strings.Contains(res.output, "<html") ||
		!strings.Contains(res.output, "media_type=application/vnd.google-apps.document") {
		t.Fatalf("default read dumped HTML: %s", res.output)
	}
	rev := fieldKV(res.output, "rev")
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Spec","block_id":"intro/p-1"}`, rt)
	if err != nil || !strings.Contains(res.output, "text=Hello") || strings.Contains(res.output, "<p>") {
		t.Fatalf("block_id IR: %s err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Spec","start":1,"end":6}`, rt)
	if err != nil || !strings.Contains(res.output, "<html>") || !strings.Contains(res.output, "<h1") ||
		!strings.Contains(res.output, "rev=") {
		t.Fatalf("HTML line window: %s err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Spec","ir":true}`, rt)
	if err != nil || strings.Contains(res.output, "text=<html") {
		t.Fatalf("ir dump: %s err=%v", res.output, err)
	}

	_, err = tools["write"].invoke(ctx, fmt.Sprintf(`{"path":"/contracts/Spec","rev":%q,"content":"<p>nope</p>"}`, rev), rt)
	if !errors.Is(err, vfs.ErrProjected) {
		t.Fatalf("content on Doc: %v", err)
	}
	_, err = tools["write"].invoke(ctx, fmt.Sprintf(`{"path":"/contracts/Spec","rev":%q,"start":1,"end":2}`, rev), rt)
	if !errors.Is(err, vfs.ErrProjected) {
		t.Fatalf("line write on Doc: %v", err)
	}
	_, err = tools["write"].invoke(ctx, fmt.Sprintf(`{"path":"/contracts/Spec","rev":%q,"old":"Hello"}`, rev), rt)
	if !errors.Is(err, vfs.ErrProjected) {
		t.Fatalf("substring write on Doc: %v", err)
	}
	_, err = tools["write"].invoke(ctx, fmt.Sprintf(`{"path":"/contracts/Spec","rev":%q,"blocks":[]}`, rev), rt)
	if err == nil || !strings.Contains(err.Error(), "empty IR replace") {
		t.Fatalf("empty blocks: %v", err)
	}
	noteRev, err := ms.ContentRev(ctx, "/contracts/note.txt")
	if err != nil {
		t.Fatal(err)
	}
	_, err = tools["write"].invoke(ctx, fmt.Sprintf(
		`{"path":"/contracts/note.txt","rev":%q,"blocks":[{"kind":"paragraph","text":"x"}]}`, noteRev.Hash), rt)
	if !errors.Is(err, vfs.ErrProjected) {
		t.Fatalf("blocks on plaintext: %v", err)
	}
	_, err = tools["write"].invoke(ctx, fmt.Sprintf(
		`{"path":"/contracts/Spec","rev":%q,"blocks":[{"kind":"paragraph","text":"World"}]}`, rev), rt)
	if err == nil || !strings.Contains(err.Error(), "tab_id required") {
		t.Fatalf("missing tab_id: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"path": "/contracts/Spec", "rev": rev, "block_id": "intro/p-1", "body": "World",
	})
	if _, err = tools["write"].invoke(ctx, string(body), rt); err != nil {
		t.Fatalf("block write: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Spec","block_id":"intro/p-1"}`, rt)
	if err != nil || !strings.Contains(res.output, "text=World") {
		t.Fatalf("block_id after write: %s err=%v", res.output, err)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Spec"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	rev = fieldKV(res.output, "rev")
	blocksBody, _ := json.Marshal(map[string]any{
		"path": "/contracts/Spec", "rev": rev, "tab_id": "t.a",
		"blocks": []map[string]any{{"kind": "paragraph", "text": "Replaced"}},
	})
	if _, err = tools["write"].invoke(ctx, string(blocksBody), rt); err != nil {
		t.Fatalf("tab blocks write: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Spec"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "Replaced") || !strings.Contains(res.output, "Other") {
		t.Fatalf("tab merge outline: %s", res.output)
	}
	rev = fieldKV(res.output, "rev")
	docsAPI.rev["doc1"] = "R-sibling"
	_, err = tools["write"].invoke(ctx, fmt.Sprintf(
		`{"path":"/contracts/Spec","rev":%q,"block_id":"intro/p-1","body":"Stale"}`, rev), rt)
	if !errors.Is(err, vfs.ErrStaleContent) {
		t.Fatalf("stale CAS: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Spec","block_id":"intro/p-1"}`, rt)
	if err != nil || !strings.Contains(res.output, "text=Replaced") {
		t.Fatalf("body after stale write: %s err=%v", res.output, err)
	}

	if _, err = tools["write"].invoke(ctx, `{"path":"/contracts/Spec.md","media_type":"application/vnd.google-apps.document","content":"x"}`, rt); err != nil {
		t.Fatal(err)
	}
	if _, err := tools["write"].invoke(ctx, `{"path":"/contracts/Policy","media_type":"application/vnd.google-apps.document","blocks":[]}`, rt); err != nil {
		t.Fatal(err)
	}
	_, err = tools["write"].invoke(ctx, `{"path":"/contracts/Foo","content":"<html><p>x</p></html>","media_type":"application/vnd.google-apps.document"}`, rt)
	if err == nil || !strings.Contains(err.Error(), "HTML") {
		t.Fatalf("html lift: %v", err)
	}
	if _, err = tools["write"].invoke(ctx, `{"path":"/contracts/Lifted","media_type":"application/vnd.google-apps.document","content":"Hello\n\nWorld"}`, rt); err != nil {
		t.Fatal(err)
	}
	lifted, err := ms.ReadText(ctx, "/contracts/Lifted")
	if err != nil {
		t.Fatal(err)
	}
	var paras []string
	for _, b := range lifted.(vfs.Structured).Blocks() {
		if b.Kind == vfs.BlockKindParagraph {
			paras = append(paras, b.Text)
		}
	}
	if strings.Join(paras, ",") != "Hello,World" {
		t.Fatalf("lifted blocks = %v", paras)
	}
	st, err := ms.Stat(ctx, "/contracts/Lifted")
	if err != nil || st.MediaType != "application/vnd.google-apps.document" {
		t.Fatalf("Lifted Stat = %+v err=%v", st, err)
	}
}

type toolMemDrive struct {
	files map[string]toolFile
}

type toolFile struct {
	meta vfs.DriveMeta
	body []byte
}

func newToolMemDrive() *toolMemDrive {
	return &toolMemDrive{files: map[string]toolFile{
		"root": {meta: vfs.DriveMeta{ID: "root", Name: ".", MimeType: "application/vnd.google-apps.folder", IsDir: true}},
		"doc1": {meta: vfs.DriveMeta{ID: "doc1", Name: "Spec", MimeType: "application/vnd.google-apps.document"}},
		"txt1": {meta: vfs.DriveMeta{ID: "txt1", Name: "note.txt", MimeType: "text/plain", Size: 3}, body: []byte("hi\n")},
	}}
}

func (d *toolMemDrive) GetMeta(_ context.Context, id string) (vfs.DriveMeta, error) {
	f, ok := d.files[id]
	if !ok {
		return vfs.DriveMeta{}, vfs.ErrNotExist
	}
	return f.meta, nil
}
func (d *toolMemDrive) GetMedia(_ context.Context, id string) (io.ReadCloser, int64, error) {
	f, ok := d.files[id]
	if !ok {
		return nil, 0, vfs.ErrNotExist
	}
	return io.NopCloser(strings.NewReader(string(f.body))), int64(len(f.body)), nil
}
func (d *toolMemDrive) List(_ context.Context, folderID string) ([]vfs.DriveMeta, error) {
	var out []vfs.DriveMeta
	for id, f := range d.files {
		if id != "root" && folderID == "root" {
			out = append(out, f.meta)
		}
	}
	return out, nil
}
func (d *toolMemDrive) Export(context.Context, string, string) (io.ReadCloser, int64, error) {
	return nil, 0, vfs.ErrNotSupported
}
func (d *toolMemDrive) PutMedia(context.Context, string, string, io.Reader, int64) (vfs.DriveMeta, error) {
	return vfs.DriveMeta{}, vfs.ErrNotSupported
}
func (d *toolMemDrive) Create(_ context.Context, parentID, name, metadataMIME, mediaMIME string, r io.Reader, size int64) (vfs.DriveMeta, error) {
	id := "new-" + name
	meta := vfs.DriveMeta{ID: id, Name: name, MimeType: metadataMIME, Version: "1"}
	var body []byte
	if r != nil && mediaMIME != "" {
		body, _ = io.ReadAll(io.LimitReader(r, size+1))
	}
	d.files[id] = toolFile{meta: meta, body: body}
	_ = parentID
	return meta, nil
}
func (d *toolMemDrive) Trash(context.Context, string) error { return nil }
func (d *toolMemDrive) Mkdir(context.Context, string, string) (vfs.DriveMeta, error) {
	return vfs.DriveMeta{}, vfs.ErrNotSupported
}

type toolMemDocs struct {
	snaps map[string]vfs.DocsSnapshot
	rev   map[string]string
}

func newToolMemDocs(id, rev string, spans []vfs.DocsSpan, tabs []vfs.DocTab) *toolMemDocs {
	return &toolMemDocs{
		snaps: map[string]vfs.DocsSnapshot{
			id: {DocumentID: id, RevisionID: rev, Tabs: tabs, Body: spans, Lists: map[string]vfs.DocsListProps{}},
		},
		rev: map[string]string{id: rev},
	}
}

func (d *toolMemDocs) Get(_ context.Context, documentID string) (vfs.DocsSnapshot, error) {
	s, ok := d.snaps[documentID]
	if !ok {
		s = vfs.DocsSnapshot{
			DocumentID: documentID, RevisionID: "R0",
			Body: []vfs.DocsSpan{{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"}},
		}
		if d.snaps == nil {
			d.snaps = map[string]vfs.DocsSnapshot{}
		}
		if d.rev == nil {
			d.rev = map[string]string{}
		}
		d.snaps[documentID] = s
		d.rev[documentID] = "R0"
		return s, nil
	}
	s.RevisionID = d.rev[documentID]
	return s, nil
}

func (d *toolMemDocs) BatchUpdate(_ context.Context, documentID string, req vfs.DocsBatch) (vfs.DocsBatchResult, error) {
	if cur := d.rev[documentID]; req.RequiredRevisionID != "" && cur != "" && req.RequiredRevisionID != cur {
		return vfs.DocsBatchResult{}, vfs.ErrConflict
	}
	s := d.snaps[documentID]
	applyToolDocsBatch(&s, req)
	next := d.rev[documentID] + "+1"
	if d.rev[documentID] == "" {
		next = "R1"
	}
	d.rev[documentID] = next
	s.RevisionID = next
	if d.snaps == nil {
		d.snaps = map[string]vfs.DocsSnapshot{}
	}
	d.snaps[documentID] = s
	return vfs.DocsBatchResult{RevisionID: next}, nil
}

func applyToolDocsBatch(s *vfs.DocsSnapshot, req vfs.DocsBatch) {
	tabOf := func(tab string) string {
		if tab != "" {
			return tab
		}
		return req.TabID
	}
	sameTab := func(spTab, reqTab string) bool {
		if reqTab == "" {
			return true
		}
		return spTab == reqTab || spTab == ""
	}
	for _, r := range req.Requests {
		if del := r.DeleteContentRange; del != nil && del.Range != nil {
			start, end := int(del.Range.StartIndex), int(del.Range.EndIndex)
			tab := tabOf(del.Range.TabId)
			var next []vfs.DocsSpan
			for _, sp := range s.Body {
				if !sameTab(sp.TabID, tab) {
					next = append(next, sp)
					continue
				}
				if sp.Kind == "sectionBreak" && sp.StartIndex == 1 {
					next = append(next, sp)
					continue
				}
				if sp.StartIndex < end && sp.EndIndex > start {
					continue
				}
				next = append(next, sp)
			}
			s.Body = next
		}
		if ins := r.InsertText; ins != nil && ins.Location != nil {
			idx := int(ins.Location.Index)
			tab := tabOf(ins.Location.TabId)
			raw := strings.TrimSuffix(ins.Text, "\n")
			level := 1
			if trimmed := strings.TrimLeft(raw, "\t"); trimmed != raw {
				level = len(raw) - len(trimmed) + 1
				raw = trimmed
			}
			s.Body = append(s.Body, vfs.DocsSpan{
				TabID: tab, StartIndex: idx, EndIndex: idx + 1 + len(ins.Text),
				Kind: "paragraph", Text: raw, Level: level,
			})
		}
		if st := r.UpdateParagraphStyle; st != nil && st.ParagraphStyle != nil && st.Range != nil {
			named := st.ParagraphStyle.NamedStyleType
			start := int(st.Range.StartIndex)
			tab := tabOf(st.Range.TabId)
			if strings.HasPrefix(named, "HEADING_") {
				for i := range s.Body {
					if s.Body[i].StartIndex == start && sameTab(s.Body[i].TabID, tab) {
						s.Body[i].Kind = "heading"
						s.Body[i].NamedStyle = named
					}
				}
			}
		}
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

func TestVFSTools_writeDocxBlocksAndInlineMarks(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("tools-docx", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.MountSpec{Point: "/work", Profile: "scratch"}); err != nil {
		t.Fatal(err)
	}
	h := mustNewAgent(t, AgentOptions{
		SessionID: "tools-docx", Store: stores.NewInMemoryStore(), MountSession: ms, Model: &mockStrategy{},
	})
	tools := map[string]*Tool{}
	for _, tool := range h.tools {
		tools[tool.Name] = tool
	}
	rt := turnRuntime(h)
	mt := "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	res, err := tools["write"].invoke(ctx, fmt.Sprintf(
		`{"path":"/work/note.docx","media_type":%q,"blocks":[{"kind":"heading","level":1,"text":"**Title**"},{"kind":"paragraph","text":"See [x](https://e)"}]}`, mt), rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "rev=") {
		t.Fatalf("create: %s", res.output)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/work/note.docx"}`, rt)
	if err != nil || !strings.Contains(res.output, "outline:") || !strings.Contains(res.output, "**Title**") {
		t.Fatalf("read outline: %s err=%v", res.output, err)
	}
	rev := fieldKV(res.output, "rev")
	// content lift on new projected file
	_, err = tools["write"].invoke(ctx, fmt.Sprintf(
		`{"path":"/work/lift.docx","media_type":%q,"content":"Hello **x**"}`, mt), rt)
	if err != nil {
		t.Fatal(err)
	}
	res, err = tools["write"].invoke(ctx, fmt.Sprintf(
		`{"path":"/work/note.docx","rev":%q,"block_id":"p-1","body":"_hi_"}`, rev), rt)
	if err != nil {
		t.Fatal(err)
	}
	_ = res
}

func TestVFSTools_projectedSheetReadWrite(t *testing.T) {
	ctx := context.Background()
	api := &toolMemDrive{files: map[string]toolFile{
		"root":   {meta: vfs.DriveMeta{ID: "root", Name: ".", MimeType: "application/vnd.google-apps.folder", IsDir: true}},
		"sheet1": {meta: vfs.DriveMeta{ID: "sheet1", Name: "Budget", MimeType: "application/vnd.google-apps.spreadsheet", Version: "1"}},
	}}
	sheetsAPI := &toolMemSheets{snaps: map[string]vfs.SheetsSnapshot{
		"sheet1": {
			SpreadsheetID: "sheet1", RevisionID: "1",
			Named: []vfs.NamedRange{{Name: "Total", SheetID: "1", A1: "B2"}},
			Sheets: []vfs.Sheet{
				{ID: "1", Title: "Budget", Rows: 3, Cols: 3, Cells: [][]vfs.Cell{
					{{Input: "Date", Value: "Date"}, {Input: "Amount", Value: "Amount"}, {Input: "Note", Value: "Note"}},
					{{Input: "2026-01-01", Value: "2026-01-01"}, {Input: "42", Value: "42"}, {Input: "ok", Value: "ok"}},
					{{Input: "=A1+1", Value: "43"}},
				}},
				{ID: "2", Title: "Notes", Rows: 1, Cols: 2, Cells: [][]vfs.Cell{
					{{Input: "Hello", Value: "Hello"}, {Input: "World", Value: "World"}},
				}},
			},
		},
	}}
	auth := vfs.NewSessionAuth()
	_ = auth.Bind("tools-sheets", vfs.Binding{
		Provider: "gdrive", Point: "/contracts", Writable: true,
		Auth: vfs.Credential{Token: "t"}, Params: map[string]string{vfs.ParamFolderID: "root"},
	})
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.DriveFactory{ID: "gdrive", Auth: auth, API: api, Sheets: sheetsAPI}); err != nil {
		t.Fatal(err)
	}
	ms, err := vfs.NewMountSession("tools-sheets", reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.Mount(ctx, vfs.BindingSpec(vfs.Binding{
		Provider: "gdrive", Point: "/contracts", Writable: true,
		Params: map[string]string{vfs.ParamFolderID: "root"},
	})); err != nil {
		t.Fatal(err)
	}
	h := mustNewAgent(t, AgentOptions{
		SessionID: "tools-sheets", Store: stores.NewInMemoryStore(), MountSession: ms, Model: &mockStrategy{},
	})
	tools := map[string]*Tool{}
	for _, tool := range h.tools {
		tools[tool.Name] = tool
	}
	rt := turnRuntime(h)

	res, err := tools["read"].invoke(ctx, `{"path":"/contracts/Budget"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "outline:") || !strings.Contains(res.output, "kind=sheet") ||
		!strings.Contains(res.output, "named_ranges:") || !strings.Contains(res.output, "Total") ||
		!strings.Contains(res.output, "budget") || !strings.Contains(res.output, "notes") ||
		!strings.Contains(res.output, "Date\\tAmount\\tNote") {
		t.Fatalf("default read outline: %s", res.output)
	}
	rev := fieldKV(res.output, "rev")

	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","block_id":"budget"}`, rt)
	if err != nil || !strings.Contains(res.output, "sheet=Budget") || !strings.Contains(res.output, "=A1+1") {
		t.Fatalf("row window: %s err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","block_id":"Budget!A3"}`, rt)
	if err != nil || !strings.Contains(res.output, "text==A1+1") {
		t.Fatalf("cell: %s err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","block_id":"Budget!A1:C1"}`, rt)
	if err != nil || !strings.Contains(res.output, "Date\tAmount\tNote") {
		t.Fatalf("range: %s err=%v", res.output, err)
	}
	_, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","start":1,"end":3}`, rt)
	if !errors.Is(err, vfs.ErrProjected) {
		t.Fatalf("line read: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","ir":true}`, rt)
	if err != nil || !strings.Contains(res.output, "sheet_count=2") {
		t.Fatalf("ir: %s err=%v", res.output, err)
	}

	body, _ := json.Marshal(map[string]any{
		"path": "/contracts/Budget", "rev": rev, "block_id": "Budget!B2", "body": "99",
	})
	if _, err = tools["write"].invoke(ctx, string(body), rt); err != nil {
		t.Fatalf("cell write: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","block_id":"Budget!B2"}`, rt)
	if err != nil || !strings.Contains(res.output, "text=99") {
		t.Fatalf("after overlay: %s err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","block_id":"Budget!A3"}`, rt)
	if err != nil || !strings.Contains(res.output, "text==A1+1") {
		t.Fatalf("formula after overlay: %s err=%v", res.output, err)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	rev = fieldKV(res.output, "rev")
	rangeBody, _ := json.Marshal(map[string]any{
		"path": "/contracts/Budget", "rev": rev, "block_id": "Budget!A2:C2",
		"lines": []string{"2026-03-01\t80\tvia-range\\twith\\nnl"},
	})
	if _, err = tools["write"].invoke(ctx, string(rangeBody), rt); err != nil {
		t.Fatalf("range overlay: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","block_id":"Budget!A2:C2"}`, rt)
	if err != nil || !strings.Contains(res.output, "2026-03-01\t80\tvia-range\\twith\\nnl") {
		t.Fatalf("range read-back: %s err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","block_id":"Budget!A3"}`, rt)
	if err != nil || !strings.Contains(res.output, "text==A1+1") {
		t.Fatalf("formula after range: %s err=%v", res.output, err)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	rev = fieldKV(res.output, "rev")
	aa2, _ := json.Marshal(map[string]any{
		"path": "/contracts/Budget", "rev": rev, "block_id": "Budget!AA2", "body": "wide",
	})
	if _, err = tools["write"].invoke(ctx, string(aa2), rt); err != nil {
		t.Fatalf("AA2 write: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","block_id":"Budget!AA2"}`, rt)
	if err != nil || !strings.Contains(res.output, "text=wide") {
		t.Fatalf("AA2 read: %s err=%v", res.output, err)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	rev = fieldKV(res.output, "rev")
	linesBody, _ := json.Marshal(map[string]any{
		"path": "/contracts/Budget", "rev": rev, "block_id": "budget", "start": 2, "end": 3,
		"lines": []string{"2026-02-01\t50\tnew"},
	})
	if _, err = tools["write"].invoke(ctx, string(linesBody), rt); err != nil {
		t.Fatalf("row overlay: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	rev = fieldKV(res.output, "rev")
	_, err = tools["write"].invoke(ctx, fmt.Sprintf(
		`{"path":"/contracts/Budget","rev":%q,"block_id":"budget","start":2,"end":4,"lines":["only-one"]}`, rev), rt)
	if err == nil || !strings.Contains(err.Error(), "line count") {
		t.Fatalf("strict rows: %v", err)
	}
	_, err = tools["write"].invoke(ctx, fmt.Sprintf(`{"path":"/contracts/Budget","rev":%q,"content":"x"}`, rev), rt)
	if !errors.Is(err, vfs.ErrProjected) {
		t.Fatalf("content on Sheet: %v", err)
	}

	replaceBody, _ := json.Marshal(map[string]any{
		"path": "/contracts/Budget", "rev": rev, "tab_id": "1",
		"blocks": []map[string]any{
			{"kind": "sheet", "text": "Date\tAmount\n2026-04-01\t7"},
		},
	})
	if _, err = tools["write"].invoke(ctx, string(replaceBody), rt); err != nil {
		t.Fatalf("tab_id+blocks: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","block_id":"budget"}`, rt)
	if err != nil || !strings.Contains(res.output, "rows=2") || !strings.Contains(res.output, "cols=2") ||
		!strings.Contains(res.output, "2026-04-01\t7") {
		t.Fatalf("shrunk sheet: %s err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","block_id":"Budget!B3"}`, rt)
	if err != nil || !strings.HasSuffix(strings.TrimSpace(res.output), "text=") {
		t.Fatalf("cleared trailer B3: %s err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","block_id":"Budget!C3"}`, rt)
	if err != nil || !strings.HasSuffix(strings.TrimSpace(res.output), "text=") {
		t.Fatalf("cleared trailer C3: %s err=%v", res.output, err)
	}

	if _, err = tools["write"].invoke(ctx, `{"path":"/contracts/Ledger","media_type":"application/vnd.google-apps.spreadsheet","blocks":[{"kind":"sheet","text":"A\tB\n1\t2","attributes":{"title":"Sheet1"}}]}`, rt); err != nil {
		t.Fatal(err)
	}
	st, err := ms.Stat(ctx, "/contracts/Ledger")
	if err != nil || st.MediaType != "application/vnd.google-apps.spreadsheet" {
		t.Fatalf("create-as-Sheet Stat = %+v err=%v", st, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Ledger","block_id":"Sheet1"}`, rt)
	if err != nil || !strings.Contains(res.output, "sheet=Sheet1") ||
		!strings.Contains(res.output, "A\tB") || !strings.Contains(res.output, "1\t2") {
		t.Fatalf("Ledger Sheet1: %s err=%v", res.output, err)
	}
	ledgerRev := fieldKV(res.output, "rev")
	ledgerReplace, _ := json.Marshal(map[string]any{
		"path": "/contracts/Ledger", "rev": ledgerRev,
		"blocks": []map[string]any{{"kind": "sheet", "text": "X\tY"}},
	})
	if _, err = tools["write"].invoke(ctx, string(ledgerReplace), rt); err != nil {
		t.Fatalf("Ledger blocks no tab_id: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Ledger","block_id":"Sheet1"}`, rt)
	if err != nil || !strings.Contains(res.output, "X\tY") {
		t.Fatalf("Ledger replace: %s err=%v", res.output, err)
	}

	if _, err = tools["write"].invoke(ctx, `{"path":"/contracts/FromCsv","media_type":"application/vnd.google-apps.spreadsheet","content":"A,B\n1,2"}`, rt); err != nil {
		t.Fatalf("CSV lift: %v", err)
	}
	fromCsv, err := ms.Stat(ctx, "/contracts/FromCsv")
	if err != nil || fromCsv.MediaType != "application/vnd.google-apps.spreadsheet" {
		t.Fatalf("FromCsv Stat = %+v err=%v", fromCsv, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/FromCsv","block_id":"Sheet1"}`, rt)
	if err != nil || !strings.Contains(res.output, "1\t2") {
		t.Fatalf("FromCsv TSV: %s err=%v", res.output, err)
	}

	if _, err = tools["write"].invoke(ctx, `{"path":"/contracts/Budget.xlsx","media_type":"application/vnd.google-apps.spreadsheet","content":"a,b\n1,2"}`, rt); err != nil {
		t.Fatal(err)
	}
	xlsx, err := ms.Stat(ctx, "/contracts/Budget.xlsx")
	if err != nil || xlsx.MediaType != "application/octet-stream" {
		t.Fatalf("xlsx Stat = %+v err=%v", xlsx, err)
	}
	if _, err = tools["write"].invoke(ctx, `{"path":"/contracts/Bare","content":"plain"}`, rt); err != nil {
		t.Fatal(err)
	}
	bare, err := ms.Stat(ctx, "/contracts/Bare")
	if err != nil || bare.MediaType != "application/octet-stream" {
		t.Fatalf("bare Stat = %+v err=%v", bare, err)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	rev = fieldKV(res.output, "rev")
	f := api.files["sheet1"]
	f.meta.Version = "stale"
	api.files["sheet1"] = f
	_, err = tools["write"].invoke(ctx, fmt.Sprintf(
		`{"path":"/contracts/Budget","rev":%q,"block_id":"Budget!B2","body":"0"}`, rev), rt)
	if !errors.Is(err, vfs.ErrStaleContent) {
		t.Fatalf("stale: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/contracts/Budget","block_id":"Budget!B2"}`, rt)
	if err != nil || !strings.Contains(res.output, "text=7") {
		t.Fatalf("body after stale: %s err=%v", res.output, err)
	}
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
	ms, err := vfs.NewMountSession("live-names", reg)
	if err != nil {
		t.Fatal(err)
	}
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

	h := mustNewAgent(t, AgentOptions{
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

type toolMemSheets struct {
	snaps map[string]vfs.SheetsSnapshot
}

func (m *toolMemSheets) Get(_ context.Context, id string) (vfs.SheetsSnapshot, error) {
	s, ok := m.snaps[id]
	if !ok {
		s = vfs.SheetsSnapshot{
			SpreadsheetID: id,
			Sheets:        []vfs.Sheet{{ID: "0", Title: "Sheet1"}},
		}
		if m.snaps == nil {
			m.snaps = map[string]vfs.SheetsSnapshot{}
		}
		m.snaps[id] = s
	}
	return s, nil
}

func (m *toolMemSheets) BatchUpdateValues(_ context.Context, id string, req vfs.SheetsValuesBatch) error {
	s := m.snaps[id]
	if s.SpreadsheetID == "" {
		s.SpreadsheetID = id
	}
	for _, vr := range req.Data {
		title, a1 := vfs.SplitSheetAddr(vr.Range)
		idx := -1
		for i, sh := range s.Sheets {
			if sh.Title == title || sh.ID == title {
				idx = i
				break
			}
		}
		if idx < 0 && len(s.Sheets) == 1 {
			idx = 0
		}
		if idx < 0 {
			s.Sheets = append(s.Sheets, vfs.Sheet{Title: title})
			idx = len(s.Sheets) - 1
		}
		r1, c1 := 1, 1
		if a1 != "" {
			if i := strings.Index(a1, ":"); i >= 0 {
				a1 = a1[:i]
			}
			if rr, cc, _, _, err := vfs.ParseA1(a1); err == nil {
				r1, c1 = rr, cc
			}
		}
		sh := s.Sheets[idx]
		for r, row := range vr.Values {
			rr := r1 - 1 + r
			for len(sh.Cells) <= rr {
				sh.Cells = append(sh.Cells, nil)
			}
			for c, val := range row {
				cc := c1 - 1 + c
				for len(sh.Cells[rr]) <= cc {
					sh.Cells[rr] = append(sh.Cells[rr], vfs.Cell{})
				}
				cell := sh.Cells[rr][cc]
				cell.Input = val
				if !strings.HasPrefix(val, "=") {
					cell.Value = val
				}
				sh.Cells[rr][cc] = cell
			}
		}
		sh.Rows = len(sh.Cells)
		s.Sheets[idx] = sh
	}
	if m.snaps == nil {
		m.snaps = map[string]vfs.SheetsSnapshot{}
	}
	m.snaps[id] = s
	return nil
}
