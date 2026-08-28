package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs"
)

// TestVFSTools_readWrite: read/write outcomes over a DirectProjection mount.
func TestVFSTools_readWrite(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	ms := mustMountTree(t, "tools-vfs", vfs.At("work", vfs.Local(base)))
	if err := ms.WriteFile(ctx, "/workspace/work/a.go", []byte("package a\n// old\nfunc A() {}\n")); err != nil {
		t.Fatal(err)
	}

	h := mustNewTurnManager(t, AgentOptions{
		SessionID:    "tools-vfs",
		MountSession: ms,
		Model:        &mockStrategy{},
	})
	tools := map[string]*Tool{}
	for _, tool := range h.tools {
		tools[tool.name] = tool
	}
	for _, name := range []string{"read", "write", "write_document", "write_spreadsheet", "run_command"} {
		if tools[name] == nil {
			t.Fatalf("missing tool %q", name)
		}
	}
	rt := turnRuntime(h)

	if _, err := tools["run_command"].invoke(ctx, `{"command":"ls workspace/work"}`, rt); !errors.Is(err, vfs.ErrFuseNotMounted) {
		t.Fatalf("run_command without HostDir: %v", err)
	}

	var page strings.Builder
	for i := 1; i <= vfs.MaxLinesPerWindow+1; i++ {
		fmt.Fprintf(&page, "%d\n", i)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/page.txt", []byte(page.String())); err != nil {
		t.Fatal(err)
	}
	res, err := tools["read"].invoke(ctx, `{"path":"/workspace/work/page.txt"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if fieldKV(res.output, "media_type") == "" ||
		fieldKV(res.output, "start") != "1" ||
		fieldKV(res.output, "end") != fmt.Sprintf("%d", 1+vfs.MaxLinesPerWindow) ||
		fieldKV(res.output, "returned") != fmt.Sprintf("%d", vfs.MaxLinesPerWindow) ||
		fieldKV(res.output, "eof") != "false" ||
		fieldKV(res.output, "next_start") != "501" ||
		!strings.Contains(res.output, "   500|500") {
		t.Fatalf("path-only first page: %s", res.output)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/work/a.go","start":1,"end":10}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "1|package a") {
		t.Fatalf("read window: %s", res.output)
	}

	if err := ms.WriteFile(ctx, "/workspace/work/a.go", []byte("package a\n// changed\nfunc A() {}\n")); err != nil {
		t.Fatal(err)
	}
	_, err = tools["write"].invoke(ctx, `{"path":"/workspace/work/a.go","start":2,"end":3,"lines":["// x"]}`, rt)
	if !errors.Is(err, vfs.ErrStaleContent) || !strings.Contains(err.Error(), "changed since you last read") {
		t.Fatalf("stale write: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/a.go", []byte("package a\n// old\nfunc A() {}\n")); err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(map[string]any{
		"path": "/workspace/work/a.go", "start": 2, "end": 3, "lines": []string{"// new"},
	})
	res, err = tools["write"].invoke(ctx, string(body), rt)
	if err != nil {
		t.Fatal(err)
	}
	if fieldKV(res.output, "path") != "/workspace/work/a.go" || fieldKV(res.output, "line_count") == "" {
		t.Fatalf("write lines: %s", res.output)
	}
	got, err := ms.ReadText(ctx, "/workspace/work/a.go")
	if err != nil || !strings.Contains(got.Text(), "// new") || !strings.Contains(got.Text(), "func A()") {
		t.Fatalf("lines-mode body: %q err=%v", got.Text(), err)
	}

	body, _ = json.Marshal(map[string]any{
		"path": "/workspace/work/a.go", "old": "func A() {}", "new": "func A() { return }",
	})
	res, err = tools["write"].invoke(ctx, string(body), rt)
	if err != nil || !strings.Contains(res.output, "replacements=1") {
		t.Fatalf("write substring: %q err=%v", res.output, err)
	}
	got, err = ms.ReadText(ctx, "/workspace/work/a.go")
	if err != nil || !strings.Contains(got.Text(), "func A() { return }") {
		t.Fatalf("substring body: %q err=%v", got.Text(), err)
	}

	if err := ms.WriteFile(ctx, "/workspace/work/dup.txt", []byte("aa aa\n")); err != nil {
		t.Fatal(err)
	}
	body, _ = json.Marshal(map[string]any{
		"path": "/workspace/work/dup.txt", "old": "aa", "new": "bb", "replace_all": true,
	})
	res, err = tools["write"].invoke(ctx, string(body), rt)
	if err != nil || !strings.Contains(res.output, "replacements=2") {
		t.Fatalf("replace_all: %q err=%v", res.output, err)
	}
	got, err = ms.ReadText(ctx, "/workspace/work/dup.txt")
	if err != nil || got.Text() != "bb bb\n" {
		t.Fatalf("replace_all body: %q err=%v", got.Text(), err)
	}

	if err := ms.WriteFile(ctx, "/workspace/work/cas.txt", []byte("keep-me\nchange-me\n")); err != nil {
		t.Fatal(err)
	}
	if _, err = tools["read"].invoke(ctx, `{"path":"/workspace/work/cas.txt"}`, rt); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/cas.txt", []byte("keep-me\nchanged\n")); err != nil {
		t.Fatal(err)
	}
	body, _ = json.Marshal(map[string]any{
		"path": "/workspace/work/cas.txt", "old": "keep-me", "new": "kept",
	})
	res, err = tools["write"].invoke(ctx, string(body), rt)
	if err != nil || !strings.Contains(res.output, "replacements=1") {
		t.Fatalf("old/new against live file: %q err=%v", res.output, err)
	}
	if gotText(t, ms, "/workspace/work/cas.txt") != "kept\nchanged\n" {
		t.Fatalf("old/new live body: %q", gotText(t, ms, "/workspace/work/cas.txt"))
	}

	res, err = tools["write"].invoke(ctx, `{"path":"/workspace/work/a.go","content":"package a\n"}`, rt)
	if err != nil || fieldKV(res.output, "path") != "/workspace/work/a.go" {
		t.Fatalf("write full: %q err=%v", res.output, err)
	}
	if gotText(t, ms, "/workspace/work/a.go") != "package a\n" {
		t.Fatalf("full write body: %q", gotText(t, ms, "/workspace/work/a.go"))
	}

	if _, err = tools["write"].invoke(ctx, `{"path":"/workspace/work/b.go","content":"package b\n"}`, rt); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/workspace/work/b.go") != "package b\n" {
		t.Fatalf("create b.go: %q", gotText(t, ms, "/workspace/work/b.go"))
	}

	_, err = tools["read"].invoke(ctx, "{\"path\":\"/workspace/work/has\\u0000x\"}", rt)
	if !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("nul path: %v", err)
	}

	_, err = tools["write"].invoke(ctx, `{"path":"/workspace/work/missing-span.txt","start":1,"end":2,"lines":["x"]}`, rt)
	if !errors.Is(err, vfs.ErrNotExist) || !strings.Contains(err.Error(), "that path does not exist. List the parent") {
		t.Fatalf("non-full missing: %v", err)
	}
	if _, err = tools["write"].invoke(ctx, `{"path":"/workspace/work/missing-span.txt","content":"created\n"}`, rt); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/workspace/work/missing-span.txt") != "created\n" {
		t.Fatalf("create after missing-span: %q", gotText(t, ms, "/workspace/work/missing-span.txt"))
	}

	requireWriteUnchanged(t, ms, tools["write"], rt, "/workspace/work/a.go",
		`{"path":"/workspace/work/a.go","old":"","new":"y"}`, "old is required")

	md := "# Hello\n\n## Install\n\nold\n"
	if err := ms.WriteFile(ctx, "/workspace/work/README.md", []byte(md)); err != nil {
		t.Fatal(err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/work/README.md","outline":true}`, rt)
	if err != nil || !strings.Contains(res.output, "outline:") ||
		!strings.Contains(res.output, "hello/install") || !strings.Contains(res.output, "kind=heading") ||
		!strings.Contains(res.output, "media_type=") {
		t.Fatalf("outline: %q err=%v", res.output, err)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/work/README.md","outline":true,"start":1,"end":4}`, rt)
	if err != nil || !strings.Contains(res.output, "# Hello") ||
		!strings.Contains(res.output, "## Install") {
		t.Fatalf("outline+range: %q err=%v", res.output, err)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/work/README.md","outline":true,"start":1,"end":999}`, rt)
	if err != nil || !strings.Contains(res.output, "eof=true") || !strings.Contains(res.output, "# Hello") {
		t.Fatalf("outline clamp: %q err=%v", res.output, err)
	}
	_, err = tools["read"].invoke(ctx, `{"path":"/workspace/work/README.md","outline":true,"start":999,"end":1000}`, rt)
	if !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("outline past EOF: %v", err)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/work/README.md","block_id":"hello/install"}`, rt)
	if err != nil {
		t.Fatalf("read block_id: %v", err)
	}
	if !strings.Contains(res.output, "block_id=hello/install") ||
		!strings.Contains(res.output, "## Install") || !strings.Contains(res.output, "old") {
		t.Fatalf("read block_id: %q", res.output)
	}

	body, _ = json.Marshal(map[string]any{
		"path": "/workspace/work/README.md", "old": "old", "new": "new body",
	})
	res, err = tools["write"].invoke(ctx, string(body), rt)
	if err != nil {
		t.Fatalf("replace markdown body: %v out=%s", err, res.output)
	}
	got, err = ms.ReadText(ctx, "/workspace/work/README.md")
	if err != nil || !strings.Contains(got.Text(), "new body") || !strings.Contains(got.Text(), "## Install") {
		t.Fatalf("after markdown replace: %q err=%v", got.Text(), err)
	}
	requireWriteUnchanged(t, ms, tools["write_document"], rt, "/workspace/work/README.md",
		`{"path":"/workspace/work/README.md","content":"<p>x</p>"}`,
		"Use write")

	md2 := "# Top\n\n## Sec\n\nkeep\n"
	if err := ms.WriteFile(ctx, "/workspace/work/head.md", []byte(md2)); err != nil {
		t.Fatal(err)
	}
	revHead, err := ms.ContentRev(ctx, "/workspace/work/head.md")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ms.Apply(ctx, "/workspace/work/head.md", vfs.Mutation{
		Rev: revHead.Hash, BlockID: "top/sec", IncludeHeading: true,
		Lines: []string{"## Renamed", "body"},
	}); err != nil {
		t.Fatalf("include_heading replace: %v", err)
	}
	got, err = ms.ReadText(ctx, "/workspace/work/head.md")
	if err != nil || !strings.Contains(got.Text(), "## Renamed") || strings.Contains(got.Text(), "## Sec") {
		t.Fatalf("include_heading body: %q err=%v", got.Text(), err)
	}

	if err := ms.WriteFile(ctx, "/workspace/work/plain.txt", []byte("no structure\n")); err != nil {
		t.Fatal(err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/work/plain.txt","outline":true}`, rt)
	if err != nil || !strings.Contains(res.output, "media_type=") ||
		!strings.Contains(res.output, "line_count=") {
		t.Fatalf("outline on plain text: %q err=%v", res.output, err)
	}
	requireWriteUnchanged(t, ms, tools["write"], rt, "/workspace/work/plain.txt",
		`{"path":"/workspace/work/plain.txt","old":"zzz-missing","new":"y"}`,
		"old text was not found")
	requireWriteUnchanged(t, ms, tools["write"], rt, "/workspace/work/plain.txt",
		`{"path":"/workspace/work/plain.txt","old":"t","new":"y"}`,
		"occurs")

	if _, err = tools["write"].invoke(ctx, `{"path":"/workspace/work/new.txt","content":"a\nb\n"}`, rt); err != nil {
		t.Fatal(err)
	}
	requireWriteUnchanged(t, ms, tools["write"], rt, "/workspace/work/new.txt",
		`{"path":"/workspace/work/new.txt","start":1,"lines":["x"]}`,
		"invalid range")
	_, err = tools["write"].invoke(ctx, `{"path":"/workspace/work/new.txt","start":99,"end":100,"lines":["x"]}`, rt)
	if !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("lines past EOF: %v", err)
	}
	if gotText(t, ms, "/workspace/work/new.txt") != "a\nb\n" {
		t.Fatalf("body after past-EOF write: %q", gotText(t, ms, "/workspace/work/new.txt"))
	}

	if _, err = tools["write"].invoke(ctx, `{"path":"/workspace/work/empty.txt","content":""}`, rt); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/workspace/work/empty.txt") != "" {
		t.Fatalf("empty create: %q", gotText(t, ms, "/workspace/work/empty.txt"))
	}
	if err := ms.WriteFile(ctx, "/workspace/work/empty.txt", []byte("keep\n")); err != nil {
		t.Fatal(err)
	}
	bodyEmpty, _ := json.Marshal(map[string]any{"path": "/workspace/work/empty.txt", "content": ""})
	if _, err = tools["write"].invoke(ctx, string(bodyEmpty), rt); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/workspace/work/empty.txt") != "" {
		t.Fatalf("empty overwrite: %q", gotText(t, ms, "/workspace/work/empty.txt"))
	}

	if err := ms.WriteFile(ctx, "/workspace/work/cut.txt", []byte("keep UNIQUE-CUT rest\n")); err != nil {
		t.Fatal(err)
	}
	bodyNilNew, _ := json.Marshal(map[string]any{
		"path": "/workspace/work/cut.txt", "old": " UNIQUE-CUT",
	})
	if _, err = tools["write"].invoke(ctx, string(bodyNilNew), rt); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/workspace/work/cut.txt") != "keep rest\n" {
		t.Fatalf("nil new: %q", gotText(t, ms, "/workspace/work/cut.txt"))
	}

	if _, err = tools["write"].invoke(ctx, `{"path":"/workspace/work/new.txt"}`, rt); err == nil || !strings.Contains(err.Error(), "nothing to change") {
		t.Fatalf("no mutation: %v", err)
	}
	_, err = tools["write"].invoke(ctx, `{"path":"/workspace/work/new.txt","content":"x","old":"a"}`, rt)
	if err == nil || !strings.Contains(err.Error(), "only one change") {
		t.Fatalf("mixed mode: %v", err)
	}

	_, err = tools["read"].invoke(ctx, `{"path":"/workspace/work/plain.txt","start":5,"end":3}`, rt)
	if err == nil || !strings.Contains(err.Error(), "invalid range") {
		t.Fatalf("inverted range: %v", err)
	}
	_, err = tools["read"].invoke(ctx, `{"path":"work/plain.txt"}`, rt)
	if !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("relative path: %v", err)
	}
	_, err = tools["read"].invoke(ctx, `{"path":"/workspace/work/plain.txt","block_id":"nope"}`, rt)
	if err == nil || !strings.Contains(err.Error(), "no structured blocks") {
		t.Fatalf("block on plain: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/pic.bin", []byte{0x89, 'P', 'N', 'G'}); err != nil {
		t.Fatal(err)
	}
	revBin, err := ms.ContentRev(ctx, "/workspace/work/pic.bin")
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
	ms := mustMountTreeReq(t, "tools-docs", vfs.Request{Bindings: []vfs.Binding{{
		Provider: vfs.ProviderGoogleDrive, Writable: true,
		Auth:   vfs.Credential{Token: "t"},
		Params: map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root"},
	}}}, vfs.At("contracts", vfs.DriveWith(api, docsAPI, nil)))
	h := mustNewTurnManager(t, AgentOptions{
		SessionID: "tools-docs", MountSession: ms, Model: &mockStrategy{},
	})
	tools := map[string]*Tool{}
	for _, tool := range h.tools {
		tools[tool.name] = tool
	}
	rt := turnRuntime(h)

	res, err := tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Spec"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "<h1") || !strings.Contains(res.output, "<p>") ||
		!strings.Contains(res.output, "   1|") || !strings.Contains(res.output, "media_type=") {
		t.Fatalf("default read HTML: %s", res.output)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Spec","outline":true}`, rt)
	if err != nil || !strings.Contains(res.output, "outline:") ||
		!strings.Contains(res.output, "intro/p-1") ||
		!strings.Contains(res.output, "media_type=application/vnd.google-apps.document") {
		t.Fatalf("outline: %s err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Spec","block_id":"intro/p-1"}`, rt)
	if err != nil || !strings.Contains(res.output, "text=Hello") {
		t.Fatalf("block_id IR: %s err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Spec","start":1,"end":6}`, rt)
	if err != nil || !strings.Contains(res.output, "<html>") || !strings.Contains(res.output, "<h1") ||
		!strings.Contains(res.output, "media_type=") {
		t.Fatalf("HTML line window: %s err=%v", res.output, err)
	}

	_, err = tools["write"].invoke(ctx, `{"path":"/workspace/contracts/Spec","content":"plain text"}`, rt)
	if err == nil || !errors.Is(err, vfs.ErrProjected) || !strings.Contains(err.Error(), "write_document") {
		t.Fatalf("write on Doc: %v", err)
	}
	_, err = tools["write_document"].invoke(ctx, `{"path":"/workspace/contracts/Spec"}`, rt)
	if err == nil || !strings.Contains(err.Error(), "nothing to change") {
		t.Fatalf("write_document empty: %v", err)
	}
	_, err = tools["write_document"].invoke(ctx, `{"path":"/workspace/contracts/Spec","content":"<p>x</p>","block_id":"intro/p-1"}`, rt)
	if err == nil || !strings.Contains(err.Error(), "only one change") {
		t.Fatalf("write_document mixed: %v", err)
	}
	_, err = tools["write_document"].invoke(ctx, `{"path":"/workspace/contracts/Spec","content":"plain text"}`, rt)
	if err == nil || !errors.Is(err, vfs.ErrUseHTML) {
		t.Fatalf("non-HTML content on Doc: %v", err)
	}
	_, err = tools["write_document"].invoke(ctx, `{"path":"/workspace/contracts/Spec","content":"<p>nope</p>"}`, rt)
	if err == nil || !errors.Is(err, vfs.ErrTabIDRequired) {
		t.Fatalf("HTML without tab_id: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Spec"}`, rt)
	if err != nil || !strings.Contains(res.output, "Other") {
		t.Fatalf("sibling tab after missing tab_id: %s err=%v", res.output, err)
	}
	_, err = tools["write"].invoke(ctx, `{"path":"/workspace/contracts/Spec","old":"Hello"}`, rt)
	if !errors.Is(err, vfs.ErrProjected) || !strings.Contains(err.Error(), "Use write_document") {
		t.Fatalf("substring write on Doc: %v", err)
	}
	_, err = ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{Blocks: []vfs.Block{}})
	if err == nil || !errors.Is(err, vfs.ErrEmptyReplace) || strings.Contains(err.Error(), "IR") {
		t.Fatalf("empty blocks: %v", err)
	}
	noteRev, err := ms.ContentRev(ctx, "/workspace/contracts/note.txt")
	if err != nil {
		t.Fatal(err)
	}
	_, err = ms.Apply(ctx, "/workspace/contracts/note.txt", vfs.Mutation{
		Rev: noteRev.Hash, Blocks: []vfs.Block{{Kind: "paragraph", Text: "x"}},
	})
	if !errors.Is(err, vfs.ErrProjected) {
		t.Fatalf("blocks on plaintext: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"path": "/workspace/contracts/Spec", "block_id": "intro/p-1", "body": "World",
	})
	res, err = tools["write_document"].invoke(ctx, string(body), rt)
	if err != nil || !strings.Contains(res.output, "outline:") || !strings.Contains(res.output, "intro/p-1") {
		t.Fatalf("block write: %q err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Spec","block_id":"intro/p-1"}`, rt)
	if err != nil || !strings.Contains(res.output, "text=World") {
		t.Fatalf("block_id after write: %s err=%v", res.output, err)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Spec"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{
		TabID: "t.a", Blocks: []vfs.Block{{Kind: "paragraph", Text: "Replaced"}},
	}); err != nil {
		t.Fatalf("tab blocks write: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Spec"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "Replaced") || !strings.Contains(res.output, "Other") {
		t.Fatalf("tab merge outline: %s", res.output)
	}
	docsAPI.conflictLeft = 1
	if _, err = tools["write_document"].invoke(ctx, `{"path":"/workspace/contracts/Spec","tab_id":"t.a","content":"<h1>Retry</h1>\n<p>Ok</p>"}`, rt); err != nil {
		t.Fatalf("persist retry: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Spec"}`, rt)
	if err != nil || !strings.Contains(res.output, "Retry") || !strings.Contains(res.output, "Ok") ||
		!strings.Contains(res.output, "Other") {
		t.Fatalf("body after persist retry: %s err=%v", res.output, err)
	}

	mdHTML := "<h1>not a doc</h1>"
	if _, err = tools["write"].invoke(ctx, fmt.Sprintf(`{"path":"/workspace/contracts/notes.md","content":%q}`, mdHTML), rt); err != nil {
		t.Fatal(err)
	}
	st, err := ms.Stat(ctx, "/workspace/contracts/notes.md")
	if err != nil || st.MediaType != "text/markdown" {
		t.Fatalf("notes.md Stat = %+v err=%v", st, err)
	}
	if got := gotText(t, ms, "/workspace/contracts/notes.md"); got != mdHTML {
		t.Fatalf("notes.md body = %q", got)
	}
	liftedBody := "Hello\n\nWorld"
	if _, err = ms.Apply(ctx, "/workspace/contracts/Lifted", vfs.Mutation{
		MediaType: "application/vnd.google-apps.document", Content: &liftedBody,
	}); err != nil {
		t.Fatal(err)
	}
	lifted, err := ms.ReadText(ctx, "/workspace/contracts/Lifted")
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
	st, err = ms.Stat(ctx, "/workspace/contracts/Lifted")
	if err != nil || st.MediaType != "application/vnd.google-apps.document" {
		t.Fatalf("Lifted Stat = %+v err=%v", st, err)
	}
	if _, err = tools["write_document"].invoke(ctx, `{"path":"/workspace/contracts/CRESPIKE","content":"<h1>CRE SPIKE</h1><p>Intro</p>"}`, rt); err != nil {
		t.Fatalf("extensionless HTML create: %v", err)
	}
	st, err = ms.Stat(ctx, "/workspace/contracts/CRESPIKE")
	if err != nil || st.MediaType != "application/vnd.google-apps.document" {
		t.Fatalf("CRESPIKE Stat = %+v err=%v", st, err)
	}
	docsAPI.batchErr = vfs.ErrInvalidWrite
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Spec"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tools["write_document"].invoke(ctx, `{"path":"/workspace/contracts/Spec","tab_id":"t.a","content":"<p>x</p>"}`, rt)
	if err == nil || !errors.Is(err, vfs.ErrInvalidWrite) ||
		!strings.Contains(err.Error(), "was not saved") ||
		!strings.Contains(err.Error(), "write the full HTML again") ||
		!strings.Contains(err.Error(), "/workspace/contracts/Spec") ||
		strings.Contains(err.Error(), "vfs:") || strings.Contains(err.Error(), "IR") {
		t.Fatalf("persist error: %v", err)
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
	snaps        map[string]vfs.DocsSnapshot
	rev          map[string]string
	batchErr     error
	conflictLeft int
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
	if d.conflictLeft > 0 {
		d.conflictLeft--
		return vfs.DocsBatchResult{}, vfs.ErrConflict
	}
	if d.batchErr != nil {
		return vfs.DocsBatchResult{}, d.batchErr
	}
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
	ms := mustMountTree(t, "tools-docx", vfs.At("work", vfs.Local(base)))
	h := mustNewTurnManager(t, AgentOptions{
		SessionID: "tools-docx", MountSession: ms, Model: &mockStrategy{},
	})
	tools := map[string]*Tool{}
	for _, tool := range h.tools {
		tools[tool.name] = tool
	}
	rt := turnRuntime(h)
	mt := "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	applied, err := ms.Apply(ctx, "/workspace/work/note.docx", vfs.Mutation{
		MediaType: mt,
		Blocks: []vfs.Block{
			{Kind: "heading", Text: "**Title**", Style: vfs.StyleMeta{Level: 1}},
			{Kind: "paragraph", Text: "See [x](https://e)"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	res := toolCallResult{output: applied.String()}
	if !strings.Contains(res.output, "rev=") {
		t.Fatalf("create: %s", res.output)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/work/note.docx"}`, rt)
	if err != nil || !strings.Contains(res.output, "<strong>Title</strong>") ||
		!strings.Contains(res.output, "See x") {
		t.Fatalf("read HTML: %s err=%v", res.output, err)
	}
	liftDocx := "Hello **x**"
	_, err = ms.Apply(ctx, "/workspace/work/lift.docx", vfs.Mutation{
		MediaType: mt, Content: &liftDocx,
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/work/lift.docx"}`, rt)
	if err != nil || !strings.Contains(res.output, "Hello") || !strings.Contains(res.output, "<strong>x</strong>") {
		t.Fatalf("lift HTML: %s err=%v", res.output, err)
	}
	res, err = tools["write_document"].invoke(ctx, `{"path":"/workspace/work/note.docx","block_id":"p-1","body":"_hi_"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/work/note.docx"}`, rt)
	if err != nil || !strings.Contains(res.output, "<em>hi</em>") {
		t.Fatalf("italic body: %s err=%v", res.output, err)
	}
}

func TestVFSTools_projectedSheetReadWrite(t *testing.T) {
	ctx := context.Background()
	api := &toolMemDrive{files: map[string]toolFile{
		"root":   {meta: vfs.DriveMeta{ID: "root", Name: ".", MimeType: "application/vnd.google-apps.folder", IsDir: true}},
		"sheet1": {meta: vfs.DriveMeta{ID: "sheet1", Name: "Budget", MimeType: "application/vnd.google-apps.spreadsheet", Version: "1"}},
	}}
	sheetsAPI := vfs.NewMemorySheets()
	sheetsAPI.Seed("sheet1", vfs.SheetsSnapshot{
		SpreadsheetID: "sheet1", RevisionID: "1",
		Named: []vfs.NamedRange{{Name: "Total", SheetID: "1", A1: "B2"}},
		Sheets: []vfs.Sheet{
			{ID: "1", Title: "Budget", Rows: 3, Cols: 3, Cells: [][]vfs.Cell{
				{{Input: "Date", Value: "Date"}, {Input: "Amount", Value: "Amount"}, {Input: "Note", Value: "Note"}},
				{{Input: "2026-01-01", Value: "2026-01-01"}, {Input: "42", Value: "42", Format: vfs.CellFormat{Number: "$#,##0.00", Bold: true}}, {Input: "ok", Value: "ok"}},
				{{Input: "=A1+1", Value: "43"}},
			}},
			{ID: "2", Title: "Notes", Rows: 1, Cols: 2, Cells: [][]vfs.Cell{
				{{Input: "Hello", Value: "Hello"}, {Input: "World", Value: "World"}},
			}},
		},
	})
	ms := mustMountTreeReq(t, "tools-sheets", vfs.Request{Bindings: []vfs.Binding{{
		Provider: vfs.ProviderGoogleDrive, Writable: true,
		Auth:   vfs.Credential{Token: "t"},
		Params: map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root"},
	}}}, vfs.At("contracts", vfs.DriveWith(api, nil, sheetsAPI)))
	h := mustNewTurnManager(t, AgentOptions{
		SessionID: "tools-sheets", MountSession: ms, Model: &mockStrategy{},
	})
	tools := map[string]*Tool{}
	for _, tool := range h.tools {
		tools[tool.name] = tool
	}
	rt := turnRuntime(h)

	res, err := tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Budget"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.output, "outline:") || !strings.Contains(res.output, "kind=sheet") ||
		!strings.Contains(res.output, "named_ranges:") || !strings.Contains(res.output, "Total") {
		t.Fatalf("default read outline: %s", res.output)
	}
	if !strings.Contains(res.output, "sheet_count=2") || !strings.Contains(res.output, "media_type=") {
		t.Fatalf("sheet header: %s", res.output)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Budget","block_id":"Budget!B2"}`, rt)
	if err != nil || !strings.Contains(res.output, "text=42") ||
		!strings.Contains(res.output, "format=number=$#,##0.00,bold") {
		t.Fatalf("cell format: %s err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Budget","block_id":"Budget!A1:C2"}`, rt)
	if err != nil || !strings.Contains(res.output, "Date") || !strings.Contains(res.output, "42") ||
		!strings.Contains(res.output, "B2 format=number=$#,##0.00,bold") {
		t.Fatalf("range: %s err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Budget","block_id":"Budget"}`, rt)
	if err != nil || !strings.Contains(res.output, "sheet=Budget") || !strings.Contains(res.output, "42") {
		t.Fatalf("sheet rows: %s err=%v", res.output, err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Budget","block_id":"Budget","start":2,"end":3}`, rt)
	if err != nil || !strings.Contains(res.output, "   2|") || !strings.Contains(res.output, "42") {
		t.Fatalf("row window: %s err=%v", res.output, err)
	}
	_, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Budget","start":1,"end":3}`, rt)
	if !errors.Is(err, vfs.ErrProjected) {
		t.Fatalf("line read: %v", err)
	}

	_, err = tools["write_spreadsheet"].invoke(ctx, `{"path":"/workspace/contracts/Budget","content":"A\tB\n1\t2"}`, rt)
	if err == nil || !strings.Contains(err.Error(), "cannot replace an existing sheet") {
		t.Fatalf("in-place sheet content: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"path": "/workspace/contracts/Budget", "block_id": "Budget!B2",
		"body": "99", "format": map[string]any{"italic": true},
	})
	if _, err = tools["write_spreadsheet"].invoke(ctx, string(body), rt); err != nil {
		t.Fatalf("overlay: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Budget","block_id":"Budget!B2"}`, rt)
	if err != nil || !strings.Contains(res.output, "text=99") ||
		!strings.Contains(res.output, "italic") || !strings.Contains(res.output, "bold") {
		t.Fatalf("after overlay: %s err=%v", res.output, err)
	}

	if _, err = ms.Apply(ctx, "/workspace/contracts/Ledger", vfs.Mutation{
		MediaType: "application/vnd.google-apps.spreadsheet",
		Blocks: []vfs.Block{{
			Kind: "sheet", Text: "A\tB\n1\t2",
			Style: vfs.StyleMeta{Attributes: map[string]string{"title": "Sheet1"}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	st, err := ms.Stat(ctx, "/workspace/contracts/Ledger")
	if err != nil || st.MediaType != "application/vnd.google-apps.spreadsheet" {
		t.Fatalf("create-as-Sheet Stat = %+v err=%v", st, err)
	}

	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Budget"}`, rt)
	if err != nil {
		t.Fatal(err)
	}
	f := api.files["sheet1"]
	f.meta.ModTime = time.Now().UTC()
	api.files["sheet1"] = f
	snap, err := sheetsAPI.Get(ctx, "sheet1")
	if err != nil {
		t.Fatal(err)
	}
	snap.Sheets[0].Cells[0][0] = vfs.Cell{Input: "Changed", Value: "Changed"}
	sheetsAPI.Seed("sheet1", snap)
	_, err = tools["write"].invoke(ctx, `{"path":"/workspace/contracts/Budget","content":"x"}`, rt)
	if err == nil || !strings.Contains(err.Error(), "write_spreadsheet") {
		t.Fatalf("write on sheet: %v", err)
	}
	_, err = tools["write_document"].invoke(ctx, `{"path":"/workspace/contracts/Budget","content":"<p>x</p>"}`, rt)
	if err == nil || !strings.Contains(err.Error(), "write_spreadsheet") {
		t.Fatalf("write_document on sheet: %v", err)
	}
	_, err = tools["write_spreadsheet"].invoke(ctx,
		`{"path":"/workspace/contracts/Budget","block_id":"Budget!B2","body":"0"}`, rt)
	if !errors.Is(err, vfs.ErrStaleContent) {
		t.Fatalf("stale: %v", err)
	}
	res, err = tools["read"].invoke(ctx, `{"path":"/workspace/contracts/Budget","block_id":"Budget!B2"}`, rt)
	if err != nil || !strings.Contains(res.output, "text=99") {
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
	ms := mustMountTree(t, "live-names", vfs.At("work", vfs.Local(base)))
	if err := ms.MkdirAll(ctx, "/workspace/work/sub"); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/a.go", []byte("package a\n")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/sub/b.go", []byte("package b\n")); err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteFile(ctx, "/workspace/work/readme.md", []byte("# r\n")); err != nil {
		t.Fatal(err)
	}
	if err := ms.FuseMount(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	h := mustNewTurnManager(t, AgentOptions{
		SessionID:    "live-names",
		MountSession: ms, Model: &mockStrategy{},
	})
	tool := h.findTool("run_command", "")
	if tool == nil {
		t.Fatal("run_command required")
	}
	ents, err := ms.ReadDir(ctx, "/workspace/work")
	if err != nil {
		t.Fatal(err)
	}
	ls, err := tool.invoke(ctx, `{"command":"ls workspace/work"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if !strings.Contains(ls.output, e.Name) {
			t.Fatalf("ls missing %q: %s", e.Name, ls.output)
		}
	}
	found, err := tool.invoke(ctx, `{"command":"find workspace/work -name '*.go'"}`, turnRuntime(h))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(found.output, "workspace/work/a.go") || !strings.Contains(found.output, "workspace/work/sub/b.go") {
		t.Fatalf("find *.go: %s", found.output)
	}
}
