package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr"

	"github.com/ryanaldo34/tacklr/vfs/mount"

	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/vfs"
)

// TestVFSTools_readWrite: read/write outcomes over an in-process mount.
func TestVFSTools_readWrite(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	ms := mustMountTree(t, "tools-vfs", vfs.At("work", vfs.Local(base)), vfs.At("ro", vfs.Local(t.TempDir())).ReadOnly())

	if err := ms.Route(ctx, "/workspace/work/a.go").
		WriteFile(ctx, []byte("package a\n// old\nfunc A() {}\n")); err != nil {
		t.Fatal(err)
	}

	tools := toolsOn(ms)

	var page strings.Builder
	for i := 1; i <= vfs.MaxLinesPerWindow+1; i++ {
		fmt.Fprintf(&page, "%d\n", i)
	}

	if err := ms.Route(ctx, "/workspace/work/page.txt").
		WriteFile(ctx, []byte(page.String())); err != nil {
		t.Fatal(err)
	}

	res, err := tools["read"](ctx, `{"path":"/workspace/work/page.txt"}`)
	if err != nil {
		t.Fatal(err)
	}
	if fieldKV(res, "media_type") == "" ||
		fieldKV(res, "start") != "1" ||
		fieldKV(res, "end") != fmt.Sprintf("%d", 1+vfs.MaxLinesPerWindow) ||
		fieldKV(res, "returned") != fmt.Sprintf("%d", vfs.MaxLinesPerWindow) ||
		fieldKV(res, "eof") != "false" ||
		fieldKV(res, "next_start") != "501" ||
		!strings.Contains(res, "   500|500") {
		t.Fatalf("path-only first page: %s", res)
	}

	res, err = tools["read"](ctx, `{"path":"/workspace/work/a.go","start":1,"end":10}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res, "1|package a") {
		t.Fatalf("read window: %s", res)
	}

	if err := ms.Route(ctx, "/workspace/work/a.go").
		WriteFile(ctx, []byte("package a\n// changed\nfunc A() {}\n")); err != nil {
		t.Fatal(err)
	}

	_, err = tools["write"](ctx, `{"path":"/workspace/work/a.go","start":2,"end":3,"lines":["// x"]}`)
	if !errors.Is(err, vfs.ErrStaleContent) || !strings.Contains(err.Error(), "changed since you last read") {
		t.Fatalf("stale write: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/work/a.go").
		WriteFile(ctx, []byte("package a\n// old\nfunc A() {}\n")); err != nil {
		t.Fatal(err)
	}

	body, _ := json.Marshal(map[string]any{
		"path": "/workspace/work/a.go", "start": 2, "end": 3, "lines": []string{"// new"},
	})
	res, err = tools["write"](ctx, string(body))
	if err != nil {
		t.Fatal(err)
	}
	if fieldKV(res, "path") != "/workspace/work/a.go" || fieldKV(res, "line_count") == "" {
		t.Fatalf("write lines: %s", res)
	}

	got, err := ms.Route(ctx, "/workspace/work/a.go").
		ReadText(ctx)
	if err != nil || !strings.Contains(got.Text(), "// new") || !strings.Contains(got.Text(), "func A()") {
		t.Fatalf("lines-mode body: %q err=%v", got.Text(), err)
	}

	body, _ = json.Marshal(map[string]any{
		"path": "/workspace/work/a.go", "old": "func A() {}", "new": "func A() { return }",
	})
	res, err = tools["write"](ctx, string(body))
	if err != nil || !strings.Contains(res, "replacements=1") {
		t.Fatalf("write substring: %q err=%v", res, err)
	}

	got, err = ms.Route(ctx, "/workspace/work/a.go").
		ReadText(ctx)
	if err != nil || !strings.Contains(got.Text(), "func A() { return }") {
		t.Fatalf("substring body: %q err=%v", got.Text(), err)
	}

	if err := ms.Route(ctx, "/workspace/work/dup.txt").
		WriteFile(ctx, []byte("aa aa\n")); err != nil {
		t.Fatal(err)
	}

	body, _ = json.Marshal(map[string]any{
		"path": "/workspace/work/dup.txt", "old": "aa", "new": "bb", "replace_all": true,
	})
	res, err = tools["write"](ctx, string(body))
	if err != nil || !strings.Contains(res, "replacements=2") {
		t.Fatalf("replace_all: %q err=%v", res, err)
	}

	got, err = ms.Route(ctx, "/workspace/work/dup.txt").
		ReadText(ctx)
	if err != nil || got.Text() != "bb bb\n" {
		t.Fatalf("replace_all body: %q err=%v", got.Text(), err)
	}

	if err := ms.Route(ctx, "/workspace/work/cas.txt").
		WriteFile(ctx, []byte("keep-me\nchange-me\n")); err != nil {
		t.Fatal(err)
	}

	if _, err = tools["read"](ctx, `{"path":"/workspace/work/cas.txt"}`); err != nil {
		t.Fatal(err)
	}

	if err := ms.Route(ctx, "/workspace/work/cas.txt").
		WriteFile(ctx, []byte("keep-me\nchanged\n")); err != nil {
		t.Fatal(err)
	}

	body, _ = json.Marshal(map[string]any{
		"path": "/workspace/work/cas.txt", "old": "keep-me", "new": "kept",
	})
	res, err = tools["write"](ctx, string(body))
	if err != nil || !strings.Contains(res, "replacements=1") {
		t.Fatalf("old/new against live file: %q err=%v", res, err)
	}
	if gotText(t, ms, "/workspace/work/cas.txt") != "kept\nchanged\n" {
		t.Fatalf("old/new live body: %q", gotText(t, ms, "/workspace/work/cas.txt"))
	}

	res, err = tools["write"](ctx, `{"path":"/workspace/work/a.go","content":"package a\n"}`)
	if err != nil || fieldKV(res, "path") != "/workspace/work/a.go" {
		t.Fatalf("write full: %q err=%v", res, err)
	}
	if gotText(t, ms, "/workspace/work/a.go") != "package a\n" {
		t.Fatalf("full write body: %q", gotText(t, ms, "/workspace/work/a.go"))
	}

	_, err = tools["read"](ctx, "{\"path\":\"/workspace/work/has\\u0000x\"}")
	if !errors.Is(err, vfs.ErrInvalidPath) {
		t.Fatalf("nul path: %v", err)
	}

	_, err = tools["write"](ctx, `{"path":"/workspace/work/missing-span.txt","start":1,"end":2,"lines":["x"]}`)
	if !errors.Is(err, vfs.ErrNotExist) || !strings.Contains(err.Error(), "that path does not exist. List the parent") {
		t.Fatalf("non-full missing: %v", err)
	}
	if _, err = tools["write"](ctx, `{"path":"/workspace/work/missing-span.txt","content":"created\n"}`); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/workspace/work/missing-span.txt") != "created\n" {
		t.Fatalf("create after missing-span: %q", gotText(t, ms, "/workspace/work/missing-span.txt"))
	}

	requireWriteUnchanged(t, ms, tools["write"], "/workspace/work/a.go",
		`{"path":"/workspace/work/a.go","old":"","new":"y"}`, "old is required")

	md := "# Hello\n\n## Install\n\nold\n"

	if err := ms.Route(ctx, "/workspace/work/README.md").
		WriteFile(ctx, []byte(md)); err != nil {
		t.Fatal(err)
	}

	res, err = tools["read"](ctx, `{"path":"/workspace/work/README.md","outline":true}`)
	if err != nil || !strings.Contains(res, "outline:") ||
		!strings.Contains(res, "hello/install") || !strings.Contains(res, "kind=heading") ||
		!strings.Contains(res, "media_type=") {
		t.Fatalf("outline: %q err=%v", res, err)
	}

	res, err = tools["read"](ctx, `{"path":"/workspace/work/README.md","outline":true,"start":1,"end":4}`)
	if err != nil || !strings.Contains(res, "# Hello") ||
		!strings.Contains(res, "## Install") {
		t.Fatalf("outline+range: %q err=%v", res, err)
	}

	res, err = tools["read"](ctx, `{"path":"/workspace/work/README.md","block_id":"hello/install"}`)
	if err != nil {
		t.Fatalf("read block_id: %v", err)
	}
	if !strings.Contains(res, "block_id=hello/install") ||
		!strings.Contains(res, "## Install") || !strings.Contains(res, "old") {
		t.Fatalf("read block_id: %q", res)
	}

	body, _ = json.Marshal(map[string]any{
		"path": "/workspace/work/README.md", "old": "old", "new": "new body",
	})
	res, err = tools["write"](ctx, string(body))
	if err != nil {
		t.Fatalf("replace markdown body: %v out=%s", err, res)
	}

	got, err = ms.Route(ctx, "/workspace/work/README.md").
		ReadText(ctx)
	if err != nil || !strings.Contains(got.Text(), "new body") || !strings.Contains(got.Text(), "## Install") {
		t.Fatalf("after markdown replace: %q err=%v", got.Text(), err)
	}
	requireWriteUnchanged(t, ms, tools["write_document"], "/workspace/work/README.md",
		`{"path":"/workspace/work/README.md","content":"<p>x</p>"}`,
		"Use write")

	md2 := "# Top\n\n## Sec\n\nkeep\n"

	if err := ms.Route(ctx, "/workspace/work/head.md").
		WriteFile(ctx, []byte(md2)); err != nil {
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

	if got, err := ms.Route(ctx, "/workspace/work/head.md").
		ReadText(ctx); err != nil || !strings.Contains(got.Text(), "## Renamed") || strings.Contains(got.Text(), "## Sec") {
		t.Fatalf("include_heading body: %q err=%v", got.Text(), err)
	}

	if err := ms.Route(ctx, "/workspace/work/plain.txt").
		WriteFile(ctx, []byte("no structure\n")); err != nil {
		t.Fatal(err)
	}

	res, err = tools["read"](ctx, `{"path":"/workspace/work/plain.txt","outline":true}`)
	if err != nil || !strings.Contains(res, "media_type=") ||
		!strings.Contains(res, "line_count=") {
		t.Fatalf("outline on plain text: %q err=%v", res, err)
	}
	requireWriteUnchanged(t, ms, tools["write"], "/workspace/work/plain.txt",
		`{"path":"/workspace/work/plain.txt","old":"zzz-missing","new":"y"}`,
		"old text was not found")
	requireWriteUnchanged(t, ms, tools["write"], "/workspace/work/plain.txt",
		`{"path":"/workspace/work/plain.txt","old":"t","new":"y"}`,
		"occurs")

	if _, err = tools["write"](ctx, `{"path":"/workspace/work/new.txt","content":"a\nb\n"}`); err != nil {
		t.Fatal(err)
	}
	requireWriteUnchanged(t, ms, tools["write"], "/workspace/work/new.txt",
		`{"path":"/workspace/work/new.txt","start":1,"lines":["x"]}`,
		"invalid range")
	_, err = tools["write"](ctx, `{"path":"/workspace/work/new.txt","start":99,"end":100,"lines":["x"]}`)
	if !errors.Is(err, vfs.ErrLineOutOfRange) {
		t.Fatalf("lines past EOF: %v", err)
	}
	if gotText(t, ms, "/workspace/work/new.txt") != "a\nb\n" {
		t.Fatalf("body after past-EOF write: %q", gotText(t, ms, "/workspace/work/new.txt"))
	}

	if err := ms.Route(ctx, "/workspace/work/empty.txt").
		WriteFile(ctx, []byte("keep\n")); err != nil {
		t.Fatal(err)
	}

	bodyEmpty, _ := json.Marshal(map[string]any{"path": "/workspace/work/empty.txt", "content": ""})
	if _, err = tools["write"](ctx, string(bodyEmpty)); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/workspace/work/empty.txt") != "" {
		t.Fatalf("empty overwrite: %q", gotText(t, ms, "/workspace/work/empty.txt"))
	}

	if err := ms.Route(ctx, "/workspace/work/cut.txt").
		WriteFile(ctx, []byte("keep UNIQUE-CUT rest\n")); err != nil {
		t.Fatal(err)
	}

	bodyNilNew, _ := json.Marshal(map[string]any{
		"path": "/workspace/work/cut.txt", "old": " UNIQUE-CUT",
	})
	if _, err = tools["write"](ctx, string(bodyNilNew)); err != nil {
		t.Fatal(err)
	}
	if gotText(t, ms, "/workspace/work/cut.txt") != "keep rest\n" {
		t.Fatalf("nil new: %q", gotText(t, ms, "/workspace/work/cut.txt"))
	}

	if _, err = tools["write"](ctx, `{"path":"/workspace/work/new.txt"}`); err == nil || !strings.Contains(err.Error(), "nothing to change") {
		t.Fatalf("no mutation: %v", err)
	}
	_, err = tools["write"](ctx, `{"path":"/workspace/work/new.txt","content":"x","old":"a"}`)
	if err == nil || !strings.Contains(err.Error(), "only one change") {
		t.Fatalf("mixed mode: %v", err)
	}

	_, err = tools["read"](ctx, `{"path":"/workspace/work/plain.txt","start":5,"end":3}`)
	if err == nil || !strings.Contains(err.Error(), "invalid range") {
		t.Fatalf("inverted range: %v", err)
	}
	_, err = tools["read"](ctx, `{"path":"/workspace/work/plain.txt","block_id":"nope"}`)
	if err == nil || !strings.Contains(err.Error(), "no structured blocks") {
		t.Fatalf("block on plain: %v", err)
	}

	if err := ms.Route(ctx, "/workspace/work/pic.bin").
		WriteFile(ctx, []byte{0x89, 'P', 'N', 'G'}); err != nil {
		t.Fatal(err)
	}

	revBin, err := ms.ContentRev(ctx, "/workspace/work/pic.bin")
	if err != nil || revBin.Hash == "" {
		t.Fatalf("binary ContentRev: %+v err=%v", revBin, err)
	}

	// Each mistake comes back as a correction naming what to fix, and none
	// names a host path.
	if err := ms.Route(ctx, "/workspace/work/notes.md").WriteFile(ctx, []byte("# A\n\nbody\n")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "locked.txt"), []byte("x"), 0o000); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		tool, args string
		want       error
	}{
		"a directory":                        {"read", `{"path":"/workspace/work"}`, vfs.ErrIsDir},
		"a read-only mount":                  {"write", `{"path":"/workspace/ro/x.txt","content":"x"}`, vfs.ErrReadOnly},
		"a line with a newline":              {"write", `{"path":"/workspace/work/a.go","start":1,"end":2,"lines":["a\nb"]}`, vfs.ErrInvalidLine},
		"a heading the file lacks":           {"read", `{"path":"/workspace/work/notes.md","block_id":"nope"}`, vfs.ErrUnknownBlock},
		"a relative path to write":           {"write", `{"path":"work/x.txt","content":"x"}`, vfs.ErrInvalidPath},
		"a relative path to a document":      {"write_document", `{"path":"work/x.docx","content":"<p>x</p>"}`, vfs.ErrInvalidPath},
		"a window past the end":              {"read", `{"path":"/workspace/work/a.go","start":50,"end":60}`, vfs.ErrLineOutOfRange},
		"an outline window past the end":     {"read", `{"path":"/workspace/work/notes.md","outline":true,"start":50,"end":60}`, vfs.ErrLineOutOfRange},
		"a path under a file":                {"read", `{"path":"/workspace/work/a.go/x"}`, vfs.ErrNotDir},
		"a path under a file to write":       {"write", `{"path":"/workspace/work/a.go/x","content":"x"}`, vfs.ErrNotDir},
		"a relative path to a spreadsheet":   {"write_spreadsheet", `{"path":"work/x.xlsx","content":"a"}`, vfs.ErrInvalidPath},
		"a spreadsheet write to a text file": {"write_spreadsheet", `{"path":"/workspace/work/a.go","block_id":"A!A1","body":"x"}`, vfs.ErrProjected},
		"a document write to a text file":    {"write_document", `{"path":"/workspace/work/a.go","content":"<p>x</p>"}`, vfs.ErrProjected},
		"a file the host locked":             {"read", `{"path":"/workspace/work/locked.txt"}`, vfs.ErrPermission},
		"blocks of a binary file":            {"read", `{"path":"/workspace/work/pic.bin","block_id":"x"}`, vfs.ErrNotTextual},
	} {
		_, err := tools[tc.tool](ctx, tc.args)
		if !errors.Is(err, tacklr.ErrCorrection) || !errors.Is(err, tc.want) || strings.Contains(err.Error(), base) {
			t.Errorf("%s = %v, want a correction for %v", name, err, tc.want)
		}
	}
}

func gotText(t *testing.T, ms *vfs.MountSession, path string) string {
	t.Helper()

	doc, err := ms.Route(context.Background(), path).
		ReadText(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return doc.Text()
}

func requireWriteUnchanged(t *testing.T, ms *vfs.MountSession, write tool, path, argsJSON, wantSubstr string) {
	t.Helper()
	before := gotText(t, ms, path)
	_, err := write(context.Background(), argsJSON)
	if err == nil || !strings.Contains(err.Error(), wantSubstr) {
		t.Fatalf("write %s: want %q, got %v", argsJSON, wantSubstr, err)
	}
	if gotText(t, ms, path) != before {
		t.Fatalf("body changed after %q: %q", wantSubstr, gotText(t, ms, path))
	}
}

func TestVFSTools_projectedDocOutlineAndBlocks(t *testing.T) {
	ctx := context.Background()
	api := toolDriveFX(t)
	api.SeedDoc("doc1", "R0", []vfs.DocsSpan{
		{TabID: "t.a", StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{TabID: "t.a", StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, Text: "Spec"},
		{TabID: "t.a", StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
		{TabID: "t.b", StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{TabID: "t.b", StartIndex: 2, EndIndex: 8, Kind: "paragraph", Text: "Other"},
	}, []vfs.DocTab{{ID: "t.a", Title: "Intro", Index: 0}, {ID: "t.b", Title: "Appendix", Index: 1}})
	ms := mustMountRequest(t, "tools-docs", vfs.Request{Bindings: []mount.Binding{{
		Provider: mount.ProviderGoogleDrive, Writable: true,
		Auth:   mount.Credential{Token: "t"},
		Params: map[string]string{mount.ParamName: "contracts", mount.ParamFolderID: "root"},
	}}}, vfs.At("contracts", testdrive.Open(t, api, nil)))
	tools := toolsOn(ms)

	if _, err := tools["read"](ctx, `{"path":"/workspace/contracts/huge.html","outline":true}`); !errors.Is(err, tacklr.ErrCorrection) || !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("read a file too large = %v, want a correction for %v", err, vfs.ErrTooLarge)
	}
	res, err := tools["read"](ctx, `{"path":"/workspace/contracts/Spec"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res, "<h1") || !strings.Contains(res, "<p>") ||
		!strings.Contains(res, "   1|") || !strings.Contains(res, "media_type=") {
		t.Fatalf("default read HTML: %s", res)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/contracts/Spec","outline":true}`)
	if err != nil || !strings.Contains(res, "outline:") ||
		!strings.Contains(res, "intro/p-1") ||
		!strings.Contains(res, "media_type=application/vnd.google-apps.document") {
		t.Fatalf("outline: %s err=%v", res, err)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/contracts/Spec","block_id":"intro/p-1"}`)
	if err != nil || !strings.Contains(res, "text=Hello") {
		t.Fatalf("block_id IR: %s err=%v", res, err)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/contracts/Spec","start":1,"end":6}`)
	if err != nil || !strings.Contains(res, "<html>") || !strings.Contains(res, "<h1") ||
		!strings.Contains(res, "media_type=") {
		t.Fatalf("HTML line window: %s err=%v", res, err)
	}

	_, err = tools["write"](ctx, `{"path":"/workspace/contracts/Spec","content":"plain text"}`)
	if err == nil || !errors.Is(err, vfs.ErrProjected) || !strings.Contains(err.Error(), "write_document") {
		t.Fatalf("write on Doc: %v", err)
	}
	_, err = tools["write_document"](ctx, `{"path":"/workspace/contracts/Spec"}`)
	if err == nil || !strings.Contains(err.Error(), "nothing to change") {
		t.Fatalf("write_document empty: %v", err)
	}
	_, err = tools["write_document"](ctx, `{"path":"/workspace/contracts/Spec","content":"<p>x</p>","block_id":"intro/p-1"}`)
	if err == nil || !strings.Contains(err.Error(), "only one change") {
		t.Fatalf("write_document mixed: %v", err)
	}
	_, err = tools["write_document"](ctx, `{"path":"/workspace/contracts/Spec","content":"plain text"}`)
	if err == nil || !errors.Is(err, vfs.ErrUseHTML) {
		t.Fatalf("non-HTML content on Doc: %v", err)
	}
	_, err = tools["write_document"](ctx, `{"path":"/workspace/contracts/Spec","content":"<p>nope</p>"}`)
	if err == nil || !errors.Is(err, vfs.ErrTabIDRequired) {
		t.Fatalf("HTML without tab_id: %v", err)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/contracts/Spec"}`)
	if err != nil || !strings.Contains(res, "Other") {
		t.Fatalf("sibling tab after missing tab_id: %s err=%v", res, err)
	}
	_, err = tools["write"](ctx, `{"path":"/workspace/contracts/Spec","old":"Hello"}`)
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
	res, err = tools["write_document"](ctx, string(body))
	if err != nil || !strings.Contains(res, "outline:") || !strings.Contains(res, "intro/p-1") {
		t.Fatalf("block write: %q err=%v", res, err)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/contracts/Spec","block_id":"intro/p-1"}`)
	if err != nil || !strings.Contains(res, "text=World") {
		t.Fatalf("block_id after write: %s err=%v", res, err)
	}

	_, err = tools["read"](ctx, `{"path":"/workspace/contracts/Spec"}`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ms.Apply(ctx, "/workspace/contracts/Spec", vfs.Mutation{
		TabID: "t.a", Blocks: []vfs.Block{{Kind: "paragraph", Text: "Replaced"}},
	}); err != nil {
		t.Fatalf("tab blocks write: %v", err)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/contracts/Spec"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res, "Replaced") || !strings.Contains(res, "Other") {
		t.Fatalf("tab merge outline: %s", res)
	}
	api.FailNextDocWrites(1)
	if _, err = tools["write_document"](ctx, `{"path":"/workspace/contracts/Spec","tab_id":"t.a","content":"<h1>Retry</h1>\n<p>Ok</p>"}`); err != nil {
		t.Fatalf("persist retry: %v", err)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/contracts/Spec"}`)
	if err != nil || !strings.Contains(res, "Retry") || !strings.Contains(res, "Ok") ||
		!strings.Contains(res, "Other") {
		t.Fatalf("body after persist retry: %s err=%v", res, err)
	}

	mdHTML := "<h1>not a doc</h1>"
	if _, err = tools["write"](ctx, fmt.Sprintf(`{"path":"/workspace/contracts/notes.md","content":%q}`, mdHTML)); err != nil {
		t.Fatal(err)
	}

	st, err := ms.Route(ctx, "/workspace/contracts/notes.md").
		Stat(ctx)
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

	lifted, err := ms.Route(ctx, "/workspace/contracts/Lifted").
		ReadText(ctx)
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

	st, err = ms.Route(ctx, "/workspace/contracts/Lifted").
		Stat(ctx)
	if err != nil || st.MediaType != "application/vnd.google-apps.document" {
		t.Fatalf("Lifted Stat = %+v err=%v", st, err)
	}
	if _, err = tools["write_document"](ctx, `{"path":"/workspace/contracts/CRESPIKE","content":"<h1>CRE SPIKE</h1><p>Intro</p>"}`); err != nil {
		t.Fatalf("extensionless HTML create: %v", err)
	}

	if st, err := ms.Route(ctx, "/workspace/contracts/CRESPIKE").
		Stat(ctx); err != nil || st.MediaType != "application/vnd.google-apps.document" {
		t.Fatalf("CRESPIKE Stat = %+v err=%v", st, err)
	}
	api.FailDocBatch()
	_, err = tools["read"](ctx, `{"path":"/workspace/contracts/Spec"}`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tools["write_document"](ctx, `{"path":"/workspace/contracts/Spec","tab_id":"t.a","content":"<p>x</p>"}`)
	if err == nil || !errors.Is(err, vfs.ErrInvalidWrite) ||
		!strings.Contains(err.Error(), "was not saved") ||
		!strings.Contains(err.Error(), "write the full HTML again") ||
		!strings.Contains(err.Error(), "/workspace/contracts/Spec") ||
		strings.Contains(err.Error(), "vfs:") || strings.Contains(err.Error(), "IR") {
		t.Fatalf("persist error: %v", err)
	}
}

func toolDriveFX(t *testing.T) *testdrive.FX {
	t.Helper()
	fx := testdrive.New()
	fx.Add("", vfs.DriveMeta{ID: "root", Name: ".", MimeType: "application/vnd.google-apps.folder"}, nil)
	fx.Add("root", vfs.DriveMeta{ID: "doc1", Name: "Spec", MimeType: "application/vnd.google-apps.document"}, nil)
	fx.Add("root", vfs.DriveMeta{ID: "txt1", Name: "note.txt", MimeType: "text/plain", Size: 3}, []byte("hi\n"))
	fx.Add("root", vfs.DriveMeta{ID: "huge", Name: "huge.html", MimeType: "text/html", Size: int64(vfs.MaxReadFileBytes) + 1}, nil)
	return fx
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
	tools := toolsOn(ms)
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
	res := applied.String()
	if !strings.Contains(res, "rev=") {
		t.Fatalf("create: %s", res)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/work/note.docx"}`)
	if err != nil || !strings.Contains(res, "<strong>Title</strong>") ||
		!strings.Contains(res, "See x") {
		t.Fatalf("read HTML: %s err=%v", res, err)
	}
	liftDocx := "Hello **x**"
	_, err = ms.Apply(ctx, "/workspace/work/lift.docx", vfs.Mutation{
		MediaType: mt, Content: &liftDocx,
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/work/lift.docx"}`)
	if err != nil || !strings.Contains(res, "Hello") || !strings.Contains(res, "<strong>x</strong>") {
		t.Fatalf("lift HTML: %s err=%v", res, err)
	}
	_, err = tools["write_document"](ctx, `{"path":"/workspace/work/note.docx","block_id":"p-1","body":"_hi_"}`)
	if err != nil {
		t.Fatal(err)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/work/note.docx"}`)
	if err != nil || !strings.Contains(res, "<em>hi</em>") {
		t.Fatalf("italic body: %s err=%v", res, err)
	}
	_, err = tools["write_document"](ctx, `{"path":"/workspace/work/note.docx","content":"<div></div>"}`)
	if !errors.Is(err, tacklr.ErrCorrection) || !errors.Is(err, vfs.ErrEmptyReplace) {
		t.Fatalf("empty body = %v, want a correction for %v", err, vfs.ErrEmptyReplace)
	}
	requireWriteUnchanged(t, ms, tools["write_document"], "/workspace/work/note.docx",
		`{"path":"/workspace/work/note.docx","content":"<p>x</p>","start":1,"end":2,"lines":["x"]}`, "pass only one change")
}

func TestVFSTools_projectedSheetReadWrite(t *testing.T) {
	ctx := context.Background()
	api := testdrive.New()
	api.Add("", vfs.DriveMeta{ID: "root", Name: ".", MimeType: "application/vnd.google-apps.folder"}, nil)
	api.Add("root", vfs.DriveMeta{ID: "sheet1", Name: "Budget", MimeType: "application/vnd.google-apps.spreadsheet", Version: "1"}, nil)
	api.SeedSheet("sheet1", vfs.SheetsSnapshot{
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
	ms := mustMountRequest(t, "tools-sheets", vfs.Request{Bindings: []mount.Binding{{
		Provider: mount.ProviderGoogleDrive, Writable: true,
		Auth:   mount.Credential{Token: "t"},
		Params: map[string]string{mount.ParamName: "contracts", mount.ParamFolderID: "root"},
	}}}, vfs.At("contracts", testdrive.Open(t, api, nil)))
	tools := toolsOn(ms)

	res, err := tools["read"](ctx, `{"path":"/workspace/contracts/Budget"}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res, "outline:") || !strings.Contains(res, "kind=sheet") ||
		!strings.Contains(res, "named_ranges:") || !strings.Contains(res, "Total") {
		t.Fatalf("default read outline: %s", res)
	}
	if !strings.Contains(res, "sheet_count=2") || !strings.Contains(res, "media_type=") {
		t.Fatalf("sheet header: %s", res)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/contracts/Budget","block_id":"Budget!B2"}`)
	if err != nil || !strings.Contains(res, "text=42") ||
		!strings.Contains(res, "format=number=$#,##0.00,bold") {
		t.Fatalf("cell format: %s err=%v", res, err)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/contracts/Budget","block_id":"Budget!A1:C2"}`)
	if err != nil || !strings.Contains(res, "Date") || !strings.Contains(res, "42") ||
		!strings.Contains(res, "B2 format=number=$#,##0.00,bold") {
		t.Fatalf("range: %s err=%v", res, err)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/contracts/Budget","block_id":"Budget"}`)
	if err != nil || !strings.Contains(res, "sheet=Budget") || !strings.Contains(res, "42") {
		t.Fatalf("sheet rows: %s err=%v", res, err)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/contracts/Budget","block_id":"Budget","start":2,"end":3}`)
	if err != nil || !strings.Contains(res, "   2|") || !strings.Contains(res, "42") {
		t.Fatalf("row window: %s err=%v", res, err)
	}
	_, err = tools["read"](ctx, `{"path":"/workspace/contracts/Budget","start":1,"end":3}`)
	if !errors.Is(err, vfs.ErrProjected) {
		t.Fatalf("line read: %v", err)
	}

	_, err = tools["write_spreadsheet"](ctx, `{"path":"/workspace/contracts/Budget","content":"A\tB\n1\t2"}`)
	if err == nil || !strings.Contains(err.Error(), "cannot replace an existing sheet") {
		t.Fatalf("in-place sheet content: %v", err)
	}

	body, _ := json.Marshal(map[string]any{
		"path": "/workspace/contracts/Budget", "block_id": "Budget!B2",
		"body": "99", "format": map[string]any{"italic": true},
	})
	if _, err = tools["write_spreadsheet"](ctx, string(body)); err != nil {
		t.Fatalf("overlay: %v", err)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/contracts/Budget","block_id":"Budget!B2"}`)
	if err != nil || !strings.Contains(res, "text=99") ||
		!strings.Contains(res, "italic") || !strings.Contains(res, "bold") {
		t.Fatalf("after overlay: %s err=%v", res, err)
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

	if st, err := ms.Route(ctx, "/workspace/contracts/Ledger").
		Stat(ctx); err != nil || st.MediaType != "application/vnd.google-apps.spreadsheet" {
		t.Fatalf("create-as-Sheet Stat = %+v err=%v", st, err)
	}

	_, err = tools["read"](ctx, `{"path":"/workspace/contracts/Budget"}`)
	if err != nil {
		t.Fatal(err)
	}
	api.Nodes["sheet1"].Meta.ModTime = api.Nodes["sheet1"].Meta.ModTime.Add(time.Second)
	api.Nodes["sheet1"].Meta.Version = "2"
	snap := api.SheetSnap("sheet1")
	snap.Sheets[0].Cells[0][0] = vfs.Cell{Input: "Changed", Value: "Changed"}
	api.SeedSheet("sheet1", snap)
	_, err = tools["write"](ctx, `{"path":"/workspace/contracts/Budget","content":"x"}`)
	if err == nil || !strings.Contains(err.Error(), "write_spreadsheet") {
		t.Fatalf("write on sheet: %v", err)
	}
	_, err = tools["write_document"](ctx, `{"path":"/workspace/contracts/Budget","content":"<p>x</p>"}`)
	if err == nil || !strings.Contains(err.Error(), "write_spreadsheet") {
		t.Fatalf("write_document on sheet: %v", err)
	}
	_, err = tools["write_spreadsheet"](ctx,
		`{"path":"/workspace/contracts/Budget","block_id":"Budget!B2","body":"0"}`)
	if !errors.Is(err, vfs.ErrStaleContent) {
		t.Fatalf("stale: %v", err)
	}
	res, err = tools["read"](ctx, `{"path":"/workspace/contracts/Budget","block_id":"Budget!B2"}`)
	if err != nil || !strings.Contains(res, "text=99") {
		t.Fatalf("body after stale: %s err=%v", res, err)
	}

	holder := vfs.NewTokenHolder(mount.Credential{Token: "t"})
	expired := mustMountTree(t, "tools-sheets-expired", vfs.At("contracts", testdrive.Open(t, api, holder)))
	holder.Set(mount.Credential{})
	_, err = toolsOn(expired)["read"](ctx, `{"path":"/workspace/contracts/Budget"}`)
	if !errors.Is(err, vfs.ErrAuthExpired) || !errors.Is(err, tacklr.ErrAuthExpired) {
		t.Fatalf("expired token read = %v, want the harness's %v so the call parks", err, tacklr.ErrAuthExpired)
	}

	// Each sheet mistake comes back as a correction naming what to fix.
	for name, tc := range map[string]struct {
		tool, args string
		want       error
	}{
		"an address that is not A1":        {"read", `{"path":"/workspace/contracts/Budget","block_id":"Budget!1A"}`, vfs.ErrInvalidA1},
		"a cell of a sheet it lacks":       {"read", `{"path":"/workspace/contracts/Budget","block_id":"Nope!A1"}`, vfs.ErrUnknownBlock},
		"a range of a sheet it lacks":      {"read", `{"path":"/workspace/contracts/Budget","block_id":"Nope!A1:B2"}`, vfs.ErrUnknownBlock},
		"the rows of a sheet it lacks":     {"read", `{"path":"/workspace/contracts/Budget","block_id":"Nope"}`, vfs.ErrUnknownBlock},
		"an inverted window":               {"read", `{"path":"/workspace/contracts/Budget","block_id":"Budget","start":3,"end":1}`, vfs.ErrLineOutOfRange},
		"content and a cell at once":       {"write_spreadsheet", `{"path":"/workspace/contracts/Budget","content":"a","block_id":"Budget!A1"}`, tacklr.ErrInvalid},
		"a new sheet without content":      {"write_spreadsheet", `{"path":"/workspace/contracts/New"}`, tacklr.ErrInvalid},
		"an existing sheet with no change": {"write_spreadsheet", `{"path":"/workspace/contracts/Budget"}`, tacklr.ErrInvalid},
	} {
		_, err := tools[tc.tool](ctx, tc.args)
		if !errors.Is(err, tacklr.ErrCorrection) || !errors.Is(err, tc.want) {
			t.Errorf("%s = %v, want a correction for %v", name, err, tc.want)
		}
	}
}

// tool is a workspace tool as the model calls it: JSON arguments in, its
// result or error out.
type tool func(ctx context.Context, args string) (string, error)

// toolsOn are the workspace tools on ms by name.
func toolsOn(ms *vfs.MountSession) map[string]tool {
	v := vfsTools{ms: ms}
	return map[string]tool{
		"read":              handle(v.read),
		"write":             handle(v.write),
		"write_document":    handle(v.writeDocument),
		"write_spreadsheet": handle(v.writeSpreadsheet),
	}
}

// handle runs h on JSON arguments, decoded as the harness decodes them.
func handle[A any](h func(context.Context, A, *tacklr.CallContext) (string, error)) tool {
	return func(ctx context.Context, args string) (string, error) {
		var a A
		if err := json.Unmarshal([]byte(args), &a); err != nil {
			return "", err
		}
		return h(ctx, a, &tacklr.CallContext{})
	}
}
