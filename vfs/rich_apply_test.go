package vfs_test

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/adapters"
)

const richGridType = "application/x-rich-grid"

// richApplySession mounts a local work tree (with note.md, blob.bin,
// Spec.docx and Budget.xlsx), a read-only local tree and the host tree,
// with the Word and Excel codecs and a grid codec for richGridType.
func richApplySession(t *testing.T, host richHostFS) *vfs.MountSession {
	t.Helper()
	ctx := t.Context()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "ro"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"note.md":   "# Intro\nbody\n# Next\nmore\n",
		"blob.bin":  "a\x00b\n",
		"ro/f.txt":  "seed\n",
		"twice.txt": "x-x",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	ms := richHostSession(t, host, []vfs.Member{
		vfs.At("work", vfs.Local(dir)),
		vfs.At("ro", vfs.Local(filepath.Join(dir, "ro"))).ReadOnly(),
	}, append(adapters.Codecs(), vfs.TabularCodec{Types: []string{richGridType}, Normalizer: vfs.SheetsHTML{}})...)
	for p, body := range map[string]string{
		"/workspace/work/Spec.docx":   "<h1>Title</h1><p>text</p>",
		"/workspace/work/Budget.xlsx": "Amount,Note\n42,ok",
	} {
		if _, err := ms.Apply(ctx, p, vfs.Mutation{Content: &body}); err != nil {
			t.Fatal(err)
		}
		// The stored checkout is the decoded file, not the created document.
		if _, err := ms.Route(ctx, p).ReadText(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return ms
}

func richStr(s string) *string { return &s }

// TestMountSession_applyRefusesBadWrites: Apply refuses each bad write with
// its error, and the files stay as they are. The writes are: a bad path, no
// change or two changes, an unmounted path, a body that is too large, a
// create that the codec refuses or that gives no text, a stale rev, a
// substring of a projected or frozen host text, a text write that fails
// its compare-and-swap (only a rich document retries), a read-only mount, an
// unknown block, a heading without its body, a block with no line span, a
// line that holds a newline, an unknown sheet, a sheet tab_id that does not
// match, a bad cell, a cell that makes the grid too large, blocks for a type
// with no block codec, blocks of a host document with no text, and a rev on
// a file with no codec or with no text.
func TestMountSession_applyRefusesBadWrites(t *testing.T) {
	t.Parallel()
	ms := richApplySession(t, richHostFS{
		"frozen": richHostText(richFrozenType, "frozen body\n"),
		"plain":  richHostText(richPlainType, "plain body\n"),
		"opaque": richHostText(richOpaqueType, "x"),
		"busy":   {mediaType: "text/plain", size: 4, open: func() io.Reader { return strings.NewReader("busy") }, conflict: true},
	})
	para := []vfs.Block{{Kind: vfs.BlockKindParagraph, Text: "x"}}
	huge := strings.Repeat("x", vfs.MaxReadFileBytes+1)
	cases := []struct {
		name       string
		path       string
		mut        vfs.Mutation
		wantErr    error
		wantErrMsg string
	}{
		{name: "a relative path", path: "rel", mut: vfs.Mutation{Content: richStr("x")}, wantErr: vfs.ErrInvalidPath},
		{name: "no change", path: "/workspace/work/note.md", wantErrMsg: "nothing to change"},
		{name: "two changes", path: "/workspace/work/note.md", mut: vfs.Mutation{Content: richStr("x"), Old: richStr("body")}, wantErrMsg: "only one change"},
		{name: "an unmounted path", path: "/nomount/x", mut: vfs.Mutation{Content: richStr("x")}, wantErr: vfs.ErrNotMounted},
		{name: "a body that is too large", path: "/workspace/work/note.md", mut: vfs.Mutation{Content: &huge}, wantErr: vfs.ErrTooLarge},
		{name: "a create that the codec refuses", path: "/workspace/work/grid", mut: vfs.Mutation{MediaType: richGridType, Content: richStr("<p>x</p>")}, wantErrMsg: "HTML content is not accepted"},
		{name: "a create that gives no text", path: "/workspace/work/pic", mut: vfs.Mutation{MediaType: richOpaqueType, Content: richStr("x")}, wantErr: vfs.ErrNotTextual},
		{name: "blocks that give no text", path: "/workspace/work/pic", mut: vfs.Mutation{MediaType: richOpaqueType, Blocks: para}, wantErr: vfs.ErrNotTextual},
		{name: "blocks for a type with no block codec", path: "/workspace/work/new.md", mut: vfs.Mutation{Blocks: para}, wantErrMsg: "blocks require a structured codec"},
		{name: "HTML for a projected host text", path: "/workspace/host/plain", mut: vfs.Mutation{Content: richStr("<p>x</p>")}, wantErr: vfs.ErrProjected},
		{name: "blocks for a projected host text", path: "/workspace/host/plain", mut: vfs.Mutation{Blocks: para}, wantErr: vfs.ErrProjected},
		{name: "a substring of a projected host text", path: "/workspace/host/plain", mut: vfs.Mutation{Old: richStr("plain"), New: richStr("x")}, wantErr: vfs.ErrProjected},
		{name: "a substring of a frozen host text", path: "/workspace/host/frozen", mut: vfs.Mutation{Old: richStr("frozen"), New: richStr("x")}, wantErr: errRichFrozen},
		{name: "a substring with a stale rev", path: "/workspace/work/note.md", mut: vfs.Mutation{Rev: "stale", Old: richStr("body"), New: richStr("x")}, wantErr: vfs.ErrStaleContent},
		{name: "a text write that fails its compare-and-swap", path: "/workspace/host/busy", mut: vfs.Mutation{Old: richStr("busy"), New: richStr("x")}, wantErr: vfs.ErrStaleContent},
		{name: "a substring on a read-only mount", path: "/workspace/ro/f.txt", mut: vfs.Mutation{Old: richStr("seed"), New: richStr("x")}, wantErr: vfs.ErrReadOnly},
		{name: "blocks with a stale rev", path: "/workspace/work/Spec.docx", mut: vfs.Mutation{Rev: "stale", Blocks: para}, wantErr: vfs.ErrStaleContent},
		{name: "an unknown block", path: "/workspace/work/note.md", mut: vfs.Mutation{BlockID: "nope", Body: richStr("x")}, wantErr: vfs.ErrUnknownBlock},
		{name: "a line that holds a newline", path: "/workspace/work/note.md", mut: vfs.Mutation{BlockID: "intro", Lines: []string{"a\nb"}}, wantErr: vfs.ErrInvalidLine},
		{name: "a heading without its body", path: "/workspace/work/Spec.docx", mut: vfs.Mutation{BlockID: "title", Body: richStr("x")}, wantErrMsg: "has no body"},
		{name: "a host block with no line span", path: "/workspace/host/frozen", mut: vfs.Mutation{BlockID: "loose", Body: richStr("x")}, wantErr: vfs.ErrLineOutOfRange},
		{name: "a rev on a file with no codec", path: "/workspace/work/blob.bin", mut: vfs.Mutation{Rev: "r", BlockID: "x", Body: richStr("x")}, wantErr: vfs.ErrNoCodec},
		{name: "a rev on a host document with no text", path: "/workspace/host/opaque", mut: vfs.Mutation{Rev: "r", BlockID: "x", Body: richStr("x")}, wantErr: vfs.ErrNotTextual},
		{name: "an unknown sheet", path: "/workspace/work/Budget.xlsx", mut: vfs.Mutation{BlockID: "Nope!A1", Body: richStr("x")}, wantErr: vfs.ErrUnknownBlock},
		{name: "a sheet tab_id that does not match", path: "/workspace/work/Budget.xlsx", mut: vfs.Mutation{BlockID: "Budget!B2", TabID: "9", Body: richStr("x")}, wantErrMsg: `tab_id "9" does not match`},
		{name: "a bad cell", path: "/workspace/work/Budget.xlsx", mut: vfs.Mutation{BlockID: "Budget!A0", Body: richStr("x")}, wantErr: vfs.ErrInvalidA1},
		{name: "a cell that makes the grid too large", path: "/workspace/work/Budget.xlsx", mut: vfs.Mutation{BlockID: "Budget!A100001", Body: richStr("x")}, wantErr: vfs.ErrTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := ms.Apply(t.Context(), tc.path, tc.mut)

			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("Apply err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantErrMsg != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErrMsg)) {
				t.Fatalf("Apply err = %v, want %q", err, tc.wantErrMsg)
			}
		})
	}
}

// TestMountSession_applyWritesSubstringsBlocksAndCells: a replace_all
// substring reports its replacements, a block body that ends in a newline
// replaces the heading body, and a formula in a cell below the grid grows
// the sheet and keeps no stale value.
func TestMountSession_applyWritesSubstringsBlocksAndCells(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	ms := richApplySession(t, nil)

	replaced, replaceErr := ms.Apply(ctx, "/workspace/work/twice.txt", vfs.Mutation{Old: richStr("x"), New: richStr("y"), ReplaceAll: true})
	_, blockErr := ms.Apply(ctx, "/workspace/work/note.md", vfs.Mutation{BlockID: "intro", Body: richStr("new body\n")})
	_, cellErr := ms.Apply(ctx, "/workspace/work/Budget.xlsx", vfs.Mutation{BlockID: "Budget!A4", Body: richStr("=B2*2")})

	if replaceErr != nil || replaced.Replacements != 2 || !strings.Contains(replaced.String(), "replacements=2") {
		t.Fatalf("replace_all = %v err=%v, want 2 replacements", replaced, replaceErr)
	}
	if twice, err := ms.Route(ctx, "/workspace/work/twice.txt").ReadFile(ctx); err != nil || string(twice) != "y-y" {
		t.Fatalf("twice.txt = %q err=%v", twice, err)
	}
	if blockErr != nil {
		t.Fatal(blockErr)
	}
	if note, err := ms.Route(ctx, "/workspace/work/note.md").ReadFile(ctx); err != nil || string(note) != "# Intro\nnew body\n# Next\nmore\n" {
		t.Fatalf("note.md = %q err=%v", note, err)
	}
	if cellErr != nil {
		t.Fatal(cellErr)
	}
	doc, err := ms.Route(ctx, "/workspace/work/Budget.xlsx").ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	g, _ := vfs.AsGrid(doc)
	if sh := g.Sheets()[0]; sh.Rows != 4 || sh.Cells[3][0].Input != "=B2*2" || sh.Cells[3][0].Value != "" {
		t.Fatalf("grown sheet = %+v", sh)
	}
}
