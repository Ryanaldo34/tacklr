package tacklr

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

// vfsTools closes over session mounts. Rev checks live here so MountSession
// stays a thin path/IR API (no high-level ReplaceLinesAt surface).
type vfsTools struct {
	ms *vfs.MountSession
}

// find_files walk budgets (temporary thin tool until run_command).
const (
	defaultFindFilesMaxResults = 50
	maxFindFilesMaxResults     = 200
	defaultFindFilesMaxDepth   = 8
	maxFindFilesMaxDepth       = 32
)

func newVFSTools(ms *vfs.MountSession) []*Tool {
	v := vfsTools{ms: ms}
	return []*Tool{
		v.pathOp("list", "List {path}", streaming.ToolCategoryRead, ToolReadAccess, 30*time.Second,
			`List a virtual directory (absolute paths like /work). Prefer run_command → ls when the session has a FUSE projection. No host shell.`,
			func(ctx context.Context, p string, rt HarnessRuntime) (string, error) {
				rt.EmitUpdate("Listing " + p)
				ents, err := v.ms.ReadDir(ctx, p)
				if err != nil {
					return "", err
				}
				var b strings.Builder
				fmt.Fprintf(&b, "path=%s count=%d\n", p, len(ents))
				for _, e := range ents {
					kind := "file"
					if e.IsDir {
						kind = "dir"
					}
					fmt.Fprintf(&b, "%s\t%s\n", kind, e.Name)
				}
				return b.String(), nil
			}),
		v.pathOp("stat", "Stat {path}", streaming.ToolCategoryRead, ToolReadAccess, 15*time.Second,
			`Stat a virtual path (size, mtime, is_dir). Prefer run_command → stat / ls -l when the session has a FUSE projection. No host paths.`,
			func(ctx context.Context, p string, rt HarnessRuntime) (string, error) {
				rt.EmitUpdate("Stat " + p)
				fi, err := v.ms.Stat(ctx, p)
				if err != nil {
					return "", err
				}
				return fmt.Sprintf("path=%s name=%s size=%d is_dir=%v mtime=%s",
					p, fi.Name, fi.Size, fi.IsDir, fi.ModTime.UTC().Format(time.RFC3339)), nil
			}),
		v.pathOp("mkdir", "Mkdir {path}", streaming.ToolCategoryEdit, ToolWriteAccess, 30*time.Second,
			`Create a directory and parents on the virtual filesystem.`,
			func(ctx context.Context, p string, rt HarnessRuntime) (string, error) {
				rt.EmitUpdate("Mkdir " + p)
				if err := v.ms.MkdirAll(ctx, p); err != nil {
					return "", err
				}
				return "ok path=" + p, nil
			}),
		v.pathOp("remove", "Remove {path}", streaming.ToolCategoryDelete, ToolWriteAccess, 30*time.Second,
			`Remove a file or empty directory on the virtual filesystem.`,
			func(ctx context.Context, p string, rt HarnessRuntime) (string, error) {
				rt.EmitUpdate("Remove " + p)
				if err := v.ms.Remove(ctx, p); err != nil {
					return "", err
				}
				return "ok path=" + p, nil
			}),
		v.newFindFiles(),
		v.newReadLines(),
		v.newReplaceLines(),
		v.newReplaceText(),
		v.newWrite(),
	}
}

type findFilesArgs struct {
	Path       string `json:"path" desc:"Absolute virtual directory (or file) to walk from."`
	Name       string `json:"name,omitempty" desc:"Optional substring or simple glob (* ?) matched against base names only."`
	MaxResults int    `json:"max_results,omitempty" desc:"Max paths to return (default 50, max 200)."`
	MaxDepth   int    `json:"max_depth,omitempty" desc:"Max directory depth from path (default 8 when omitted or 0, max 32)."`
}

func (v vfsTools) newFindFiles() *Tool {
	return NewTool(ToolConfig{
		Name:        "find_files",
		DisplayName: "Find files {path}",
		Description: `Bounded live walk of the virtual filesystem for path names. Prefer run_command → fd / find when the session has a FUSE projection.

Matches base names against an optional name filter (substring or simple * ? glob). Returns absolute virtual paths only — no host paths. Does not search file contents (use run_command → rg, or find_content for indexed text).`,
		Category: streaming.ToolCategorySearch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args findFilesArgs, rt HarnessRuntime) (string, error) {
			root, err := absVirtual(args.Path)
			if err != nil {
				return "", err
			}
			maxRes := args.MaxResults
			if maxRes <= 0 {
				maxRes = defaultFindFilesMaxResults
			}
			if maxRes > maxFindFilesMaxResults {
				maxRes = maxFindFilesMaxResults
			}
			// JSON omit and 0 both mean default depth.
			maxDepth := args.MaxDepth
			if maxDepth <= 0 {
				maxDepth = defaultFindFilesMaxDepth
			}
			if maxDepth > maxFindFilesMaxDepth {
				maxDepth = maxFindFilesMaxDepth
			}
			pat := strings.TrimSpace(args.Name)
			rt.EmitUpdate("Finding files under " + root)
			var hits []string
			if err := walkFind(ctx, v.ms, root, 0, maxDepth, pat, maxRes, &hits); err != nil {
				return "", err
			}
			var b strings.Builder
			fmt.Fprintf(&b, "root=%s count=%d\n", root, len(hits))
			for _, p := range hits {
				b.WriteString(p)
				b.WriteByte('\n')
			}
			return b.String(), nil
		},
	})
}

func walkFind(ctx context.Context, ms *vfs.MountSession, cur string, depth, maxDepth int, namePat string, maxRes int, hits *[]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(*hits) >= maxRes {
		return nil
	}
	// Root only: one Stat. Nested dirs use DirEntry.IsDir (no re-Stat).
	st, err := ms.Stat(ctx, cur)
	if err != nil {
		return err
	}
	if !st.IsDir {
		if matchFindName(path.Base(cur), namePat) {
			*hits = append(*hits, cur)
		}
		return nil
	}
	return walkFindDir(ctx, ms, cur, depth, maxDepth, namePat, maxRes, hits)
}

func walkFindDir(ctx context.Context, ms *vfs.MountSession, cur string, depth, maxDepth int, namePat string, maxRes int, hits *[]string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(*hits) >= maxRes || depth >= maxDepth {
		return nil
	}
	ents, err := ms.ReadDir(ctx, cur)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if len(*hits) >= maxRes {
			return nil
		}
		child := path.Join(cur, e.Name)
		if e.IsDir {
			if err := walkFindDir(ctx, ms, child, depth+1, maxDepth, namePat, maxRes, hits); err != nil {
				return err
			}
			continue
		}
		if matchFindName(e.Name, namePat) {
			*hits = append(*hits, child)
		}
	}
	return nil
}

func matchFindName(base, pat string) bool {
	if pat == "" {
		return true
	}
	if strings.ContainsAny(pat, "*?") {
		ok, err := path.Match(pat, base)
		return err == nil && ok
	}
	return strings.Contains(base, pat)
}

type pathArgs struct {
	Path string `json:"path" desc:"Absolute virtual path (e.g. /work/main.go). Never a host path."`
}

type readLinesArgs struct {
	Path    string `json:"path" desc:"Absolute virtual path to read."`
	Start   int    `json:"start,omitempty" desc:"1-based start line (inclusive). Ignored when block_id is set."`
	End     int    `json:"end,omitempty" desc:"1-based end line (exclusive). Ignored when block_id is set."`
	BlockID string `json:"block_id,omitempty" desc:"Structured block id (e.g. heading path installation or api/errors). Media-agnostic."`
	Outline bool   `json:"outline,omitempty" desc:"If true, include structured block outline when available."`
}

type replaceLinesArgs struct {
	Path           string   `json:"path" desc:"Absolute virtual path to edit."`
	Rev            string   `json:"rev" desc:"Content hash from the latest read or successful write for this path."`
	Start          int      `json:"start,omitempty" desc:"1-based start line (inclusive). Ignored when block_id is set."`
	End            int      `json:"end,omitempty" desc:"1-based end line (exclusive). Ignored when block_id is set."`
	Lines          []string `json:"lines,omitempty" desc:"Replacement lines (no embedded newlines). Empty deletes the span. Or use body."`
	Body           string   `json:"body,omitempty" desc:"Replacement body as one string (split on newlines). Used if lines is empty."`
	BlockID        string   `json:"block_id,omitempty" desc:"Replace this structured block's body (or full span if include_heading)."`
	IncludeHeading bool     `json:"include_heading,omitempty" desc:"When block_id is a heading, replace the heading line too."`
}

type replaceTextArgs struct {
	Path       string `json:"path" desc:"Absolute virtual path to edit."`
	Rev        string `json:"rev" desc:"Content hash from the latest read/write for this path."`
	Old        string `json:"old" desc:"Exact substring to find. Must be unique unless replace_all."`
	New        string `json:"new" desc:"Replacement text."`
	ReplaceAll bool   `json:"replace_all,omitempty" desc:"Replace every occurrence of old."`
}

type writeArgs struct {
	Path    string `json:"path" desc:"Absolute virtual path to write."`
	Content string `json:"content" desc:"Full new file body (UTF-8)."`
	Rev     string `json:"rev,omitempty" desc:"Required when path exists: hash from latest read/write. Omit only to create."`
}

func (v vfsTools) pathOp(
	name, display string,
	cat streaming.ToolCategory,
	access mapset.Set[ToolPermission],
	timeout time.Duration,
	desc string,
	fn func(context.Context, string, HarnessRuntime) (string, error),
) *Tool {
	return NewTool(ToolConfig{
		Name: name, DisplayName: display, Description: desc,
		Category: cat, Access: access, Timeout: timeout,
		Handler: func(ctx context.Context, args pathArgs, rt HarnessRuntime) (string, error) {
			p, err := absVirtual(args.Path)
			if err != nil {
				return "", err
			}
			return fn(ctx, p, rt)
		},
	})
}

func (v vfsTools) newReadLines() *Tool {
	return NewTool(ToolConfig{
		Name:        "read_lines",
		DisplayName: "Read {path}",
		Description: `Read a virtual path via IR: line window and/or structured block.

Use start/end for half-open 1-based lines, or block_id for a structured region (e.g. Markdown heading path). Set outline=true to list blocks when the document has structure. Returns rev — pass it to replace_lines / replace_text / write. Prefer block_id for large doc edits; lines for small patches.`,
		Category: streaming.ToolCategoryRead,
		Access:   ToolReadAccess,
		Timeout:  60 * time.Second,
		Handler: func(ctx context.Context, args readLinesArgs, rt HarnessRuntime) (string, error) {
			p, err := absVirtual(args.Path)
			if err != nil {
				return "", err
			}
			rt.EmitUpdate("Reading " + p)

			// Structured path: need full IR for blocks.
			if args.BlockID != "" || args.Outline {
				doc, err := v.ms.ReadText(ctx, p)
				if err != nil {
					return "", err
				}
				rev := vfs.ContentHash(doc.Text())
				var blocks []vfs.Block
				if s, ok := doc.(vfs.Structured); ok {
					blocks = s.Blocks()
				}
				var b strings.Builder
				fmt.Fprintf(&b, "path=%s rev=%s media_type=%s line_count=%d\n",
					p, rev, doc.MediaType(), doc.LineCount())
				if args.Outline {
					if len(blocks) > 0 {
						b.WriteString("outline:\n")
						for _, bl := range blocks {
							fmt.Fprintf(&b, "  %s kind=%s level=%d L%d-L%d %q\n",
								bl.ID, bl.Kind, bl.Style.Level, bl.Style.Span.StartLine, bl.Style.Span.EndLine, bl.Text)
						}
					}
				}
				start, end := args.Start, args.End
				if args.BlockID != "" {
					if len(blocks) == 0 {
						return "", fmt.Errorf("no structured blocks on this document")
					}
					bl, ok := vfs.FindBlock(blocks, args.BlockID)
					if !ok {
						return "", fmt.Errorf("unknown block_id %q", args.BlockID)
					}
					start, end = bl.Style.Span.StartLine, bl.Style.Span.EndLine
					fmt.Fprintf(&b, "block_id=%s\n", bl.ID)
				}
				if start > 0 && end >= start {
					win, err := lineWindowFromTextDoc(doc, start, end)
					if err != nil {
						return "", err
					}
					fmt.Fprintf(&b, "start=%d end=%d returned=%d eof=%v next_start=%d\n",
						win.start, win.end, len(win.lines), win.eof, win.next)
					for i, line := range win.lines {
						fmt.Fprintf(&b, "%6d|%s\n", win.start+i, line)
					}
				}
				return b.String(), nil
			}

			if args.Start < 1 || args.End < args.Start {
				return "", fmt.Errorf("invalid range start=%d end=%d (or set block_id / outline)", args.Start, args.End)
			}
			win, err := v.ms.ReadLines(ctx, p, args.Start, args.End)
			if err != nil {
				return "", err
			}
			rev := win.Rev
			if rev.Hash == "" {
				if r, rerr := v.ms.ContentRev(ctx, p); rerr == nil {
					rev = r
				}
			}
			var b strings.Builder
			fmt.Fprintf(&b, "path=%s rev=%s start=%d end=%d returned=%d eof=%v next_start=%d\n",
				win.Path, rev.Hash, win.Start, win.End, win.Returned, win.EOF, win.NextStart)
			for i, line := range win.Lines {
				fmt.Fprintf(&b, "%6d|%s\n", win.Start+i, line)
			}
			return b.String(), nil
		},
	})
}

func (v vfsTools) newReplaceLines() *Tool {
	return NewTool(ToolConfig{
		Name:        "replace_lines",
		DisplayName: "Replace {path}",
		Description: `Replace content in a virtual file using a content rev.

Provide either start/end + lines, or block_id + lines/body (structured region; works for Markdown headings and later Docs/Word blocks). rev must match session-visible body. On stale rev, re-read and retry.`,
		Category: streaming.ToolCategoryEdit,
		Access:   ToolWriteAccess,
		Timeout:  60 * time.Second,
		Handler: func(ctx context.Context, args replaceLinesArgs, rt HarnessRuntime) (string, error) {
			p, err := absVirtual(args.Path)
			if err != nil {
				return "", err
			}
			if strings.TrimSpace(args.Rev) == "" {
				return "", fmt.Errorf("rev is required")
			}
			lines := args.Lines
			if len(lines) == 0 && args.Body != "" {
				lines = strings.Split(args.Body, "\n")
				// Trailing newline in body → trailing empty element; OK (matches TextDocument).
				if strings.HasSuffix(args.Body, "\n") && len(lines) > 0 && lines[len(lines)-1] == "" {
					lines = lines[:len(lines)-1]
				}
			}
			rt.EmitUpdate("Editing " + p)
			doc, err := v.loadMatching(ctx, p, args.Rev)
			if err != nil {
				return "", err
			}
			start, end := args.Start, args.End
			if args.BlockID != "" {
				var blocks []vfs.Block
				if s, ok := doc.(vfs.Structured); ok {
					blocks = s.Blocks()
				}
				bl, ok := vfs.FindBlock(blocks, args.BlockID)
				if !ok {
					return "", fmt.Errorf("unknown block_id %q", args.BlockID)
				}
				start, end, err = vfs.BlockReplaceSpan(bl, args.IncludeHeading)
				if err != nil {
					return "", err
				}
			}
			if start < 1 || end < start {
				return "", fmt.Errorf("invalid range start=%d end=%d (or set block_id)", start, end)
			}
			if err := doc.ReplaceLines(start, end, lines); err != nil {
				return "", err
			}
			return v.stage(ctx, doc)
		},
	})
}

type lineWin struct {
	start, end, next int
	lines            []string
	eof              bool
}

func lineWindowFromTextDoc(doc vfs.Textual, start, end int) (lineWin, error) {
	n := doc.LineCount()
	if start < 1 || end < start {
		return lineWin{}, fmt.Errorf("invalid range")
	}
	eof := false
	if end > n+1 {
		end = n + 1
		eof = true
	}
	if start > n+1 {
		return lineWin{}, vfs.ErrLineOutOfRange
	}
	lines, err := doc.Lines(start, end)
	if err != nil {
		return lineWin{}, err
	}
	return lineWin{start: start, end: end, lines: lines, eof: eof, next: start + len(lines)}, nil
}

func (v vfsTools) newReplaceText() *Tool {
	return NewTool(ToolConfig{
		Name:        "replace_text",
		DisplayName: "Replace text {path}",
		Description: `Replace an exact substring, gated by content rev.

When replace_all is false, old must occur exactly once. Prefer for small unique patches; use replace_lines for spans. Persists immediately.`,
		Category: streaming.ToolCategoryEdit,
		Access:   ToolWriteAccess,
		Timeout:  60 * time.Second,
		Handler: func(ctx context.Context, args replaceTextArgs, rt HarnessRuntime) (string, error) {
			p, err := absVirtual(args.Path)
			if err != nil {
				return "", err
			}
			if strings.TrimSpace(args.Rev) == "" {
				return "", fmt.Errorf("rev is required")
			}
			if args.Old == "" {
				return "", fmt.Errorf("old is required")
			}
			rt.EmitUpdate("Editing " + p)
			doc, err := v.loadMatching(ctx, p, args.Rev)
			if err != nil {
				return "", err
			}
			body := doc.Text()
			n := strings.Count(body, args.Old)
			switch {
			case n == 0:
				return "", fmt.Errorf("old text not found")
			case !args.ReplaceAll && n != 1:
				return "", fmt.Errorf("old text occurs %d times (need unique match or replace_all)", n)
			}
			if args.ReplaceAll {
				doc.SetText(strings.ReplaceAll(body, args.Old, args.New))
			} else {
				doc.SetText(strings.Replace(body, args.Old, args.New, 1))
			}
			out, err := v.stage(ctx, doc)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%s replacements=%d", out, n), nil
		},
	})
}

func (v vfsTools) newWrite() *Tool {
	return NewTool(ToolConfig{
		Name:        "write",
		DisplayName: "Write {path}",
		Description: `Write a full file body (create or replace). Persists immediately.

When the path exists, rev is required and must match. Prefer replace_lines / replace_text for partial edits.`,
		Category: streaming.ToolCategoryEdit,
		Access:   ToolWriteAccess,
		Timeout:  60 * time.Second,
		Handler: func(ctx context.Context, args writeArgs, rt HarnessRuntime) (string, error) {
			p, err := absVirtual(args.Path)
			if err != nil {
				return "", err
			}
			rt.EmitUpdate("Writing " + p)
			if _, err := v.ms.Stat(ctx, p); err == nil {
				if strings.TrimSpace(args.Rev) == "" {
					return "", fmt.Errorf("rev required when path exists")
				}
				cur, err := v.ms.ContentRev(ctx, p)
				if err != nil {
					return "", err
				}
				if cur.Hash != args.Rev {
					return "", vfs.ErrStaleContent
				}
			} else if !errors.Is(err, vfs.ErrNotExist) {
				return "", err
			}
			if len(args.Content) > vfs.MaxReadFileBytes {
				return "", vfs.ErrTooLarge
			}
			mt, err := v.ms.Classify(ctx, p, []byte(args.Content))
			if err != nil {
				return "", err
			}
			return v.stage(ctx, vfs.NewTextDocument(p, mt, "utf-8", args.Content))
		},
	})
}

func (v vfsTools) loadMatching(ctx context.Context, p, expected string) (vfs.Textual, error) {
	doc, err := v.ms.ReadText(ctx, p)
	if err != nil {
		return nil, err
	}
	if vfs.ContentHash(doc.Text()) != expected {
		return nil, vfs.ErrStaleContent
	}
	return doc, nil
}

func (v vfsTools) stage(ctx context.Context, doc vfs.Textual) (string, error) {
	if err := v.ms.WriteDocument(ctx, doc); err != nil {
		return "", err
	}
	return fmt.Sprintf("path=%s rev=%s line_count=%d", doc.Path(), vfs.ContentHash(doc.Text()), doc.LineCount()), nil
}

func absVirtual(p string) (string, error) {
	p = strings.TrimSpace(p)
	if p == "" || !path.IsAbs(p) {
		return "", fmt.Errorf("path must be an absolute virtual path")
	}
	if strings.ContainsAny(p, "\\\x00") {
		return "", vfs.ErrInvalidPath
	}
	return path.Clean(p), nil
}
