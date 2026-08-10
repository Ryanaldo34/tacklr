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

func newVFSTools(ms *vfs.MountSession) []*Tool {
	if ms == nil {
		return nil
	}
	v := vfsTools{ms: ms}
	return []*Tool{
		v.pathOp("list", "List {path}", streaming.ToolCategoryRead, ToolReadAccess, 30*time.Second,
			`List a virtual directory (absolute paths like /work). No host shell.`,
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
			`Stat a virtual path (size, mtime, is_dir). No host paths.`,
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
		v.newReadLines(),
		v.newReplaceLines(),
		v.newReplaceText(),
		v.newWrite(),
	}
}

type pathArgs struct {
	Path string `json:"path" desc:"Absolute virtual path (e.g. /work/main.go). Never a host path."`
}

type readLinesArgs struct {
	Path  string `json:"path" desc:"Absolute virtual path to read."`
	Start int    `json:"start" desc:"1-based start line (inclusive)."`
	End   int    `json:"end" desc:"1-based end line (exclusive). Half-open [start, end)."`
}

type replaceLinesArgs struct {
	Path  string   `json:"path" desc:"Absolute virtual path to edit."`
	Rev   string   `json:"rev" desc:"Content hash from the latest read_lines or successful write for this path."`
	Start int      `json:"start" desc:"1-based start line (inclusive)."`
	End   int      `json:"end" desc:"1-based end line (exclusive). Half-open [start, end)."`
	Lines []string `json:"lines" desc:"Replacement lines (no embedded newlines). Empty deletes the span."`
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
		DisplayName: "Read lines {path}",
		Description: `Read a half-open line window [start, end) from a virtual path (1-based).

Returns numbered lines and a content rev hash. Pass rev to replace_lines, replace_text, or write. Page with next_start until eof=true. Prefer small windows. No host shell.`,
		Category: streaming.ToolCategoryRead,
		Access:   ToolReadAccess,
		Timeout:  60 * time.Second,
		Handler: func(ctx context.Context, args readLinesArgs, rt HarnessRuntime) (string, error) {
			p, err := absVirtual(args.Path)
			if err != nil {
				return "", err
			}
			if args.Start < 1 || args.End < args.Start {
				return "", fmt.Errorf("invalid range start=%d end=%d", args.Start, args.End)
			}
			rt.EmitUpdate(fmt.Sprintf("Reading %s [%d,%d)", p, args.Start, args.End))
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
		DisplayName: "Replace lines {path}",
		Description: `Replace half-open line span [start, end) using rev from read_lines.

rev must match the current session-visible body. Stages write-back IR (checkpoint/Sync flushes). On stale rev, re-read and retry.`,
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
			rt.EmitUpdate("Editing " + p)
			doc, err := v.loadMatching(ctx, p, args.Rev)
			if err != nil {
				return "", err
			}
			if err := doc.ReplaceLines(args.Start, args.End, args.Lines); err != nil {
				return "", err
			}
			return v.stage(ctx, doc)
		},
	})
}

func (v vfsTools) newReplaceText() *Tool {
	return NewTool(ToolConfig{
		Name:        "replace_text",
		DisplayName: "Replace text {path}",
		Description: `Replace an exact substring, gated by content rev.

When replace_all is false, old must occur exactly once. Prefer for small unique patches; use replace_lines for spans. Stages write-back IR.`,
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
		Description: `Write a full file body (create or replace), write-through to the mount.

When the path exists, rev is required and must match. Prefer replace_lines / replace_text for partial edits (those stage write-back IR).`,
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
			// Write-through so create/replace is visible without Sync.
			if err := v.ms.WriteFile(ctx, p, []byte(args.Content)); err != nil {
				return "", err
			}
			doc := vfs.NewTextDocument(p, "text/plain", "utf-8", args.Content)
			return fmt.Sprintf("path=%s rev=%s line_count=%d", p, vfs.ContentHash(args.Content), doc.LineCount()), nil
		},
	})
}

func (v vfsTools) loadMatching(ctx context.Context, p, expected string) (*vfs.TextDocument, error) {
	doc, err := v.ms.ReadText(ctx, p)
	if err != nil {
		return nil, err
	}
	if vfs.ContentHash(doc.Text()) != expected {
		return nil, vfs.ErrStaleContent
	}
	return doc, nil
}

func (v vfsTools) stage(ctx context.Context, doc *vfs.TextDocument) (string, error) {
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
