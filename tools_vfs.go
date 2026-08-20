package tacklr

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/streaming"
	"github.com/ryanaldo34/tacklr/vfs"
)

const smallFormatCells = 32

// vfsTools is the thin tool adapter over MountSession.Apply / ReadLines / ReadText.
type vfsTools struct {
	ms                 *vfs.MountSession
	permissionRequired bool
}

func newVFSTools(ms *vfs.MountSession, permissionRequired bool) []*Tool {
	v := vfsTools{ms: ms, permissionRequired: permissionRequired}
	return []*Tool{v.newRead(), v.newWrite()}
}

type readArgs struct {
	Path    string `json:"path" desc:"Absolute virtual path to read."`
	Rev     string `json:"rev,omitempty" desc:"Optional expected content hash from a prior read or write. Mismatch returns stale content."`
	Start   int    `json:"start,omitempty" desc:"1-based start line (inclusive). Ignored when block_id is set."`
	End     int    `json:"end,omitempty" desc:"1-based end line (exclusive). Ignored when block_id is set."`
	BlockID string `json:"block_id,omitempty" desc:"Structured block id (e.g. heading path installation or api/errors). Media-agnostic."`
	Outline bool   `json:"outline,omitempty" desc:"If true, include structured block outline when available."`
	IR      bool   `json:"ir,omitempty" desc:"If true, include media_type/encoding/line_count. Full text= only when no window or block."`
}

type writeBlock struct {
	ID         string            `json:"id,omitempty"`
	Kind       string            `json:"kind"`
	Text       string            `json:"text" desc:"Block body. Docs/Word inline marks: **bold**, _italic_ (or *italic*), ~~strike~~, [label](url). Structure is kind/level — do not put # headings or - lists in text. No marks = plain text (old marks dropped)."`
	Level      int               `json:"level,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

type writeArgs struct {
	Path           string          `json:"path" desc:"Absolute virtual path to write."`
	Rev            string          `json:"rev,omitempty" desc:"Required when path exists: hash from the latest read. Omit only to create (full mode)."`
	Content        *string         `json:"content,omitempty" desc:"Full new file body (UTF-8). Creates or replaces the whole file. Empty creates or truncates. Create-only lift for Docs."`
	IRText         *string         `json:"ir_text,omitempty" desc:"Full IR body. Same as content; if both are set they must match."`
	Start          *int            `json:"start,omitempty" desc:"1-based start line (inclusive). Lines mode."`
	End            *int            `json:"end,omitempty" desc:"1-based end line (exclusive). Required in lines mode."`
	Lines          []string        `json:"lines,omitempty" desc:"Replacement lines (no embedded newlines). Empty deletes the span. Or use body."`
	Body           *string         `json:"body,omitempty" desc:"Replacement body as one string (split on newlines). Used if lines is empty."`
	Old            *string         `json:"old,omitempty" desc:"Exact substring to find. Must be unique unless replace_all."`
	New            *string         `json:"new,omitempty" desc:"Replacement text. Omitted treated as empty."`
	ReplaceAll     bool            `json:"replace_all,omitempty" desc:"Replace every occurrence of old."`
	BlockID        string          `json:"block_id,omitempty" desc:"Replace this structured block's body (or full span if include_heading)."`
	IncludeHeading bool            `json:"include_heading,omitempty" desc:"When block_id is a heading, replace the heading line too."`
	MediaType      string          `json:"media_type,omitempty" desc:"Create-as-Doc: application/vnd.google-apps.document. Ignored when the path exists. Foo.md is never a Doc."`
	Blocks         *[]writeBlock   `json:"blocks,omitempty" desc:"Replace a tab body (SetBlocks) or create a Doc/Word file from IR. text uses **bold** _italic_ ~~strike~~ [label](url)."`
	TabID          string          `json:"tab_id,omitempty" desc:"Required for blocks when the Doc has more than one tab."`
	Format         *vfs.CellFormat `json:"format,omitempty" desc:"Optional cell format bag overlaid on the same block_id range (number, bold, italic, strike, fill, color, align, border). Value-only writes leave format; format-only writes leave values."`
}

func (v vfsTools) newRead() *Tool {
	return NewTool(ToolConfig{
		Name:        "read",
		DisplayName: "Read {path}",
		Description: `Read a virtual file (not a knowledge object). First page by default, or a line window / block.

Path only on ordinary files → start=1 through 1+MaxLinesPerWindow plus rev (pass rev to write).
Path only on Google Docs/Word (projected) → outline. Use rg on the FUSE tree for HTML hits, then read({block_id}) for IR text.
start/end → half-open 1-based window (HTML lines on Docs). block_id → that region's IR text. outline=true → block list (text uses **bold** _italic_ ~~strike~~ [label](url); kind/level is structure). ir=true → media_type/encoding (no HTML dump on Docs).
Sheets: block_id is a sheet (id, title, or slug) or Sheet!A1 / Sheet!A1:C3. start/end are 1-based rows of that sheet, not HTML lines. One-cell and small A1 reads print format when present; row windows stay TSV unless ir is set on a small range.
Knowledge objects with no file: read_object. Live names/grep: run_command → ls / rg.`,
		Category: streaming.ToolCategoryRead,
		Access:   ToolReadAccess,
		Timeout:  60 * time.Second,
		Handler: func(ctx context.Context, args readArgs, rt HarnessRuntime) (string, error) {
			p, err := absVirtual(args.Path)
			if err != nil {
				return "", err
			}
			rt.EmitUpdate("Reading " + p)

			fi, err := v.ms.Stat(ctx, p)
			if err != nil {
				return "", err
			}
			projected := vfs.IsProjected(fi.MediaType)

			explicitWindow := args.Start > 0 || args.End > 0
			pathOnly := !explicitWindow && args.BlockID == "" && !args.Outline && !args.IR
			if pathOnly && projected {
				args.Outline = true
			} else if !explicitWindow && args.BlockID == "" && !args.Outline {
				args.Start = 1
				args.End = 1 + vfs.MaxLinesPerWindow
				explicitWindow = true
			}

			if args.BlockID != "" || args.Outline || args.IR {
				return v.readStructured(ctx, p, args, explicitWindow)
			}

			if projected && fi.MediaType == "application/vnd.google-apps.spreadsheet" {
				return "", vfs.ErrProjected
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
				r, rerr := v.ms.ContentRev(ctx, p)
				if rerr != nil {
					if args.Rev != "" {
						return "", fmt.Errorf("read: %w", rerr)
					}
				} else {
					rev = r
				}
			}
			if args.Rev != "" && args.Rev != rev.Hash {
				return "", vfs.ErrStaleContent
			}
			var b strings.Builder
			growLineWindow(&b, 96+len(win.Path)+len(rev.Hash), win.Lines)
			fmt.Fprintf(&b, "path=%s rev=%s start=%d end=%d returned=%d eof=%v next_start=%d\n",
				win.Path, rev.Hash, win.Start, win.End, win.Returned, win.EOF, win.NextStart)
			for i, line := range win.Lines {
				fmt.Fprintf(&b, "%6d|%s\n", win.Start+i, line)
			}
			return b.String(), nil
		},
	})
}

func (v vfsTools) readStructured(ctx context.Context, p string, args readArgs, explicitWindow bool) (string, error) {
	doc, err := v.ms.ReadText(ctx, p)
	if err != nil {
		return "", err
	}
	rev := vfs.ContentToken(doc)
	if args.Rev != "" && args.Rev != rev {
		return "", vfs.ErrStaleContent
	}
	projected := vfs.IsProjected(doc.MediaType())
	var blocks []vfs.Block
	if s, ok := doc.(vfs.Structured); ok {
		blocks = s.Blocks()
	}
	var b strings.Builder
	b.Grow(128 + len(p) + len(rev) + len(doc.MediaType()))
	td, tabular := doc.(*vfs.TabularDocument)
	if args.IR {
		if tabular {
			fmt.Fprintf(&b, "path=%s rev=%s media_type=%s encoding=%s line_count=%d sheet_count=%d\n",
				p, rev, doc.MediaType(), doc.Encoding(), doc.LineCount(), len(td.Sheets()))
		} else {
			fmt.Fprintf(&b, "path=%s rev=%s media_type=%s encoding=%s line_count=%d\n",
				p, rev, doc.MediaType(), doc.Encoding(), doc.LineCount())
		}
	} else {
		fmt.Fprintf(&b, "path=%s rev=%s media_type=%s line_count=%d\n",
			p, rev, doc.MediaType(), doc.LineCount())
	}
	if rd, ok := doc.(*vfs.RichDocument); ok && args.Outline {
		if tabs := rd.Tabs(); len(tabs) > 0 {
			b.WriteString("tabs:\n")
			for _, tb := range tabs {
				fmt.Fprintf(&b, "  %s title=%q index=%d\n", tb.ID, tb.Title, tb.Index)
			}
		}
	}
	if tabular && args.Outline {
		if named := td.NamedRanges(); len(named) > 0 {
			b.WriteString("named_ranges:\n")
			for _, n := range named {
				fmt.Fprintf(&b, "  %s sheet_id=%s a1=%s\n", n.Name, n.SheetID, n.A1)
			}
		}
	}
	if args.Outline && len(blocks) > 0 {
		b.WriteString("outline:\n")
		for _, bl := range blocks {
			tab := ""
			if bl.Style.Attributes != nil {
				tab = bl.Style.Attributes["tab_id"]
			}
			if tab != "" {
				fmt.Fprintf(&b, "  %s kind=%s level=%d tab=%s L%d-L%d %q\n",
					bl.ID, bl.Kind, bl.Style.Level, tab, bl.Style.Span.StartLine, bl.Style.Span.EndLine, bl.Text)
			} else {
				fmt.Fprintf(&b, "  %s kind=%s level=%d L%d-L%d %q\n",
					bl.ID, bl.Kind, bl.Style.Level, bl.Style.Span.StartLine, bl.Style.Span.EndLine, bl.Text)
			}
		}
	}
	if tabular {
		return v.readTabular(td, args, &b)
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
		fmt.Fprintf(&b, "block_id=%s\n", bl.ID)
		if projected {
			fmt.Fprintf(&b, "text=%s\n", bl.Text)
			return b.String(), nil
		}
		start, end = bl.Style.Span.StartLine, bl.Style.Span.EndLine
	}
	if start > 0 && end >= start {
		win, err := lineWindowFromTextDoc(doc, start, end)
		if err != nil {
			return "", err
		}
		growLineWindow(&b, 64, win.Lines)
		fmt.Fprintf(&b, "start=%d end=%d returned=%d eof=%v next_start=%d\n",
			win.Start, win.End, win.Returned, win.EOF, win.NextStart)
		for i, line := range win.Lines {
			fmt.Fprintf(&b, "%6d|%s\n", win.Start+i, line)
		}
	}
	if args.IR && args.BlockID == "" && !explicitWindow && !projected {
		fmt.Fprintf(&b, "text=%s\n", doc.Text())
	}
	return b.String(), nil
}

func (v vfsTools) readTabular(td *vfs.TabularDocument, args readArgs, b *strings.Builder) (string, error) {
	if args.BlockID == "" {
		return b.String(), nil
	}
	sheetKey, a1 := vfs.SplitSheetAddr(args.BlockID)
	if a1 != "" {
		r1, c1, r2, c2, err := vfs.ParseA1(a1)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(b, "block_id=%s\n", args.BlockID)
		if r1 == r2 && c1 == c2 {
			cell, err := td.Cell(sheetKey, a1)
			if err != nil {
				return "", err
			}
			fmt.Fprintf(b, "%s\n", formatToolCell(cell))
			return b.String(), nil
		}
		tsv, err := td.ReadRangeTSV(sheetKey, a1)
		if err != nil {
			return "", err
		}
		b.WriteString(tsv)
		if tsv != "" && !strings.HasSuffix(tsv, "\n") {
			b.WriteByte('\n')
		}
		cells := (r2 - r1 + 1) * (c2 - c1 + 1)
		if cells > 0 && cells <= smallFormatCells {
			if err := writeRangeFormats(b, td, sheetKey, r1, c1, r2, c2); err != nil {
				return "", err
			}
		}
		return b.String(), nil
	}
	start, end := args.Start, args.End
	if start < 1 {
		start = 1
		end = 1 + vfs.MaxLinesPerWindow
	} else if end < start {
		end = start + vfs.MaxLinesPerWindow
	}
	sh, lines, err := td.ReadRows(sheetKey, start, end)
	if err != nil {
		return "", err
	}
	fmt.Fprintf(b, "sheet=%s rows=%d cols=%d start=%d\n", sh.Title, sh.Rows, sh.Cols, start)
	growLineWindow(b, 0, lines)
	for i, line := range lines {
		fmt.Fprintf(b, "%6d|%s\n", start+i, line)
	}
	if args.IR {
		endRow := start + len(lines) - 1
		if sh.Cols > 0 && len(lines)*sh.Cols <= smallFormatCells {
			if err := writeRangeFormats(b, td, sheetKey, start, 1, endRow, sh.Cols); err != nil {
				return "", err
			}
		}
	}
	return b.String(), nil
}

func formatToolCell(c vfs.Cell) string {
	s := "text=" + c.Display()
	if f := c.Format.String(); f != "" {
		s += " format=" + f
	}
	return s
}

func writeRangeFormats(b *strings.Builder, td *vfs.TabularDocument, sheet string, r1, c1, r2, c2 int) error {
	for r := r1; r <= r2; r++ {
		for c := c1; c <= c2; c++ {
			cell, err := td.Cell(sheet, vfs.FormatA1(r, c))
			if err != nil {
				return err
			}
			if cell.Format.IsZero() {
				continue
			}
			fmt.Fprintf(b, "%s format=%s\n", vfs.FormatA1(r, c), cell.Format.String())
		}
	}
	return nil
}

func (v vfsTools) newWrite() *Tool {
	cfg := ToolConfig{
		Name:        "write",
		DisplayName: "Write {path}",
		Description: `Write a virtual file: full body, line span, substring, structured block, or Docs blocks. Exactly one mode per call.

Pass rev from read when the path exists. Create only via content or ir_text (empty content creates or truncates), or media_type+blocks for a Google Doc. Foo.md is never a Doc. Extensionless Spec without media_type is plaintext.
Projected Docs/Word: use block_id or blocks. Inline marks in block text: **bold**, _italic_, ~~strike~~, [label](url). kind/level is structure (not # or -). No marks = plain replace (drops old marks). Line/HTML/SetText writes return an error. content lift is create-only. Persists immediately.
Sheets: block_id is a sheet or Sheet!A1:C3 overlay. start/end are rows and line count must equal end-start. Optional format overlays the same range (value-only leaves format; format-only leaves values).`,
		Category: streaming.ToolCategoryEdit,
		Access:   ToolWriteAccess,
		Timeout:  60 * time.Second,
		Handler: func(ctx context.Context, args writeArgs, rt HarnessRuntime) (string, error) {
			p, err := absVirtual(args.Path)
			if err != nil {
				return "", err
			}
			n, full := writeModeCount(args)
			switch {
			case n == 0:
				return "", fmt.Errorf("write: no mutation")
			case n > 1:
				return "", fmt.Errorf("write: exactly one of content|ir_text, old, block_id, start, blocks")
			}
			mut := vfs.Mutation{
				Rev:            args.Rev,
				Old:            args.Old,
				New:            args.New,
				ReplaceAll:     args.ReplaceAll,
				Start:          args.Start,
				End:            args.End,
				Lines:          args.Lines,
				Body:           args.Body,
				BlockID:        args.BlockID,
				IncludeHeading: args.IncludeHeading,
				TabID:          args.TabID,
				MediaType:      args.MediaType,
				Format:         args.Format,
			}
			if full {
				body, err := fullWriteBody(args)
				if err != nil {
					return "", err
				}
				mut.Content = &body
			}
			if args.Blocks != nil {
				next := make([]vfs.Block, 0, len(*args.Blocks))
				for _, wb := range *args.Blocks {
					next = append(next, vfs.Block{
						ID: wb.ID, Kind: wb.Kind, Text: wb.Text,
						Style: vfs.StyleMeta{Level: wb.Level, Attributes: wb.Attributes},
					})
				}
				mut.Blocks = next
			}
			rt.EmitUpdate("Writing " + p)
			res, err := v.ms.Apply(ctx, p, mut)
			if err != nil {
				return "", err
			}
			return res.String(), nil
		},
	}
	if v.permissionRequired {
		cfg.OnCall = []OnCallFunc{ToolPermissionOnCall}
	}
	return NewTool(cfg)
}

func writeModeCount(args writeArgs) (n int, full bool) {
	full = args.Content != nil || args.IRText != nil
	if full {
		n++
	}
	if args.Old != nil {
		n++
	}
	if args.BlockID != "" {
		n++
	}
	if args.Start != nil && args.BlockID == "" {
		n++
	}
	if args.Blocks != nil {
		n++
	}
	return n, full
}

func fullWriteBody(args writeArgs) (string, error) {
	switch {
	case args.Content != nil && args.IRText != nil:
		if *args.Content != *args.IRText {
			return "", fmt.Errorf("write: content and ir_text disagree")
		}
		return *args.Content, nil
	case args.IRText != nil:
		return *args.IRText, nil
	default:
		return *args.Content, nil
	}
}

func lineWindowFromTextDoc(doc vfs.Textual, start, end int) (vfs.LineWindow, error) {
	n := doc.LineCount()
	if start < 1 || end < start {
		return vfs.LineWindow{}, fmt.Errorf("invalid range")
	}
	eof := false
	if end > n+1 {
		end = n + 1
		eof = true
	}
	if start > n+1 {
		return vfs.LineWindow{}, vfs.ErrLineOutOfRange
	}
	lines, err := doc.Lines(start, end)
	if err != nil {
		return vfs.LineWindow{}, err
	}
	return vfs.LineWindow{
		Start: start, End: end, Lines: lines,
		Returned: len(lines), EOF: eof, NextStart: start + len(lines),
	}, nil
}

func growLineWindow(b *strings.Builder, extra int, lines []string) {
	n := extra
	for _, line := range lines {
		n += 8 + len(line)
	}
	b.Grow(n)
}

func absVirtual(p string) (string, error) { return vfs.CleanPath(p) }
