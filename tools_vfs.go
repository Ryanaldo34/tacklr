package tacklr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ryanaldo34/tacklr/internal/command"
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
	return []*Tool{v.newRead(), v.newWrite(), v.newWriteDocument(), v.newWriteSpreadsheet()}
}

type readArgs struct {
	Path    string `json:"path" desc:"Absolute virtual path."`
	Start   int    `json:"start,omitempty" desc:"1-based first line (inclusive). Sheets: first row of the sheet in block_id. Ignored when block_id is a cell or A1 range."`
	End     int    `json:"end,omitempty" desc:"1-based last line (exclusive). Sheets: last row (exclusive). Ignored when block_id is a cell or A1 range."`
	BlockID string `json:"block_id,omitempty" desc:"Markdown heading path, Docs/Word block id, sheet name, Sheet!A1, or Sheet!A1:C3."`
	Outline bool   `json:"outline,omitempty" desc:"List headings, Docs/Word blocks and tabs, or sheets and named ranges."`
}

type writeTextArgs struct {
	Path       string   `json:"path" desc:"Absolute virtual path."`
	Content    *string  `json:"content,omitempty" desc:"Full file body. Creates the path if missing."`
	Start      *int     `json:"start,omitempty" desc:"1-based first line to replace (inclusive). Use with end and lines or body."`
	End        *int     `json:"end,omitempty" desc:"1-based last line to replace (exclusive)."`
	Lines      []string `json:"lines,omitempty" desc:"Replacement lines with no embedded newlines. Empty deletes the span."`
	Body       *string  `json:"body,omitempty" desc:"Replacement text split on newlines when lines is empty."`
	Old        *string  `json:"old,omitempty" desc:"Exact substring that must still be in the current file. Must be unique unless replace_all."`
	New        *string  `json:"new,omitempty" desc:"Replacement for old. Omit to delete the substring."`
	ReplaceAll bool     `json:"replace_all,omitempty" desc:"Replace every old match. Default is one unique match."`
}

type writeDocumentArgs struct {
	Path    string   `json:"path" desc:"Absolute virtual path."`
	Content *string  `json:"content,omitempty" desc:"Full UTF-8 HTML body. Creates the path if missing. Extensionless HTML becomes a native Doc/Word on Drive/Graph."`
	Start   *int     `json:"start,omitempty" desc:"1-based first HTML line to replace (inclusive). Use with end and lines or body."`
	End     *int     `json:"end,omitempty" desc:"1-based last HTML line to replace (exclusive)."`
	Lines   []string `json:"lines,omitempty" desc:"Replacement HTML lines with no embedded newlines. Empty deletes the span."`
	Body    *string  `json:"body,omitempty" desc:"Replacement text split on newlines when lines is empty."`
	BlockID string   `json:"block_id,omitempty" desc:"Block id from read outline=true."`
	TabID   string   `json:"tab_id,omitempty" desc:"Required when the document has more than one tab."`
}

type writeSpreadsheetArgs struct {
	Path    string           `json:"path" desc:"Absolute virtual path."`
	BlockID string           `json:"block_id,omitempty" desc:"Existing spreadsheet: Sheet!A1 (one cell)."`
	Body    *string          `json:"body,omitempty" desc:"Cell value for block_id Sheet!A1."`
	Lines   []string         `json:"lines,omitempty" desc:"Cell value as lines when body is empty."`
	Format  *vfs.FormatPatch `json:"format,omitempty" desc:"Cell format on Sheet!A1. Omit a field to leave it. Value-only writes leave format; format-only writes leave the value."`
	Content *string          `json:"content,omitempty" desc:"Create a new spreadsheet only. TSV/CSV body on a path that does not exist yet."`
}

func (v vfsTools) newRead() *Tool {
	return NewTool(ToolConfig{
		Name:        "read",
		DisplayName: "Read {path}",
		Description: `Read a virtual file. Use media_type in the result to choose write, write_document, or write_spreadsheet. Path-only returns the first page of numbered lines (Docs/Word as HTML: one block per line). The N| prefix is display only — it is not in the file.

start/end: 1-based half-open window. Sheets: rows of the sheet named in block_id.
block_id: markdown heading, Docs/Word block, sheet, Sheet!A1, or Sheet!A1:C3. One cell and small A1 ranges include format when set; sheet row windows are TSV.
outline: headings, Doc tabs/blocks, or sheets. Sheets path-only is outline.
Knowledge objects: read_object. Names/grep: run_command → ls / rg.`,
		Category: ToolCategoryRead,
		Access:   ToolReadAccess,
		Timeout:  60 * time.Second,
		Handler: func(ctx context.Context, args readArgs, rt HarnessRuntime) (string, error) {
			p, err := vfs.CleanPath(args.Path)
			if err != nil {
				return "", vfsToolErr("read", args.Path, err)
			}
			rt.EmitUpdate("Reading " + p)

			fi, err := v.ms.Stat(ctx, p)
			if err != nil {
				return "", vfsToolErr("read", p, err)
			}
			projected := vfs.IsProjected(fi.MediaType)

			explicitWindow := args.Start > 0 || args.End > 0
			pathOnly := !explicitWindow && args.BlockID == "" && !args.Outline
			if pathOnly && projected && isTabularMedia(fi.MediaType) {
				args.Outline = true
			} else if !explicitWindow && args.BlockID == "" && !args.Outline {
				args.Start = 1
				args.End = 1 + vfs.MaxLinesPerWindow
			}

			if args.BlockID != "" || args.Outline {
				return v.readStructured(ctx, p, args)
			}

			if projected && isTabularMedia(fi.MediaType) {
				return "", vfsToolErr("read", p, vfs.ErrProjected)
			}

			if args.Start < 1 || args.End < args.Start {
				return "", Correctionf(vfs.ErrLineOutOfRange, "read: invalid range start=%d end=%d. Use 1-based half-open start/end, or omit them to read the first page, or pass block_id / outline", args.Start, args.End)
			}
			win, err := v.ms.ReadLines(ctx, p, args.Start, args.End)
			if err != nil {
				return "", vfsToolErr("read", p, err)
			}
			var b strings.Builder
			growLineWindow(&b, 96+len(win.Path)+len(fi.MediaType), win.Lines)
			fmt.Fprintf(&b, "path=%s media_type=%s start=%d end=%d returned=%d eof=%v next_start=%d\n",
				win.Path, fi.MediaType, win.Start, win.End, win.Returned, win.EOF, win.NextStart)
			for i, line := range win.Lines {
				fmt.Fprintf(&b, "%6d|%s\n", win.Start+i, line)
			}
			return b.String(), nil
		},
	})
}

func (v vfsTools) readStructured(ctx context.Context, p string, args readArgs) (string, error) {
	doc, err := v.ms.ReadText(ctx, p)
	if err != nil {
		return "", vfsToolErr("read", p, err)
	}
	projected := vfs.IsProjected(doc.MediaType())
	var blocks []vfs.Block
	if s, ok := doc.(vfs.Structured); ok {
		blocks = s.Blocks()
	}
	var b strings.Builder
	b.Grow(128 + len(p) + len(doc.MediaType()))
	td, tabular := vfs.AsGrid(doc)
	if tabular {
		fmt.Fprintf(&b, "path=%s media_type=%s line_count=%d sheet_count=%d\n",
			p, doc.MediaType(), doc.LineCount(), len(td.Sheets()))
	} else {
		fmt.Fprintf(&b, "path=%s media_type=%s line_count=%d\n",
			p, doc.MediaType(), doc.LineCount())
	}
	if rd, ok := vfs.AsRich(doc); ok && args.Outline {
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
			return "", fmt.Errorf("read %s: this file has no structured blocks. Read the path without block_id (Docs/Word return numbered HTML lines)", p)
		}
		bl, ok := vfs.FindBlock(blocks, args.BlockID)
		if !ok {
			return "", fmt.Errorf("unknown block_id %q. Read the path with outline=true and use an id from that outline", args.BlockID)
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
	return b.String(), nil
}

func (v vfsTools) readTabular(td vfs.Grid, args readArgs, b *strings.Builder) (string, error) {
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
	return b.String(), nil
}

func formatToolCell(c vfs.Cell) string {
	s := "text=" + c.Display()
	if f := c.Format.String(); f != "" {
		s += " format=" + f
	}
	return s
}

func writeRangeFormats(b *strings.Builder, td vfs.Grid, sheet string, r1, c1, r2, c2 int) error {
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

const (
	writeToolName            = "write"
	writeDocumentToolName    = "write_document"
	writeSpreadsheetToolName = "write_spreadsheet"
)

func (v vfsTools) newWrite() *Tool {
	cfg := ToolConfig{
		Name:        writeToolName,
		DisplayName: "Write {path}",
		Description: `Edit a plaintext or source file (not Docs/Word/Sheets). Use after read when media_type is text or code. Exactly one of content, old, or start. old must still be in the file. Do not copy the N| prefix.`,
		Category:    ToolCategoryEdit,
		Access:      ToolWriteAccess,
		Timeout:     60 * time.Second,
		Handler: func(ctx context.Context, args writeTextArgs, rt HarnessRuntime) (string, error) {
			p, err := vfs.CleanPath(args.Path)
			if err != nil {
				return "", vfsToolErr(writeToolName, args.Path, err)
			}
			if _, err := v.requireWriteFamily(ctx, p, writeToolName); err != nil {
				return "", err
			}
			n := 0
			if args.Content != nil {
				n++
			}
			if args.Old != nil {
				n++
			}
			if args.Start != nil {
				n++
			}
			switch {
			case n == 0:
				return "", fmt.Errorf("write %s: nothing to change. Pass content, or old and new, or start and end with lines", p)
			case n > 1:
				return "", fmt.Errorf("write %s: pass only one change: content, or old/new, or start/end lines", p)
			}
			return v.applyWrite(ctx, writeToolName, p, vfs.Mutation{
				Content:    args.Content,
				Old:        args.Old,
				New:        args.New,
				ReplaceAll: args.ReplaceAll,
				Start:      args.Start,
				End:        args.End,
				Lines:      args.Lines,
				Body:       args.Body,
			}, rt)
		},
	}
	return v.withWritePermission(cfg)
}

func (v vfsTools) newWriteDocument() *Tool {
	cfg := ToolConfig{
		Name:        writeDocumentToolName,
		DisplayName: "Write document {path}",
		Description: `Edit a Google Doc or Word file. Use when media_type is a document. Exactly one of content (HTML), start/end lines, or block_id. Multi-tab requires tab_id. Not for .md or spreadsheets.`,
		Category:    ToolCategoryEdit,
		Access:      ToolWriteAccess,
		Timeout:     60 * time.Second,
		Handler: func(ctx context.Context, args writeDocumentArgs, rt HarnessRuntime) (string, error) {
			p, err := vfs.CleanPath(args.Path)
			if err != nil {
				return "", vfsToolErr(writeDocumentToolName, args.Path, err)
			}
			if _, err := v.requireWriteFamily(ctx, p, writeDocumentToolName); err != nil {
				return "", err
			}
			n := 0
			if args.Content != nil {
				n++
			}
			if args.Start != nil {
				n++
			}
			if args.BlockID != "" {
				n++
			}
			switch {
			case n == 0:
				return "", fmt.Errorf("write_document %s: nothing to change. Pass content (HTML), or start and end with lines, or block_id", p)
			case n > 1:
				return "", fmt.Errorf("write_document %s: pass only one change: content, or start/end lines, or block_id", p)
			}
			return v.applyWrite(ctx, writeDocumentToolName, p, vfs.Mutation{
				Content: args.Content,
				Start:   args.Start,
				End:     args.End,
				Lines:   args.Lines,
				Body:    args.Body,
				BlockID: args.BlockID,
				TabID:   args.TabID,
			}, rt)
		},
	}
	return v.withWritePermission(cfg)
}

func (v vfsTools) newWriteSpreadsheet() *Tool {
	cfg := ToolConfig{
		Name:        writeSpreadsheetToolName,
		DisplayName: "Write spreadsheet {path}",
		Description: `Edit one spreadsheet cell. Use when media_type is a spreadsheet. block_id is Sheet!A1. Optional format. Create a new sheet with content on a new path only.`,
		Category:    ToolCategoryEdit,
		Access:      ToolWriteAccess,
		Timeout:     60 * time.Second,
		Handler: func(ctx context.Context, args writeSpreadsheetArgs, rt HarnessRuntime) (string, error) {
			p, err := vfs.CleanPath(args.Path)
			if err != nil {
				return "", vfsToolErr(writeSpreadsheetToolName, args.Path, err)
			}
			exists, err := v.requireWriteFamily(ctx, p, writeSpreadsheetToolName)
			if err != nil {
				return "", err
			}
			hasContent := args.Content != nil
			hasCell := args.BlockID != ""
			switch {
			case hasContent && hasCell:
				return "", fmt.Errorf("write_spreadsheet %s: pass only one change: content on a new path, or block_id Sheet!A1 on an existing sheet", p)
			case !exists && !hasContent:
				return "", fmt.Errorf("write_spreadsheet %s: create a new spreadsheet with content, or write an existing sheet cell with block_id Sheet!A1", p)
			case exists && hasContent:
				return "", Correctionf(vfs.ErrProjected, "%s already exists. write_spreadsheet edits one cell with block_id Sheet!A1; it cannot replace an existing sheet in place", p)
			case exists && !hasCell:
				return "", fmt.Errorf("write_spreadsheet %s: nothing to change. Pass block_id Sheet!A1 with body and/or format", p)
			}
			return v.applyWrite(ctx, writeSpreadsheetToolName, p, vfs.Mutation{
				Content: args.Content,
				BlockID: args.BlockID,
				Body:    args.Body,
				Lines:   args.Lines,
				Format:  args.Format,
			}, rt)
		},
	}
	return v.withWritePermission(cfg)
}

func (v vfsTools) withWritePermission(cfg ToolConfig) *Tool {
	if v.permissionRequired {
		cfg.OnCall = []OnCallFunc{ToolPermissionOnCall}
	}
	return NewTool(cfg)
}

func (v vfsTools) applyWrite(ctx context.Context, tool, p string, mut vfs.Mutation, rt HarnessRuntime) (string, error) {
	rt.EmitUpdate("Writing " + p)
	res, err := v.ms.Apply(ctx, p, mut)
	if err != nil {
		return "", vfsToolErr(tool, p, err)
	}
	return formatWriteResult(res), nil
}

// requireWriteFamily allows create (path missing). An existing file must match tool.
func (v vfsTools) requireWriteFamily(ctx context.Context, p, tool string) (exists bool, err error) {
	fi, err := v.ms.Stat(ctx, p)
	if errors.Is(err, vfs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, vfsToolErr(tool, p, err)
	}
	want := writeToolName
	switch {
	case isTabularMedia(fi.MediaType):
		want = writeSpreadsheetToolName
	case vfs.IsProjected(fi.MediaType):
		want = writeDocumentToolName
	}
	if want != tool {
		return true, Correctionf(vfs.ErrProjected, "%s is %s. Use %s, not %s", p, fi.MediaType, want, tool)
	}
	return true, nil
}

// vfsToolErr maps usage mistakes at the VFS tool, not in the turn processor.
func vfsToolErr(name, path string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, vfs.ErrInvalidWrite), errors.Is(err, vfs.ErrConflict):
		return Correctionf(vfs.ErrInvalidWrite, "%s was not saved. Some of the content may already be in the file. Read the file, then write the full HTML again", path)
	case errors.Is(err, vfs.ErrStaleContent):
		return Correctionf(vfs.ErrStaleContent, "%s changed since you last read it. Read the file again, then write", path)
	case errors.Is(err, vfs.ErrUseHTML):
		return Correctionf(vfs.ErrUseHTML, "%s is a document. Pass content as HTML, for example <h1>Title</h1> and <p>paragraphs</p>", path)
	case errors.Is(err, vfs.ErrEmptyReplace):
		return Correctionf(vfs.ErrEmptyReplace, "%s: %s", path, vfs.ErrEmptyReplace.Error())
	case errors.Is(err, vfs.ErrTabIDRequired):
		return Correctionf(vfs.ErrTabIDRequired, "%s: %s", path, vfs.ErrTabIDRequired.Error())
	case errors.Is(err, vfs.ErrProjected):
		return Correctionf(vfs.ErrProjected, "%s: that write is not supported on this file type. Use write for plaintext, write_document for Docs/Word, write_spreadsheet for Sheet!A1", path)
	case errors.Is(err, vfs.ErrNotExist):
		return Correctionf(vfs.ErrNotExist, "%s: that path does not exist. List the parent with run_command (ls) or read a known path, then retry", name)
	case errors.Is(err, vfs.ErrInvalidPath):
		return Correctionf(vfs.ErrInvalidPath, "%s: that path is not a valid virtual path. Use an absolute path under a mount (for example /workspace/documents/Name)", name)
	case errors.Is(err, vfs.ErrIsDir):
		return Correctionf(vfs.ErrIsDir, "%s: that path is a directory. Read a file inside it, or list it with run_command ls", name)
	case errors.Is(err, vfs.ErrNotDir):
		return Correctionf(vfs.ErrNotDir, "%s: that path is a file, not a directory", name)
	case errors.Is(err, vfs.ErrAuthExpired):
		return fmt.Errorf("%s: cloud credentials expired: %w", name, errors.Join(ErrFailed, err))
	case errors.Is(err, vfs.ErrPermission):
		return Correctionf(vfs.ErrPermission, "%s: permission denied on that path. Use a path the session is allowed to access", name)
	case errors.Is(err, vfs.ErrReadOnly):
		return Correctionf(vfs.ErrReadOnly, "%s: that mount is read-only. Read the file, or write under a writable mount", name)
	case errors.Is(err, vfs.ErrLineOutOfRange):
		return Correctionf(vfs.ErrLineOutOfRange, "%s: that line range is outside the file. Read the path without start/end to see line_count, then request a window that fits", name)
	case errors.Is(err, vfs.ErrInvalidLine):
		return Correctionf(vfs.ErrInvalidLine, "%s: a replacement line contained a newline. Pass each line as its own array entry, with no embedded \\n", name)
	case errors.Is(err, vfs.ErrTooLarge):
		return Correctionf(vfs.ErrTooLarge, "%s: that payload is too large. Write a smaller window or split the document", name)
	case errors.Is(err, vfs.ErrFuseNotMounted):
		return Correctionf(vfs.ErrFuseNotMounted, "%s: the host shell is not mounted. Use read/write on virtual paths instead of run_command", name)
	case errors.Is(err, vfs.ErrNotTextual):
		return Correctionf(vfs.ErrNotTextual, "%s: that file is not text. Use a different path, or write a text/HTML document", name)
	default:
		return err
	}
}

func formatWriteResult(res vfs.ApplyResult) string {
	s := fmt.Sprintf("path=%s line_count=%d", res.Path, res.LineCount)
	if res.Replacements > 0 {
		s += fmt.Sprintf(" replacements=%d", res.Replacements)
	}
	if len(res.Outline) == 0 {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 32*len(res.Outline))
	b.WriteString(s)
	b.WriteString("\noutline:\n")
	for _, bl := range res.Outline {
		fmt.Fprintf(&b, "  %s kind=%s level=%d L%d-L%d %q\n",
			bl.ID, bl.Kind, bl.Style.Level, bl.Style.Span.StartLine, bl.Style.Span.EndLine, bl.Text)
	}
	return b.String()
}

func isTabularMedia(mt string) bool {
	return strings.Contains(mt, "spreadsheet")
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

const runCommandTimeout = 60 * time.Second

type runCommandArgs struct {
	Command string `json:"command" desc:"Host shell command. Runs as /bin/sh -c. cwd is the VFS root. Use relative paths (work/foo). Absolute /work is the host /work until a later jail."`
}

func newRunCommand(ms *vfs.MountSession, permissionRequired bool) *Tool {
	cfg := ToolConfig{
		Name:        "run_command",
		DisplayName: "Run {command}",
		Description: `Run a host shell command as /bin/sh -c. cwd is the VFS root (FUSE mount). Use relative paths (work/foo, ./work/foo). Absolute /work is the host /work until a later jail. Non-zero exit is a successful tool result (exit=N).`,
		Category:    ToolCategoryExecute,
		Access:      ToolExecuteAccess,
		Timeout:     runCommandTimeout,
		Handler: func(ctx context.Context, args runCommandArgs, rt HarnessRuntime) (string, error) {
			dir := ms.HostDir()
			if dir == "" {
				return "", vfsToolErr("run_command", "", vfs.ErrFuseNotMounted)
			}
			cmdStr := strings.TrimSpace(args.Command)
			if cmdStr == "" {
				return "", Correctionf(ErrInvalid, "run_command: command is required. Pass a shell command string, for example ls work")
			}
			rt.EmitUpdate("Running " + cmdStr)
			return command.Run(ctx, dir, cmdStr)
		},
	}
	if permissionRequired {
		cfg.OnCall = []OnCallFunc{ToolPermissionOnCall}
	}
	return NewTool(cfg)
}
