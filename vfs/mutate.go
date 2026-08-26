package vfs

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path"
	"strings"
)

// Mutation is one write against a virtual path. Addresses are replace-whole
// (Content) or replace-span (line Start/End, or BlockID including Sheet!A1).
// Old/New finds a span; Blocks is create/replace of structured IR on Apply.
type Mutation struct {
	Rev            string
	Content        *string
	Old            *string
	New            *string
	ReplaceAll     bool
	Start, End     *int
	Lines          []string
	Body           *string
	BlockID        string
	IncludeHeading bool
	Blocks         []Block
	TabID          string
	MediaType      string
	Format         *FormatPatch
}

// ApplyResult is the post-write identity of the document.
type ApplyResult struct {
	Path         string
	Rev          string
	LineCount    int
	Replacements int
	Outline      []Block
}

func (r ApplyResult) String() string {
	s := fmt.Sprintf("path=%s rev=%s line_count=%d", r.Path, r.Rev, r.LineCount)
	if r.Replacements > 0 {
		s += fmt.Sprintf(" replacements=%d", r.Replacements)
	}
	return s
}

func (m Mutation) modeCount() (n int, full bool) {
	full = m.Content != nil
	if full {
		n++
	}
	if m.Old != nil {
		n++
	}
	if m.BlockID != "" {
		n++
	}
	if m.Start != nil && m.BlockID == "" {
		n++
	}
	if m.Blocks != nil {
		n++
	}
	return n, full
}

// Apply persists one mutation. Empty Rev uses the harness lastRev or a live read.
func (ms *MountSession) Apply(ctx context.Context, virtualPath string, mut Mutation) (ApplyResult, error) {
	p, err := CleanPath(virtualPath)
	if err != nil {
		return ApplyResult{}, err
	}
	n, full := mut.modeCount()
	switch {
	case n == 0:
		return ApplyResult{}, fmt.Errorf("write: nothing to change")
	case n > 1:
		return ApplyResult{}, fmt.Errorf("write: pass only one change: content, line range, or block_id")
	}

	fi, err := ms.Stat(ctx, p)
	exists := err == nil
	if err != nil && !errors.Is(err, ErrNotExist) {
		return ApplyResult{}, err
	}
	if !exists && !full && mut.Blocks == nil {
		return ApplyResult{}, ErrNotExist
	}

	switch {
	case full:
		return ms.applyFull(ctx, p, exists, fi, mut)
	case mut.Blocks != nil:
		return ms.applyBlocks(ctx, p, exists, fi, mut)
	case mut.Old != nil:
		return ms.applySubstring(ctx, p, mut)
	case mut.BlockID != "":
		return ms.applyBlock(ctx, p, mut)
	default:
		return ms.applyLines(ctx, p, mut)
	}
}

func (ms *MountSession) applyFull(ctx context.Context, p string, exists bool, fi FileInfo, mut Mutation) (ApplyResult, error) {
	body := ""
	if mut.Content != nil {
		body = *mut.Content
	}
	if len(body) > MaxReadFileBytes {
		return ApplyResult{}, ErrTooLarge
	}
	if exists {
		if IsProjected(fi.MediaType) {
			return ms.applyProjectedFull(ctx, p, mut, body)
		}
		if err := ms.matchRev(ctx, p, mut.Rev); err != nil {
			return ApplyResult{}, err
		}
		return ms.stage(ctx, NewTextDocument(p, fi.MediaType, "utf-8", body))
	}
	n := min(len(body), 512)
	mt := ms.createMediaType(p, mut.MediaType, []byte(body[:n]))
	doc, err := createDocument(p, mt, mut)
	if err != nil {
		return ApplyResult{}, err
	}
	text, ok := doc.(Textual)
	if !ok {
		return ApplyResult{}, ErrNotTextual
	}
	return ms.stage(ctx, text)
}

func (ms *MountSession) applyProjectedFull(ctx context.Context, p string, mut Mutation, body string) (ApplyResult, error) {
	doc, err := ms.checkout(ctx, p, mut.Rev)
	if err != nil {
		return ApplyResult{}, err
	}
	if _, ok := asGridBody(doc); ok {
		return ApplyResult{}, ErrProjected
	}
	rd, ok := AsRich(doc)
	if !ok {
		return ApplyResult{}, ErrProjected
	}
	if !looksLikeHTML(body) {
		return ApplyResult{}, ErrUseHTML
	}
	blocks, err := decodeDocsHTML([]byte(body))
	if err != nil {
		return ApplyResult{}, err
	}
	if len(blocks) == 0 {
		return ApplyResult{}, ErrEmptyReplace
	}
	next, err := mergeTabBlocks(rd, blocks, mut.TabID)
	if err != nil {
		return ApplyResult{}, err
	}
	rd.SetBlocks(next)
	d, _ := asIR(doc)
	return ms.stage(ctx, d)
}

func (ms *MountSession) createMediaType(p, requested string, sample []byte) string {
	if path.Ext(p) != "" {
		return DetectMediaType(path.Base(p), sample)
	}
	if requested != "" {
		return requested
	}
	if looksLikeHTML(string(sample)) {
		if mt := ms.nativeRichMediaType(p); mt != "" {
			return mt
		}
	}
	return DetectMediaType(path.Base(p), sample)
}

func (ms *MountSession) nativeRichMediaType(p string) string {
	e, _, rel, err := ms.table().resolveEntry(p)
	if err != nil {
		return ""
	}
	return nativeRichOf(e.provider, rel)
}

func nativeRichOf(p Provider, rel string) string {
	switch t := p.(type) {
	case *driveProvider:
		return mimeGoogleDocument
	case *graphProvider:
		return extMediaTypes[".docx"]
	case workspaceProvider:
		alias, rest, err := splitAlias(rel)
		if err != nil || alias == "" {
			return ""
		}
		m, err := t.lookup(alias)
		if err != nil {
			return ""
		}
		return nativeRichOf(m.inner, rest)
	default:
		return ""
	}
}

func looksLikeHTML(s string) bool {
	t := strings.TrimSpace(s)
	return strings.HasPrefix(t, "<") && strings.Contains(t, ">")
}

func (ms *MountSession) applySubstring(ctx context.Context, p string, mut Mutation) (ApplyResult, error) {
	if mut.Old == nil || *mut.Old == "" {
		return ApplyResult{}, fmt.Errorf("old is required; pass the exact unique substring to replace")
	}
	doc, err := ms.checkout(ctx, p, mut.Rev)
	if err != nil {
		return ApplyResult{}, err
	}
	if IsProjected(doc.MediaType()) {
		return ApplyResult{}, ErrProjected
	}
	repl := ""
	if mut.New != nil {
		repl = *mut.New
	}
	body := doc.Text()
	n := strings.Count(body, *mut.Old)
	switch {
	case n == 0:
		return ApplyResult{}, fmt.Errorf("old text was not found; read the file and copy the exact substring into old")
	case !mut.ReplaceAll && n != 1:
		return ApplyResult{}, fmt.Errorf("old text occurs %d times; pass replace_all=true or a unique substring", n)
	}
	limit := 1
	if mut.ReplaceAll {
		limit = n
	}
	if err := doc.SetText(strings.Replace(body, *mut.Old, repl, limit)); err != nil {
		return ApplyResult{}, err
	}
	out, err := ms.stage(ctx, doc)
	if err != nil {
		return ApplyResult{}, err
	}
	out.Replacements = n
	return out, nil
}

func (ms *MountSession) applyBlock(ctx context.Context, p string, mut Mutation) (ApplyResult, error) {
	doc, err := ms.checkout(ctx, p, mut.Rev)
	if err != nil {
		return ApplyResult{}, err
	}
	if g, ok := asGridBody(doc); ok {
		d, _ := asIR(doc)
		return ms.applyTabularBlock(ctx, d, g, mut)
	}
	var blocks []Block
	if s, ok := doc.(Structured); ok {
		blocks = s.Blocks()
	}
	bl, ok := FindBlock(blocks, mut.BlockID)
	if !ok {
		return ApplyResult{}, fmt.Errorf("unknown block_id %q; read with outline=true and use an id from that outline", mut.BlockID)
	}
	if mut.TabID != "" && bl.Style.Attributes != nil {
		if got := bl.Style.Attributes["tab_id"]; got != "" && got != mut.TabID {
			return ApplyResult{}, fmt.Errorf("vfs: tab_id %q does not match block %q", mut.TabID, got)
		}
	}
	if rd, ok := AsRich(doc); ok {
		text := strings.Join(replacementLines(mut.Lines, mut.Body), "\n")
		if err := rd.ReplaceBlock(bl.ID, text, mut.IncludeHeading); err != nil {
			return ApplyResult{}, err
		}
		return ms.stage(ctx, doc)
	}
	start, end, err := BlockReplaceSpan(bl, mut.IncludeHeading)
	if err != nil {
		return ApplyResult{}, err
	}
	if err := doc.ReplaceLines(start, end, replacementLines(mut.Lines, mut.Body)); err != nil {
		return ApplyResult{}, err
	}
	return ms.stage(ctx, doc)
}

func (ms *MountSession) applyTabularBlock(ctx context.Context, td *IR, g *gridBody, mut Mutation) (ApplyResult, error) {
	sheetKey, a1 := SplitSheetAddr(mut.BlockID)
	idx, ok := g.findSheet(sheetKey)
	if !ok {
		return ApplyResult{}, fmt.Errorf("unknown block_id %q; read with outline=true and use an id from that outline", mut.BlockID)
	}
	if mut.TabID != "" && g.sheets[idx].ID != "" && g.sheets[idx].ID != mut.TabID {
		return ApplyResult{}, fmt.Errorf("vfs: tab_id %q does not match block %q", mut.TabID, g.sheets[idx].ID)
	}
	hasValue := mut.Body != nil || len(mut.Lines) > 0
	var format *FormatPatch
	if !mut.Format.empty() {
		format = mut.Format
	}
	if !hasValue && format == nil {
		return ApplyResult{}, fmt.Errorf("write: sheet cell needs a value (body or lines) or a format patch")
	}
	if a1 == "" {
		return ApplyResult{}, fmt.Errorf("%w: sheet write requires Sheet!A1", ErrNotSupported)
	}
	r1, c1, r2, c2, err := parseA1(a1)
	if err != nil {
		return ApplyResult{}, err
	}
	if r1 != r2 || c1 != c2 {
		return ApplyResult{}, fmt.Errorf("%w: sheet write is one cell (Sheet!A1)", ErrNotSupported)
	}
	var input *string
	if hasValue {
		text := strings.Join(replacementLines(mut.Lines, mut.Body), "\n")
		input = &text
	}
	if err := g.overlayCell(idx, r1, c1, input, format); err != nil {
		return ApplyResult{}, err
	}
	return ms.stage(ctx, td)
}

func (ms *MountSession) applyBlocks(ctx context.Context, p string, exists bool, fi FileInfo, mut Mutation) (ApplyResult, error) {
	next := make([]Block, 0, len(mut.Blocks))
	for _, b := range mut.Blocks {
		attrs := maps.Clone(b.Style.Attributes)
		if mut.TabID != "" {
			if attrs == nil {
				attrs = map[string]string{}
			}
			attrs["tab_id"] = mut.TabID
		}
		b.Style.Attributes = attrs
		next = append(next, b)
	}
	if !exists {
		mt := ms.createMediaType(p, mut.MediaType, nil)
		created := mut
		created.Blocks = next
		doc, err := createDocument(p, mt, created)
		if err != nil {
			return ApplyResult{}, err
		}
		text, ok := doc.(Textual)
		if !ok {
			return ApplyResult{}, ErrNotTextual
		}
		return ms.stage(ctx, text)
	}
	if !IsProjected(fi.MediaType) {
		return ApplyResult{}, ErrProjected
	}
	if len(next) == 0 {
		return ApplyResult{}, ErrEmptyReplace
	}
	doc, err := ms.checkout(ctx, p, mut.Rev)
	if err != nil {
		return ApplyResult{}, err
	}
	if _, ok := asGridBody(doc); ok {
		return ApplyResult{}, fmt.Errorf("%w: sheet replace uses create (content or blocks on a new path)", ErrNotSupported)
	}
	rd, ok := AsRich(doc)
	if !ok {
		return ApplyResult{}, ErrProjected
	}
	merged, err := mergeTabBlocks(rd, next, mut.TabID)
	if err != nil {
		return ApplyResult{}, err
	}
	rd.SetBlocks(merged)
	d, _ := asIR(doc)
	return ms.stage(ctx, d)
}

func mergeTabBlocks(rd Rich, next []Block, tabID string) ([]Block, error) {
	tabs := rd.Tabs()
	if len(tabs) > 1 && tabID == "" {
		return nil, ErrTabIDRequired
	}
	if tabID == "" && len(tabs) == 1 {
		tabID = tabs[0].ID
	}
	if tabID != "" {
		for i := range next {
			if blockAttr(next[i], "tab_id") == "" {
				setAttr(&next[i], "tab_id", tabID)
			}
		}
		if len(tabs) > 0 {
			existing := rd.Blocks()
			keep := make([]Block, 0, len(existing))
			for _, b := range existing {
				if blockAttr(b, "tab_id") != tabID {
					keep = append(keep, b)
				}
			}
			next = append(next, keep...)
		}
	}
	return next, nil
}

func (ms *MountSession) applyLines(ctx context.Context, p string, mut Mutation) (ApplyResult, error) {
	if mut.End == nil || *mut.Start < 1 || *mut.End < *mut.Start {
		return ApplyResult{}, fmt.Errorf("invalid range start=%d end=%v; use 1-based half-open start/end, or omit them and pass content", *mut.Start, mut.End)
	}
	doc, err := ms.checkout(ctx, p, mut.Rev)
	if err != nil {
		return ApplyResult{}, err
	}
	if _, ok := asGridBody(doc); ok {
		return ApplyResult{}, ErrProjected
	}
	if err := doc.ReplaceLines(*mut.Start, *mut.End, replacementLines(mut.Lines, mut.Body)); err != nil {
		return ApplyResult{}, err
	}
	return ms.stage(ctx, doc)
}

func replacementLines(lines []string, body *string) []string {
	if len(lines) > 0 || body == nil || *body == "" {
		return lines
	}
	out := strings.Split(*body, "\n")
	if strings.HasSuffix(*body, "\n") && len(out) > 0 && out[len(out)-1] == "" {
		out = out[:len(out)-1]
	}
	return out
}

func (ms *MountSession) matchRev(ctx context.Context, p, rev string) error {
	expected := strings.TrimSpace(rev)
	if expected == "" {
		expected = ms.storedRev(p)
	}
	if expected == "" {
		return nil
	}
	cur, err := ms.ContentRev(ctx, p)
	if err != nil {
		return err
	}
	if cur.Hash != expected {
		return ErrStaleContent
	}
	return nil
}

func (ms *MountSession) checkout(ctx context.Context, p, rev string) (Textual, error) {
	expected := strings.TrimSpace(rev)
	if expected == "" {
		expected = ms.storedRev(p)
	}
	if expected == "" {
		return ms.ReadText(ctx, p)
	}
	return ms.loadMatching(ctx, p, expected)
}

func (ms *MountSession) loadMatching(ctx context.Context, p, expected string) (Textual, error) {
	doc, err := ms.ReadText(ctx, p)
	if err != nil {
		return nil, err
	}
	if ContentToken(doc) != expected {
		return nil, ErrStaleContent
	}
	return doc, nil
}

func (ms *MountSession) stage(ctx context.Context, doc Textual) (ApplyResult, error) {
	if err := ms.WriteDocument(ctx, doc); err != nil {
		if errors.Is(err, ErrConflict) {
			retried, rerr := ms.retryRichPersist(ctx, doc)
			if rerr != nil {
				return ApplyResult{}, rerr
			}
			doc = retried
		} else {
			return ApplyResult{}, err
		}
	}
	token := ContentToken(doc)
	ms.rememberRev(doc.Path(), token)
	out := ApplyResult{Path: doc.Path(), Rev: token, LineCount: doc.LineCount()}
	if rd, ok := AsRich(doc); ok {
		out.Outline = rd.Blocks()
	}
	return out, nil
}

func (ms *MountSession) retryRichPersist(ctx context.Context, doc Textual) (Textual, error) {
	if _, ok := AsRich(doc); !ok {
		return nil, ErrStaleContent
	}
	fresh, err := ms.ReadText(ctx, doc.Path())
	if err != nil {
		return nil, persistWriteErr(err)
	}
	d, ok := asIR(doc)
	f, fok := asIR(fresh)
	if !ok || !fok {
		return nil, persistWriteErr(ErrConflict)
	}
	d.hint = f.hint
	if err := ms.WriteDocument(ctx, d); err != nil {
		return nil, persistWriteErr(err)
	}
	return d, nil
}

func persistWriteErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch {
	case errors.Is(err, ErrInvalidWrite),
		errors.Is(err, ErrNotExist),
		errors.Is(err, ErrAuthExpired),
		errors.Is(err, ErrPermission),
		errors.Is(err, ErrNotSupported),
		errors.Is(err, ErrReadOnly),
		errors.Is(err, ErrTooLarge),
		errors.Is(err, ErrStaleContent),
		errors.Is(err, ErrUseHTML),
		errors.Is(err, ErrProjected):
		return err
	}
	return ErrInvalidWrite
}
