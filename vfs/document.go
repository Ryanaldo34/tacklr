package vfs

import (
	"context"
	"strings"
)

// Document is the general content IR. Codecs produce Document values that are
// independent of storage backends (local, S3, Drive, …).
//
// Path is always a virtual path — never a host filesystem path or bucket key.
type Document interface {
	Path() string
	MediaType() string
}

// Textual is document content that has a plaintext form (source, Markdown,
// Engrams, later Word/Docs/PDF extracts). Images and other binaries do not
// implement it — callers use a comma-ok assert.
//
// Text() is that plaintext (FUSE / encode). Line numbers are
// 1-based. Lines(start, end) is half-open [start, end).
// SetText / SetLine / ReplaceLines mutate this value; persist with WriteDocument.
// SetText returns ErrProjected on types whose Text() is a derived projection.
type Textual interface {
	Document
	Encoding() string
	Text() string
	LineCount() int
	Line(n int) (string, error)
	Lines(start, end int) ([]string, error)
	SetText(text string) error
	SetLine(n int, line string) error
	ReplaceLines(start, end int, replacement []string) error
}

// Structured is optional for documents with a block tree (Markdown headings,
// Word, Google Docs, …). Callers use Blocks(); empty means no structure.
type Structured interface {
	Document
	Blocks() []Block
}

// Block kind vocabulary (stable for tools and brain props). Grow carefully.
const (
	BlockKindPreamble  = "preamble"
	BlockKindHeading   = "heading"
	BlockKindParagraph = "paragraph"
	BlockKindListItem  = "list_item"
	BlockKindTable     = "table"
	BlockKindImage     = "image"
)

// StyleMeta is optional presentation/structure for rich documents.
type StyleMeta struct {
	Kind       string
	Level      int
	Span       Span
	Attributes map[string]string
}

// Span is a line-based content address (1-based half-open).
type Span struct {
	StartLine int
	EndLine   int
}

// Block is a structural unit (heading region, paragraph, …).
// For Markdown, blocks are a projected view over the textual body (not a second body).
type Block struct {
	ID    string
	Kind  string
	Text  string
	Style StyleMeta
}

// FindBlock looks up by ID or Style.Attributes["heading_path"] (exact).
func FindBlock(blocks []Block, idOrPath string) (Block, bool) {
	idOrPath = strings.TrimSpace(idOrPath)
	if idOrPath == "" {
		return Block{}, false
	}
	for _, b := range blocks {
		if b.ID == idOrPath {
			return b, true
		}
		if b.Style.Attributes != nil && b.Style.Attributes["heading_path"] == idOrPath {
			return b, true
		}
	}
	return Block{}, false
}

// BlockReplaceSpan returns half-open 1-based lines to replace for a block.
// For headings, includeHeading false skips the heading line (body only).
func BlockReplaceSpan(b Block, includeHeading bool) (start, end int, err error) {
	start, end = b.Style.Span.StartLine, b.Style.Span.EndLine
	if start < 1 || end < start {
		return 0, 0, ErrLineOutOfRange
	}
	if b.Kind == BlockKindHeading && !includeHeading {
		start++
		if start > end {
			return start, end, nil // empty body
		}
	}
	return start, end, nil
}

// TextDocument is the concrete plaintext/source IR (Document + Textual).
// One UTF-8 body + line-start index. Empty files have LineCount 0.
type TextDocument struct {
	path, mediaType, encoding string
	text                      string
	starts                    []int
	encoder                   Encoder
	richBlocks                []Block
}

// NewTextDocument builds a TextDocument from already-decoded UTF-8 text.
func NewTextDocument(path, mediaType, encoding, text string) *TextDocument {
	if encoding == "" {
		encoding = "utf-8"
	}
	if mediaType == "" {
		mediaType = "text/plain"
	}
	d := &TextDocument{path: path, mediaType: mediaType, encoding: encoding}
	_ = d.SetText(text)
	return d
}

// NewEncodedTextDocument creates a textual canonical projection whose source
// bytes are produced by encoder when the document is written.
func NewEncodedTextDocument(path, mediaType, encoding, text string, encoder Encoder) *TextDocument {
	d := NewTextDocument(path, mediaType, encoding, text)
	d.encoder = encoder
	return d
}

func EncodeDocument(ctx context.Context, doc Document) ([]byte, error) {
	if doc == nil {
		return nil, ErrNotTextual
	}
	if t, ok := doc.(*TextDocument); ok && t.encoder != nil {
		return t.encoder.Encode(ctx, t)
	}
	if c, ok := defaultContentRegistry.codec(normalizeMediaType(doc.MediaType())); ok {
		if enc, ok := c.(Encoder); ok {
			return enc.Encode(ctx, doc)
		}
	}
	text, ok := doc.(Textual)
	if !ok {
		return nil, ErrNotTextual
	}
	return EncodeTextual(text)
}

func (d *TextDocument) Path() string      { return d.path }
func (d *TextDocument) MediaType() string { return d.mediaType }
func (d *TextDocument) Encoding() string  { return d.encoding }
func (d *TextDocument) Text() string      { return d.text }
func (d *TextDocument) LineCount() int    { return len(d.starts) }

// Blocks implements Structured. Structure is projected from the body by media
// type (e.g. Markdown headings). Always recomputed from Text() so edits cannot
// leave a stale outline. Empty means no structure for this type.
func (d *TextDocument) Blocks() []Block {
	return structureFor(d)
}

func (d *TextDocument) Line(n int) (string, error) {
	if n < 1 || n > len(d.starts) {
		return "", ErrLineOutOfRange
	}
	return d.lineSlice(n - 1), nil
}

func (d *TextDocument) Lines(start, end int) ([]string, error) {
	n := len(d.starts)
	if start < 1 || end < start || end > n+1 {
		return nil, ErrLineOutOfRange
	}
	if start == end {
		return []string{}, nil
	}
	out := make([]string, end-start)
	for i := range out {
		out[i] = d.lineSlice(start - 1 + i)
	}
	return out, nil
}

// SetText replaces the full body and rebuilds the line index.
func (d *TextDocument) SetText(text string) error {
	d.text = text
	if text == "" {
		d.starts = nil
		return nil
	}
	starts := make([]int, 1, strings.Count(text, "\n")+1)
	starts[0] = 0
	for i := 0; i < len(text); i++ {
		if text[i] == '\n' {
			starts = append(starts, i+1)
		}
	}
	d.starts = starts
	return nil
}

// SetLine replaces line n (1-based). line must not contain '\n'.
func (d *TextDocument) SetLine(n int, line string) error {
	return d.ReplaceLines(n, n+1, []string{line})
}

// ReplaceLines replaces half-open [start, end) with replacement lines (no '\n' in elements).
// Splices the UTF-8 body; it does not allocate a []string of every line.
func (d *TextDocument) ReplaceLines(start, end int, replacement []string) error {
	n := len(d.starts)
	if start < 1 || end < start || end > n+1 {
		return ErrLineOutOfRange
	}
	for _, line := range replacement {
		if strings.Contains(line, "\n") {
			return ErrInvalidLine
		}
	}
	if n == 0 {
		return d.SetText(strings.Join(replacement, "\n"))
	}

	prefixLines := start - 1
	suffixLines := n - (end - 1)
	moreAfterPrefix := len(replacement) > 0 || suffixLines > 0

	need := len(d.text) + 1
	for _, line := range replacement {
		need += len(line) + 1
	}
	var b strings.Builder
	b.Grow(need)

	if prefixLines > 0 {
		if prefixLines == n {
			b.WriteString(d.text)
			if moreAfterPrefix {
				b.WriteByte('\n')
			}
		} else if moreAfterPrefix {
			b.WriteString(d.text[:d.starts[prefixLines]])
		} else {
			b.WriteString(d.text[:d.starts[prefixLines]-1])
		}
	}
	for i, line := range replacement {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(line)
	}
	if suffixLines > 0 {
		if len(replacement) > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(d.text[d.starts[end-1]:])
	}
	return d.SetText(b.String())
}

func (d *TextDocument) lineSlice(i int) string {
	start := d.starts[i]
	if i+1 < len(d.starts) {
		return d.text[start : d.starts[i+1]-1]
	}
	return d.text[start:]
}
