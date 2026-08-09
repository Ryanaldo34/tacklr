package vfs

import (
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

// Textual is line-addressable document content (plaintext, source, JSON-as-text, …).
//
// Line numbers are 1-based. Lines(start, end) uses a half-open range in that
// space: start inclusive, end exclusive. Lines(1, LineCount()+1) returns all lines.
type Textual interface {
	Document
	Encoding() string
	Text() string
	LineCount() int
	// Line returns line n (1-based) without the terminating \n.
	Line(n int) (string, error)
	// Lines returns lines in the half-open 1-based range [start, end).
	Lines(start, end int) ([]string, error)
}

// Structured is optional for documents that expose a block tree (Word, Google Docs, …).
// Plaintext TextDocument does not implement Structured.
type Structured interface {
	Document
	Blocks() []Block
}

// StyleMeta is optional presentation/structure for rich documents.
// Plaintext codecs leave Style empty / unused.
type StyleMeta struct {
	Kind       string // "heading" | "paragraph" | "table" | "list" | "code" | …
	Level      int    // heading level 1–9; 0 if N/A
	Span       Span
	Attributes map[string]string
}

// Span is a content address within a Document (line-based for text).
type Span struct {
	StartLine int // 1-based inclusive; 0 = unused
	EndLine   int // 1-based exclusive
}

// Block is a top-level structural unit for Structured documents.
type Block struct {
	ID    string
	Kind  string
	Text  string // plain projection when available
	Style StyleMeta
}

// TextDocument is the concrete plaintext/source IR (Document + Textual).
//
// Storage is a single UTF-8 body plus a line-start index (not a second full copy
// of the text as []string). Line views are slices into text until an edit
// rebuilds the body.
//
// Empty files have LineCount 0. A sole "\n" yields two empty lines.
type TextDocument struct {
	path, mediaType, encoding string
	text                      string
	starts                    []int // byte offsets of each line start into text
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
	d.SetText(text)
	return d
}

func (d *TextDocument) Path() string      { return d.path }
func (d *TextDocument) MediaType() string { return d.mediaType }
func (d *TextDocument) Encoding() string  { return d.encoding }
func (d *TextDocument) Text() string      { return d.text }
func (d *TextDocument) LineCount() int    { return len(d.starts) }

func (d *TextDocument) Line(n int) (string, error) {
	if n < 1 || n > len(d.starts) {
		return "", ErrLineOutOfRange
	}
	return d.lineAt(n - 1), nil
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
		out[i] = d.lineAt(start - 1 + i)
	}
	return out, nil
}

// SetText replaces the full body and rebuilds the line index.
func (d *TextDocument) SetText(text string) {
	d.text = text
	d.starts = indexLineStarts(text)
}

// SetLine replaces line n (1-based). line must not contain '\n'.
func (d *TextDocument) SetLine(n int, line string) error {
	if n < 1 || n > len(d.starts) {
		return ErrLineOutOfRange
	}
	if strings.Contains(line, "\n") {
		return ErrInvalidLine
	}
	lines := d.allLines()
	lines[n-1] = line
	d.setFromLines(lines)
	return nil
}

// ReplaceLines replaces the half-open range [start, end) with replacement.
//
//	ReplaceLines(1, 1, []string{"x"})     // insert at start (or into empty file)
//	ReplaceLines(2, 4, nil)               // delete lines 2–3
//	ReplaceLines(1, LineCount()+1, lines) // replace entire body
//
// Each replacement element must not contain '\n'.
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
	old := d.allLines()
	next := make([]string, 0, (start-1)+len(replacement)+(n-(end-1)))
	next = append(next, old[:start-1]...)
	next = append(next, replacement...)
	next = append(next, old[end-1:]...)
	d.setFromLines(next)
	return nil
}

func (d *TextDocument) setFromLines(lines []string) {
	if len(lines) == 0 {
		d.text = ""
		d.starts = nil
		return
	}
	d.text = strings.Join(lines, "\n")
	d.starts = indexLineStarts(d.text)
}

// lineAt returns the 0-based line as a slice of d.text (no copy of content).
func (d *TextDocument) lineAt(i int) string {
	start := d.starts[i]
	if i+1 < len(d.starts) {
		// Next line starts after the '\n' that ends this line.
		return d.text[start : d.starts[i+1]-1]
	}
	return d.text[start:]
}

func (d *TextDocument) allLines() []string {
	n := len(d.starts)
	if n == 0 {
		return nil
	}
	out := make([]string, n)
	for i := range out {
		out[i] = d.lineAt(i)
	}
	return out
}

// indexLineStarts matches strings.Split(text, "\n") boundaries.
// Empty text → nil (LineCount 0). Trailing '\n' yields a final empty line.
func indexLineStarts(text string) []int {
	if text == "" {
		return nil
	}
	starts := make([]int, 1, strings.Count(text, "\n")+1)
	starts[0] = 0
	for i := 0; i < len(text); i++ {
		if text[i] == '\n' {
			starts = append(starts, i+1)
		}
	}
	return starts
}

// AsTextual returns d as Textual, or ErrNotTextual.
func AsTextual(d Document) (Textual, error) {
	if t, ok := d.(Textual); ok {
		return t, nil
	}
	return nil, ErrNotTextual
}

// FormatLines joins Lines(start, end) with \n for tool-style output.
func FormatLines(t Textual, start, end int) (string, error) {
	lines, err := t.Lines(start, end)
	if err != nil {
		return "", err
	}
	return strings.Join(lines, "\n"), nil
}
