package vfs

import (
	"maps"
	"strings"
)

const (
	MarkBold   = "bold"
	MarkItalic = "italic"
	MarkStrike = "strike"
	MarkHref   = "href"
)

// Run is one contiguous marked slice of a block. Marks keys are MarkBold,
// MarkItalic, MarkStrike, MarkHref. Agents never send Runs; they write
// markdown in Block.Text (**bold**, _italic_, ~~strike~~, [label](url)).
type Run struct {
	Text  string
	Marks map[string]string
}

// ParseInline turns the agent markdown subset into runs.
func ParseInline(s string) []Run {
	if s == "" {
		return nil
	}
	return mergeRuns(parseInline(s, nil))
}

// FormatInline is the canonical agent spelling. Italic is always _text_.
func FormatInline(runs []Run) string {
	if len(runs) == 0 {
		return ""
	}
	n := 0
	for _, r := range runs {
		n += len(r.Text) + 16
	}
	var b strings.Builder
	b.Grow(n)
	for _, r := range runs {
		b.WriteString(formatRun(r))
	}
	return b.String()
}

// PlainInline is Block.Text with mark delimiters removed.
func PlainInline(s string) string {
	return runsPlain(ParseInline(s))
}

func runsPlain(runs []Run) string {
	if len(runs) == 0 {
		return ""
	}
	n := 0
	for _, r := range runs {
		n += len(r.Text)
	}
	var b strings.Builder
	b.Grow(n)
	for _, r := range runs {
		b.WriteString(r.Text)
	}
	return b.String()
}

func (b Block) PlainText() string {
	if b.Kind == BlockKindImage {
		return b.Text
	}
	if len(b.Runs) > 0 {
		return runsPlain(b.Runs)
	}
	if b.Kind == BlockKindTable {
		return b.Text
	}
	return PlainInline(b.Text)
}

func (b Block) inlineRuns() []Run {
	if b.Kind == BlockKindImage || b.Kind == BlockKindTable {
		return nil
	}
	if len(b.Runs) > 0 {
		return b.Runs
	}
	return ParseInline(b.Text)
}

func normalizeInline(b *Block) {
	if b == nil || b.Kind == BlockKindImage {
		return
	}
	if b.Kind == BlockKindTable {
		grid, err := parseTSV(b.Text)
		if err != nil {
			return
		}
		for i := range grid {
			for j := range grid[i] {
				grid[i][j] = FormatInline(ParseInline(grid[i][j]))
			}
		}
		b.Text = encodeTSV(grid)
		b.Runs = nil
		return
	}
	if len(b.Runs) > 0 {
		b.Runs = mergeRuns(b.Runs)
		b.Text = FormatInline(b.Runs)
		return
	}
	b.Runs = ParseInline(b.Text)
	b.Text = FormatInline(b.Runs)
}

func parseInline(s string, base map[string]string) []Run {
	var out []Run
	var lit strings.Builder
	flush := func() {
		if lit.Len() == 0 {
			return
		}
		out = append(out, Run{Text: lit.String(), Marks: cloneMarks(base)})
		lit.Reset()
	}
	for i := 0; i < len(s); {
		if s[i] == '\\' && i+1 < len(s) {
			lit.WriteByte(s[i+1])
			i += 2
			continue
		}
		if strings.HasPrefix(s[i:], "**") {
			if end := indexClose(s, i+2, "**"); end >= 0 {
				flush()
				out = append(out, parseInline(s[i+2:end], withMark(base, MarkBold, "true"))...)
				i = end + 2
				continue
			}
		}
		if strings.HasPrefix(s[i:], "~~") {
			if end := indexClose(s, i+2, "~~"); end >= 0 {
				flush()
				out = append(out, parseInline(s[i+2:end], withMark(base, MarkStrike, "true"))...)
				i = end + 2
				continue
			}
		}
		if s[i] == '[' {
			if label, href, next, ok := parseLink(s, i); ok {
				flush()
				out = append(out, parseInline(label, withMark(base, MarkHref, href))...)
				i = next
				continue
			}
		}
		if s[i] == '_' {
			if end := indexClose(s, i+1, "_"); end >= 0 {
				flush()
				out = append(out, parseInline(s[i+1:end], withMark(base, MarkItalic, "true"))...)
				i = end + 1
				continue
			}
		}
		if s[i] == '*' && !strings.HasPrefix(s[i:], "**") {
			if end := indexClose(s, i+1, "*"); end >= 0 {
				flush()
				out = append(out, parseInline(s[i+1:end], withMark(base, MarkItalic, "true"))...)
				i = end + 1
				continue
			}
		}
		lit.WriteByte(s[i])
		i++
	}
	flush()
	return out
}

func parseLink(s string, i int) (label, href string, next int, ok bool) {
	if i >= len(s) || s[i] != '[' {
		return "", "", i, false
	}
	depth := 1
	j := i + 1
	for j < len(s) {
		if s[j] == '\\' && j+1 < len(s) {
			j += 2
			continue
		}
		if s[j] == '[' {
			depth++
		}
		if s[j] == ']' {
			depth--
			if depth == 0 {
				if j+1 >= len(s) || s[j+1] != '(' {
					return "", "", i, false
				}
				k := strings.IndexByte(s[j+2:], ')')
				if k < 0 {
					return "", "", i, false
				}
				return s[i+1 : j], s[j+2 : j+2+k], j + 3 + k, true
			}
		}
		j++
	}
	return "", "", i, false
}

func indexClose(s string, from int, delim string) int {
	for i := from; i+len(delim) <= len(s); i++ {
		if s[i] == '\\' {
			i++
			continue
		}
		if strings.HasPrefix(s[i:], delim) {
			return i
		}
	}
	return -1
}

func formatRun(r Run) string {
	inner := escapeInline(r.Text)
	if r.Marks[MarkItalic] == "true" {
		inner = "_" + inner + "_"
	}
	if r.Marks[MarkBold] == "true" {
		inner = "**" + inner + "**"
	}
	if r.Marks[MarkStrike] == "true" {
		inner = "~~" + inner + "~~"
	}
	if href := r.Marks[MarkHref]; href != "" {
		inner = "[" + inner + "](" + strings.ReplaceAll(href, ")", "\\)") + ")"
	}
	return inner
}

func escapeInline(s string) string {
	if !strings.ContainsAny(s, "\\*_~[]") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 4)
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\', '*', '_', '~', '[', ']':
			b.WriteByte('\\')
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

func mergeRuns(in []Run) []Run {
	if len(in) == 0 {
		return nil
	}
	out := make([]Run, 0, len(in))
	for _, r := range in {
		if r.Text == "" {
			continue
		}
		if n := len(out); n > 0 && marksEqual(out[n-1].Marks, r.Marks) {
			out[n-1].Text += r.Text
			continue
		}
		out = append(out, Run{Text: r.Text, Marks: cloneMarks(r.Marks)})
	}
	return out
}

func withMark(base map[string]string, k, v string) map[string]string {
	out := cloneMarks(base)
	if out == nil {
		out = map[string]string{}
	}
	out[k] = v
	return out
}

func cloneMarks(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	return maps.Clone(m)
}

func cloneRuns(in []Run) []Run {
	if len(in) == 0 {
		return nil
	}
	out := make([]Run, len(in))
	for i, r := range in {
		out[i] = Run{Text: r.Text, Marks: cloneMarks(r.Marks)}
	}
	return out
}

func marksEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
