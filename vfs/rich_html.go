package vfs

import (
	"strconv"
	"strings"
)

func projectHTMLSpans(blocks []Block, tabs []DocTab) (string, []Span) {
	titleOf := map[string]string{}
	for _, t := range tabs {
		titleOf[t.ID] = t.Title
	}
	useSections := false
	if len(tabs) > 1 {
		useSections = true
	} else {
		seen := map[string]struct{}{}
		for _, b := range blocks {
			id := blockAttr(b, "tab_id")
			if id != "" {
				seen[id] = struct{}{}
			}
		}
		useSections = len(seen) > 1
	}

	var b strings.Builder
	need := 32
	for _, bl := range blocks {
		need += len(bl.Text) + 32
	}
	b.Grow(need)
	spans := make([]Span, len(blocks))
	line := 1
	write := func(s string) {
		b.WriteString(s)
		if n := strings.Count(s, "\n"); n > 0 {
			line += n
		}
	}
	writeEsc := func(s string) {
		writeHTMLEscaped(&b, s)
		if n := strings.Count(s, "\n"); n > 0 {
			line += n
		}
	}
	write("<html><body>\n")

	curTab := "\x00"
	openLists := make([]string, 0, 4)
	var curListID string

	closeLists := func(to int) {
		for len(openLists) > to {
			write("</")
			write(openLists[len(openLists)-1])
			write(">")
			openLists = openLists[:len(openLists)-1]
		}
		if len(openLists) == 0 {
			curListID = ""
		}
	}

	openTab := func(tabID string) {
		if !useSections {
			return
		}
		if curTab != "\x00" {
			write("</section>\n")
		}
		title := titleOf[tabID]
		if title == "" {
			title = tabID
		}
		write(`<section data-tab-id="`)
		writeEsc(tabID)
		write("\">\n")
		write(`<h1 class="tacklr-tab">`)
		writeEsc(title)
		write("</h1>\n")
		curTab = tabID
	}

	for i, bl := range blocks {
		tabID := blockAttr(bl, "tab_id")
		if useSections && (i == 0 || tabID != curTab) {
			closeLists(0)
			if len(openLists) == 0 {
				write("\n")
			}
			// closeLists writes tags without newline; keep section on its own lines
			openTab(tabID)
		}
		if bl.Kind != BlockKindListItem {
			if len(openLists) > 0 {
				closeLists(0)
				write("\n")
			}
		}
		start := line
		switch bl.Kind {
		case BlockKindHeading:
			level := bl.Style.Level
			if level < 1 {
				level = 1
			}
			if level > 6 {
				level = 6
			}
			tag := "h" + strconv.Itoa(level)
			write("<")
			write(tag)
			write(">")
			writeInlineHTML(write, writeEsc, bl)
			write("</")
			write(tag)
			write(">\n")
		case BlockKindParagraph:
			write("<p>")
			writeInlineHTML(write, writeEsc, bl)
			write("</p>\n")
		case BlockKindListItem:
			listType := blockAttr(bl, "list_type")
			if listType != "ol" {
				listType = "ul"
			}
			listID := blockAttr(bl, "list_id")
			level := bl.Style.Level
			if level < 1 {
				level = 1
			}
			if listID != curListID && curListID != "" {
				closeLists(0)
				write("\n")
				start = line
			}
			curListID = listID
			for len(openLists) < level {
				write("<")
				write(listType)
				write(">")
				openLists = append(openLists, listType)
			}
			if len(openLists) > level {
				closeLists(level)
			}
			write("<li>")
			writeInlineHTML(write, writeEsc, bl)
			nextNests := false
			if i+1 < len(blocks) && blocks[i+1].Kind == BlockKindListItem {
				nextID := blockAttr(blocks[i+1], "list_id")
				if nextID == listID && blocks[i+1].Style.Level > level {
					nextNests = true
				}
			}
			if !nextNests {
				write("</li>")
			}
			if i+1 >= len(blocks) || blocks[i+1].Kind != BlockKindListItem || blockAttr(blocks[i+1], "list_id") != listID {
				closeLists(0)
				write("\n")
			}
		case BlockKindTable:
			grid, err := parseTSV(bl.Text)
			if err != nil {
				grid = nil
			}
			write("<table>")
			for _, row := range grid {
				write("<tr>")
				for _, cell := range row {
					write("<td>")
					writeInlineHTML(write, writeEsc, Block{Text: cell, Runs: ParseInline(cell)})
					write("</td>")
				}
				write("</tr>")
			}
			write("</table>\n")
		case BlockKindImage:
			oid := blockAttr(bl, "object_id")
			src := blockAttr(bl, "content_uri")
			write(`<figure data-object-id="`)
			writeEsc(oid)
			write(`"`)
			if src != "" {
				write(`><img alt="`)
				writeEsc(bl.Text)
				write(`" src="`)
				writeEsc(src)
				write(`"></figure>` + "\n")
			} else {
				write("></figure>\n")
			}
		default:
			if bl.Text != "" {
				write("<p>")
				writeInlineHTML(write, writeEsc, bl)
				write("</p>\n")
			}
		}
		end := line
		if end < start {
			end = start
		}
		spans[i] = Span{StartLine: start, EndLine: end}
	}
	if len(openLists) > 0 {
		closeLists(0)
		write("\n")
	}
	if useSections && curTab != "\x00" {
		write("</section>\n")
	}
	write("</body></html>")
	return b.String(), spans
}

// writeHTMLEscaped writes s with the same replacements as html.EscapeString.
func writeHTMLEscaped(b *strings.Builder, s string) {
	last := 0
	for i := 0; i < len(s); i++ {
		var esc string
		switch s[i] {
		case '&':
			esc = "&amp;"
		case '\'':
			esc = "&#39;"
		case '<':
			esc = "&lt;"
		case '>':
			esc = "&gt;"
		case '"':
			esc = "&#34;"
		default:
			continue
		}
		b.WriteString(s[last:i])
		b.WriteString(esc)
		last = i + 1
	}
	b.WriteString(s[last:])
}

func writeInlineHTML(write, writeEsc func(string), bl Block) {
	runs := bl.inlineRuns()
	if len(runs) == 0 {
		writeEsc(bl.Text)
		return
	}
	for _, r := range runs {
		if href := r.Marks[MarkHref]; href != "" {
			write(`<a href="`)
			writeEsc(href)
			write(`">`)
		}
		if r.Marks[MarkStrike] == "true" {
			write("<s>")
		}
		if r.Marks[MarkBold] == "true" {
			write("<strong>")
		}
		if r.Marks[MarkItalic] == "true" {
			write("<em>")
		}
		writeEsc(r.Text)
		if r.Marks[MarkItalic] == "true" {
			write("</em>")
		}
		if r.Marks[MarkBold] == "true" {
			write("</strong>")
		}
		if r.Marks[MarkStrike] == "true" {
			write("</s>")
		}
		if r.Marks[MarkHref] != "" {
			write("</a>")
		}
	}
}
