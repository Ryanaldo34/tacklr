package vfs

import "strings"

func projectTabularHTML(sheets []Sheet) string {
	var b strings.Builder
	need := 32
	for _, sh := range sheets {
		need += len(sh.Title) + sh.Rows*sh.Cols*8
	}
	b.Grow(need)
	b.WriteString("<html><body>\n")
	for _, sh := range sheets {
		b.WriteString(`<h1 class="tacklr-tab">`)
		writeHTMLEscaped(&b, sh.Title)
		b.WriteString("</h1>\n")
		b.WriteString("<table>\n")
		for _, row := range sh.Cells {
			b.WriteString("<tr>")
			for _, c := range row {
				b.WriteString("<td>")
				writeHTMLEscaped(&b, c.Display())
				b.WriteString("</td>")
			}
			b.WriteString("</tr>\n")
		}
		b.WriteString("</table>\n")
	}
	b.WriteString("</body></html>")
	return b.String()
}
