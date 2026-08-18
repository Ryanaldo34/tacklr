package adapters

import (
	"context"
	"fmt"
	stdhtml "html"
	"strings"

	"golang.org/x/net/html"

	"github.com/ryanaldo34/tacklr/vfs"
)

const HTMLMediaType = "text/html"

// HTML handles the subset emitted by common rich text editors. It preserves
// block structure, inline marks, links, and list items.
type HTML struct{}

func (HTML) DecodeRich(ctx context.Context, _ string, _ string, data []byte) (*vfs.RichTextDocument, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := html.Parse(strings.NewReader(string(data)))
	if err != nil {
		return nil, err
	}
	doc := &vfs.RichTextDocument{Schema: vfs.RichTextSchema}
	var walk func(*html.Node, map[string]string, int)
	index := 0
	walk = func(node *html.Node, marks map[string]string, listLevel int) {
		if node.Type != html.ElementNode {
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				walk(child, marks, listLevel)
			}
			return
		}
		switch node.Data {
		case "script", "style", "head":
			return
		case "p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "li", "blockquote":
			block := collectHTMLBlock(node, marks, listLevel)
			if block.Text != "" || len(block.Runs) > 0 {
				index++
				block.ID = fmt.Sprintf("block-%06d", index)
				doc.Blocks = append(doc.Blocks, block)
			}
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child, marks, listLevel)
		}
	}
	walk(root, nil, 0)
	return doc, nil
}

func (HTML) EncodeRich(ctx context.Context, doc *vfs.RichTextDocument) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var b strings.Builder
	b.WriteString("<!doctype html><html><head><meta charset=\"utf-8\"></head><body>")
	for _, block := range doc.Blocks {
		tag := "p"
		switch {
		case block.Kind == "heading" && block.Level > 0:
			tag = fmt.Sprintf("h%d", min(block.Level, 6))
		case block.Kind == "list-item":
			tag = "li"
		case block.Kind == "quote":
			tag = "blockquote"
		}
		b.WriteString("<" + tag + ">")
		for _, run := range block.Runs {
			value := stdhtml.EscapeString(run.Text)
			if href := run.Attributes["href"]; href != "" {
				value = `<a href="` + stdhtml.EscapeString(href) + `">` + value + "</a>"
			}
			for _, mark := range []struct{ name, tag string }{{"bold", "strong"}, {"italic", "em"}, {"underline", "u"}, {"strike", "s"}} {
				if run.Attributes[mark.name] == "true" || run.Attributes[mark.tag] == "true" {
					value = "<" + mark.tag + ">" + value + "</" + mark.tag + ">"
				}
			}
			b.WriteString(value)
		}
		b.WriteString("</" + tag + ">")
	}
	b.WriteString("</body></html>")
	return []byte(b.String()), nil
}

func collectHTMLBlock(node *html.Node, inherited map[string]string, listLevel int) vfs.RichTextBlock {
	kind, level := "paragraph", 0
	if strings.HasPrefix(node.Data, "h") && len(node.Data) == 2 {
		kind = "heading"
		level = int(node.Data[1] - '0')
	} else if node.Data == "li" {
		kind = "list-item"
	}
	block := vfs.RichTextBlock{Kind: kind, Level: level, Attributes: map[string]string{}}
	var visit func(*html.Node, map[string]string)
	visit = func(current *html.Node, marks map[string]string) {
		if current.Type == html.TextNode {
			text := strings.NewReplacer("\r", "", "\n", " ", "\t", " ").Replace(current.Data)
			if strings.TrimSpace(text) != "" {
				block.Runs = append(block.Runs, vfs.RichTextRun{Text: text, Attributes: marks})
			}
			return
		}
		next := cloneAttrs(marks)
		if current.Type == html.ElementNode {
			switch current.Data {
			case "strong", "b":
				next["bold"] = "true"
			case "em", "i":
				next["italic"] = "true"
			case "u":
				next["underline"] = "true"
			case "s", "strike":
				next["strike"] = "true"
			case "a":
				for _, attr := range current.Attr {
					if attr.Key == "href" {
						next["href"] = attr.Val
					}
				}
			}
		}
		for child := current.FirstChild; child != nil; child = child.NextSibling {
			visit(child, next)
		}
	}
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		visit(child, cloneAttrs(inherited))
	}
	return block
}

func cloneAttrs(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}
