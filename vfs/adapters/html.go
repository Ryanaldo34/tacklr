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

func (HTML) DecodeBlocks(ctx context.Context, _ string, _ string, data []byte) ([]vfs.Block, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, err := html.Parse(strings.NewReader(string(data)))
	if err != nil {
		return nil, err
	}
	var blocks []vfs.Block
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
				blocks = append(blocks, block)
			}
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child, marks, listLevel)
		}
	}
	walk(root, nil, 0)
	return blocks, nil
}

func (HTML) EncodeBlocks(ctx context.Context, blocks []vfs.Block) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var b strings.Builder
	b.WriteString("<!doctype html><html><head><meta charset=\"utf-8\"></head><body>")
	for _, block := range blocks {
		tag := "p"
		switch {
		case block.Kind == vfs.BlockKindHeading && block.Style.Level > 0:
			tag = fmt.Sprintf("h%d", min(block.Style.Level, 6))
		case block.Kind == vfs.BlockKindListItem || block.Kind == "list-item":
			tag = "li"
		case block.Kind == "quote":
			tag = "blockquote"
		}
		b.WriteString("<" + tag + ">")
		runs := block.Runs
		if len(runs) == 0 {
			runs = vfs.ParseInline(block.Text)
		}
		for _, run := range runs {
			value := stdhtml.EscapeString(run.Text)
			if href := run.Marks[vfs.MarkHref]; href != "" {
				value = `<a href="` + stdhtml.EscapeString(href) + `">` + value + "</a>"
			}
			if run.Marks[vfs.MarkBold] == "true" {
				value = "<strong>" + value + "</strong>"
			}
			if run.Marks[vfs.MarkItalic] == "true" {
				value = "<em>" + value + "</em>"
			}
			if run.Marks[vfs.MarkStrike] == "true" {
				value = "<s>" + value + "</s>"
			}
			b.WriteString(value)
		}
		b.WriteString("</" + tag + ">")
	}
	b.WriteString("</body></html>")
	return []byte(b.String()), nil
}

func collectHTMLBlock(node *html.Node, inherited map[string]string, listLevel int) vfs.Block {
	kind, level := vfs.BlockKindParagraph, 0
	if strings.HasPrefix(node.Data, "h") && len(node.Data) == 2 {
		kind = vfs.BlockKindHeading
		level = int(node.Data[1] - '0')
	} else if node.Data == "li" {
		kind = vfs.BlockKindListItem
	}
	block := vfs.Block{Kind: kind, Style: vfs.StyleMeta{Level: level}}
	var visit func(*html.Node, map[string]string)
	visit = func(current *html.Node, marks map[string]string) {
		if current.Type == html.TextNode {
			text := strings.NewReplacer("\r", "", "\n", " ", "\t", " ").Replace(current.Data)
			if strings.TrimSpace(text) != "" {
				block.Runs = append(block.Runs, vfs.Run{Text: text, Marks: marks})
			}
			return
		}
		next := cloneAttrs(marks)
		if current.Type == html.ElementNode {
			switch current.Data {
			case "strong", "b":
				next[vfs.MarkBold] = "true"
			case "em", "i":
				next[vfs.MarkItalic] = "true"
			case "s", "strike":
				next[vfs.MarkStrike] = "true"
			case "a":
				for _, attr := range current.Attr {
					if attr.Key == "href" {
						next[vfs.MarkHref] = attr.Val
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
	block.Text = vfs.FormatInline(block.Runs)
	return block
}

func cloneAttrs(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}
