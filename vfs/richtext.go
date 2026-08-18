package vfs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// RichTextSchema is the version of Tacklr's interchange representation.
const RichTextSchema = "https://tacklr.dev/schemas/richtext/v1"

// RichTextDocument is the stable, storage-independent representation used by
// format adapters. Unknown attributes are intentionally retained as strings so
// adapters can preserve provider-specific metadata without changing the core.
type RichTextDocument struct {
	Schema   string            `json:"schema"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Blocks   []RichTextBlock   `json:"blocks"`
}

type RichTextBlock struct {
	ID         string            `json:"id"`
	Kind       string            `json:"kind"`
	Level      int               `json:"level,omitempty"`
	Text       string            `json:"text,omitempty"`
	Runs       []RichTextRun     `json:"runs,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
	Children   []RichTextBlock   `json:"children,omitempty"`
}

type RichTextRun struct {
	Text       string            `json:"text"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// RichTextNormalizer converts a source format to and from the canonical schema.
// A normalizer owns all format-specific behavior, for example DOCX XML or an
// external Google Docs representation.
type RichTextNormalizer interface {
	DecodeRich(ctx context.Context, path, mediaType string, data []byte) (*RichTextDocument, error)
	EncodeRich(ctx context.Context, doc *RichTextDocument) ([]byte, error)
}

// RichTextCodec adapts a RichTextNormalizer to the VFS codec registry.
// Decode yields a RichDocument (blocks are the source of truth; Text() is the
// HTML projection). Encode writes native bytes (DOCX, editor HTML, …).
// Not an IdentityCodec — FUSE is EROFS; persist via WriteDocument.
type RichTextCodec struct {
	Types      []string
	Normalizer RichTextNormalizer
}

func (c RichTextCodec) MediaTypes() []string { return c.Types }

func (c RichTextCodec) Decode(ctx context.Context, path, mediaType string, data []byte) (Document, error) {
	if c.Normalizer == nil {
		return nil, fmt.Errorf("vfs: rich text normalizer required")
	}
	doc, err := c.Normalizer.DecodeRich(ctx, path, mediaType, data)
	if err != nil {
		return nil, err
	}
	if err := validateRichText(doc); err != nil {
		return nil, err
	}
	return NewRichDocument(path, mediaType, projectRichBlocks(doc.Blocks, 1)), nil
}

func (c RichTextCodec) Encode(ctx context.Context, doc Document) ([]byte, error) {
	if c.Normalizer == nil {
		return nil, fmt.Errorf("vfs: rich text normalizer required")
	}
	rich, err := richTextFromDocument(doc)
	if err != nil {
		return nil, err
	}
	if err := validateRichText(rich); err != nil {
		return nil, err
	}
	return c.Normalizer.EncodeRich(ctx, rich)
}

func projectRichBlocks(blocks []RichTextBlock, line int) []Block {
	if len(blocks) == 0 {
		return nil
	}
	out := make([]Block, 0, len(blocks))
	for _, block := range blocks {
		text := block.Text
		if text == "" && len(block.Runs) > 0 {
			var b strings.Builder
			for _, run := range block.Runs {
				b.WriteString(run.Text)
			}
			text = b.String()
		}
		kind := normalizeRichKind(block.Kind)
		end := line + maxRichTextLines(text)
		attrs := map[string]string{"heading_path": block.ID}
		for k, v := range block.Attributes {
			attrs[k] = v
		}
		out = append(out, Block{ID: block.ID, Kind: kind, Text: text, Style: StyleMeta{
			Kind: kind, Level: block.Level, Span: Span{StartLine: line, EndLine: end}, Attributes: attrs,
		}})
		line = end
		out = append(out, projectRichBlocks(block.Children, line)...)
	}
	return out
}

func normalizeRichKind(kind string) string {
	switch kind {
	case "list-item":
		return BlockKindListItem
	default:
		return kind
	}
}

func adapterRichKind(kind string) string {
	if kind == BlockKindListItem {
		return "list-item"
	}
	return kind
}

func richTextFromDocument(doc Document) (*RichTextDocument, error) {
	switch d := doc.(type) {
	case *RichDocument:
		return richTextFromBlocks(d.Blocks()), nil
	case *TextDocument:
		var rich RichTextDocument
		if err := json.Unmarshal([]byte(d.Text()), &rich); err != nil {
			return nil, fmt.Errorf("vfs: invalid rich text JSON: %w", err)
		}
		return &rich, nil
	default:
		return nil, ErrNotTextual
	}
}

func richTextFromBlocks(blocks []Block) *RichTextDocument {
	out := &RichTextDocument{Schema: RichTextSchema, Blocks: make([]RichTextBlock, 0, len(blocks))}
	for _, b := range blocks {
		attrs := map[string]string{}
		for k, v := range b.Style.Attributes {
			if k == "heading_path" || k == "tab_id" {
				continue
			}
			attrs[k] = v
		}
		rb := RichTextBlock{
			ID:         b.ID,
			Kind:       adapterRichKind(b.Kind),
			Level:      b.Style.Level,
			Text:       b.Text,
			Attributes: attrs,
		}
		if t := strings.ReplaceAll(b.Text, "\n", " "); t != "" {
			rb.Runs = []RichTextRun{{Text: t}}
		}
		out.Blocks = append(out.Blocks, rb)
	}
	return out
}

func maxRichTextLines(text string) int {
	return max(1, strings.Count(text, "\n")+1)
}

func validateRichText(doc *RichTextDocument) error {
	if doc == nil {
		return fmt.Errorf("vfs: rich text document required")
	}
	if doc.Schema != "" && doc.Schema != RichTextSchema {
		return fmt.Errorf("vfs: unsupported rich text schema %q", doc.Schema)
	}
	seen := make(map[string]struct{})
	var visit func([]RichTextBlock) error
	visit = func(blocks []RichTextBlock) error {
		for _, block := range blocks {
			if strings.TrimSpace(block.ID) == "" || strings.TrimSpace(block.Kind) == "" {
				return fmt.Errorf("vfs: rich text block requires id and kind")
			}
			if _, ok := seen[block.ID]; ok {
				return fmt.Errorf("vfs: duplicate rich text block id %q", block.ID)
			}
			seen[block.ID] = struct{}{}
			for _, run := range block.Runs {
				if strings.ContainsRune(run.Text, '\n') {
					return fmt.Errorf("vfs: rich text run %q contains a newline", block.ID)
				}
			}
			if err := visit(block.Children); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(doc.Blocks)
}
