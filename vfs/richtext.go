package vfs

import (
	"context"
	"fmt"
)

// BlockNormalizer converts a source format to and from []Block.
type BlockNormalizer interface {
	DecodeBlocks(ctx context.Context, path, mediaType string, data []byte) ([]Block, error)
	EncodeBlocks(ctx context.Context, blocks []Block) ([]byte, error)
}

// BlockCodec adapts a BlockNormalizer to the VFS codec registry.
// Decode yields a RichDocument. Encode writes native bytes (DOCX, …).
// Not an IdentityCodec — FUSE is EROFS; persist via WriteDocument.
type BlockCodec struct {
	Types      []string
	Normalizer BlockNormalizer
}

func (c BlockCodec) MediaTypes() []string { return c.Types }

func (c BlockCodec) Decode(ctx context.Context, path, mediaType string, data []byte) (Document, error) {
	if c.Normalizer == nil {
		return nil, fmt.Errorf("vfs: block normalizer required")
	}
	blocks, err := c.Normalizer.DecodeBlocks(ctx, path, mediaType, data)
	if err != nil {
		return nil, err
	}
	return NewRichDocument(path, mediaType, blocks), nil
}

func (c BlockCodec) Encode(ctx context.Context, doc Document) ([]byte, error) {
	if c.Normalizer == nil {
		return nil, fmt.Errorf("vfs: block normalizer required")
	}
	rd, ok := doc.(*RichDocument)
	if !ok {
		return nil, ErrNotTextual
	}
	return c.Normalizer.EncodeBlocks(ctx, rd.Blocks())
}
