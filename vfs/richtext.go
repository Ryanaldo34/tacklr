package vfs

import (
	"context"
	"fmt"
)

// BlockNormalizer converts a source format to []Block.
type BlockNormalizer interface {
	DecodeBlocks(ctx context.Context, path, mediaType string, data []byte) ([]Block, error)
}

// BlockCodec adapts a BlockNormalizer to the VFS codec registry.
// Decode yields a RichDocument. Not an IdentityCodec: the shell (vfs.Files)
// reads its documents as text and refuses writes.
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
