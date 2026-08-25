package adapters

import (
	"context"

	"github.com/ryanaldo34/tacklr/vfs"
)

const HTMLMediaType = "text/html"

// HTML encodes and decodes the pretty HTML dialect used for Docs/Word agent
// buffers. It is not registered as the text/html file codec (FUSE would steal
// .html). One heading, paragraph, list item, or table per line.
type HTML struct{}

func (HTML) DecodeBlocks(ctx context.Context, _ string, _ string, data []byte) ([]vfs.Block, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return vfs.DecodeHTMLBlocks(data)
}

func (HTML) EncodeBlocks(ctx context.Context, blocks []vfs.Block) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []byte(vfs.EncodeHTMLBlocks(blocks)), nil
}
