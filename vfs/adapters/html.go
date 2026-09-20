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

// RegisterCommon registers Word and Excel. HTML stays a TextCodec type unless a
// host registers adapters.HTML itself — stealing text/html makes .html EROFS.
// Safe to call more than once on the same registry: existing DOCX/XLSX bindings
// are left in place (first registration wins).
func RegisterCommon(reg *vfs.ContentRegistry) error {
	if _, ok := reg.Lookup(DOCXMediaType); !ok {
		_ = reg.Register(vfs.BlockCodec{Types: []string{DOCXMediaType}, Normalizer: DOCX{}})
	}
	if _, ok := reg.Lookup(XLSXMediaType); !ok {
		return reg.Register(vfs.TabularCodec{Types: []string{XLSXMediaType}, Normalizer: XLSX{}})
	}
	return nil
}
