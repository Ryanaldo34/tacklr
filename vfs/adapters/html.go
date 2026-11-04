package adapters

import (
	"context"

	"github.com/ryanaldo34/tacklr/vfs"
)

const htmlMediaType = "text/html"

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

// Codecs are the Word and Excel codecs, for OpenVFS.WithCodecs. HTML is not
// among them: text/html stays a text type, so .html files keep raw-byte
// writes; a host that wants HTML blocks adds vfs.BlockCodec with HTML itself.
func Codecs() []vfs.Codec {
	return []vfs.Codec{
		vfs.BlockCodec{Types: []string{DOCXMediaType}, Normalizer: DOCX{}},
		vfs.TabularCodec{Types: []string{XLSXMediaType}, Normalizer: XLSX{}},
	}
}
