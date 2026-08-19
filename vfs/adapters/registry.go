package adapters

import "github.com/ryanaldo34/tacklr/vfs"

// RegisterCommon registers Word. HTML stays a TextCodec type unless a host
// registers adapters.HTML itself — stealing text/html makes .html EROFS.
func RegisterCommon(reg *vfs.ContentRegistry) error {
	return reg.Register(vfs.BlockCodec{Types: []string{DOCXMediaType}, Normalizer: DOCX{}})
}
