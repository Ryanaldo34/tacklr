package adapters

import "github.com/ryanaldo34/tacklr/vfs"

// RegisterCommon registers Word and Excel. HTML stays a TextCodec type unless a
// host registers adapters.HTML itself — stealing text/html makes .html EROFS.
func RegisterCommon(reg *vfs.ContentRegistry) error {
	if err := reg.Register(vfs.BlockCodec{Types: []string{DOCXMediaType}, Normalizer: DOCX{}}); err != nil {
		return err
	}
	return reg.Register(XLSX{})
}
