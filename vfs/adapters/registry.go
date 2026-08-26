package adapters

import (
	"github.com/ryanaldo34/tacklr/vfs"
)

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
