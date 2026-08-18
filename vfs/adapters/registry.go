package adapters

import "github.com/ryanaldo34/tacklr/vfs"

// RegisterCommon registers the source formats supported by this package.
// Hosts may register individual codecs when they need stricter policy.
func RegisterCommon(reg *vfs.ContentRegistry) error {
	if err := reg.Register(vfs.RichTextCodec{Types: []string{DOCXMediaType}, Normalizer: DOCX{}}); err != nil {
		return err
	}
	return reg.Register(vfs.RichTextCodec{Types: []string{HTMLMediaType}, Normalizer: HTML{}})
}
