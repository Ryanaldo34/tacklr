// Package google holds what is Google-specific in the REST calls the vfs
// adapters make: the multipart upload. Packages drive, docs and sheets hold
// the wire types: the subset of each API's JSON the adapters read and write.
package google

import (
	"context"
	"io"
	"mime/multipart"
	"net/textproto"
	"net/url"

	"github.com/ryanaldo34/tacklr/internal/rest"
)

// Upload sends meta and media as one multipart/related upload through c and
// decodes the reply into out.
func Upload(ctx context.Context, c rest.Client, method, path string, q url.Values, meta any, mediaMIME string, media io.Reader, out any) error {
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	go func() {
		pw.CloseWithError(writeParts(mw, rest.Encode(meta), mediaMIME, media))
	}()
	q.Set("uploadType", "multipart")
	err := c.Stream(ctx, method, path, q, "multipart/related; boundary="+mw.Boundary(), pr, out)
	_ = pr.Close()
	return err
}

func writeParts(mw *multipart.Writer, meta []byte, mediaMIME string, media io.Reader) error {
	part, err := mw.CreatePart(textproto.MIMEHeader{"Content-Type": {"application/json; charset=UTF-8"}})
	if err == nil {
		_, err = part.Write(meta)
	}
	if err == nil {
		part, err = mw.CreatePart(textproto.MIMEHeader{"Content-Type": {mediaMIME}})
	}
	if err == nil {
		_, err = io.Copy(part, media)
	}
	if err == nil {
		err = mw.Close()
	}
	return err
}
