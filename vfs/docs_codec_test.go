package vfs_test

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

// docsZip is a zip of these entries; a name that ends in "/" is a folder.
// Method 99 is a compression that zip does not know.
func docsZip(t *testing.T, method uint16, entries ...string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	for _, name := range entries {
		h := &zip.FileHeader{Name: name, Method: method}
		create := zw.CreateHeader
		if method == 99 {
			create = zw.CreateRaw
		}
		w, err := create(h)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasSuffix(name, "/") {
			_, _ = w.Write([]byte("<p>" + name + "</p>"))
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// The Docs codec reads the blocks of Google Docs HTML: a frameset page
// with no body, scripts and styles it skips, a list item with no list,
// images outside a paragraph and in an empty or nested figure, comments
// in a paragraph, and tables that are empty or have short rows. A zip
// export gives its HTML at the root before nested HTML, past folders.
// HTML nested deeper than the parser allows and a zip entry that does not
// open do not decode.
func TestDocsCodec_decodesGoogleDocsHTML(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]struct {
		data []byte
		want []string // kind:text:object_id per block; nil: an error
	}{
		"a frameset page":       {[]byte("<html><frameset></frameset></html>"), []string{}},
		"script and style":      {[]byte("<body><script>s()</script><style>p{}</style><p>kept</p></body>"), []string{"paragraph:kept:"}},
		"a bare list item":      {[]byte("<li>one</li>"), []string{"list_item:one:"}},
		"a block image":         {[]byte(`<body><img src="https://img" alt="Pic"></body>`), []string{"image:Pic:img-html-1"}},
		"figures":               {[]byte(`<figure><img data-object-id="kix.f" alt="Fig"></figure><figure></figure>`), []string{"image:Fig:kix.f", "image::img-html-1"}},
		"inline comment":        {[]byte("<p>a<!-- note -->b<script>x</script></p>"), []string{"paragraph:ab:"}},
		"tables":                {[]byte("<table></table><table><tr><td>a</td><td>b</td></tr><tr><td>c</td></tr></table>"), []string{"table:a\tb\nc\t:"}},
		"a zip":                 {docsZip(t, zip.Deflate, "images/", "images/nested.html", "Doc.html"), []string{"paragraph:Doc.html:"}},
		"deep HTML":             {[]byte(strings.Repeat("<div>", 600)), nil},
		"an unknown zip method": {docsZip(t, 99, "index.html"), nil},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			doc, err := vfs.DocsCodec{}.Decode(t.Context(), "/x", "", c.data)
			if c.want == nil {
				if err == nil {
					t.Fatal("want an error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got := []string{}
			for _, b := range doc.(vfs.Structured).Blocks() {
				got = append(got, b.Kind+":"+b.PlainText()+":"+b.Style.Attributes["object_id"])
			}
			if strings.Join(got, "|") != strings.Join(c.want, "|") {
				t.Fatalf("blocks = %q, want %q", got, c.want)
			}
		})
	}
}
