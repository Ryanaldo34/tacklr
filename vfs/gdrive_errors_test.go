package vfs_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"runtime/debug"
	"slices"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/internal/testdrive"
	"github.com/ryanaldo34/tacklr/internal/testhttp"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// driveOpenAt opens the Drive provider of fx at folderID ("" is My
// Drive), as a host's Open func does.
func driveOpenAt(t *testing.T, fx *testdrive.FX, folderID string, writable bool) vfs.Provider {
	t.Helper()
	b := mount.Binding{Writable: writable, Auth: mount.Credential{Token: "t"}, Params: map[string]string{}}
	if folderID != "" {
		b.Params[mount.ParamFolderID] = folderID
	}
	p, err := testdrive.Open(t, fx, nil)(t.Context(), "s", b)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// driveShortcuts is testdrive.Tree with My Drive, a shortcut to a file
// that is gone and a shortcut to itself.
func driveShortcuts() *testdrive.FX {
	fx := testdrive.Tree()
	fx.Add("", vfs.DriveMeta{ID: "root", Name: "My Drive", MimeType: "application/vnd.google-apps.folder"}, nil)
	fx.Add("root-a", vfs.DriveMeta{ID: "dangle", Name: "dangle", MimeType: "application/vnd.google-apps.shortcut", TargetID: "gone"}, nil)
	fx.Add("root-a", vfs.DriveMeta{ID: "loop", Name: "loop", MimeType: "application/vnd.google-apps.shortcut", TargetID: "loop"}, nil)
	return fx
}

// A host that calls a Drive provider directly gets the result of each
// call: a canceled context, a path that leaves the mount, a missing file,
// a shortcut that leads nowhere, a write to a read-only provider, a file
// where a folder must be and a token that Drive refuses.
func TestDrive_providerCallsReturnTheirResult(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	fx := driveShortcuts()
	rw := driveOpenAt(t, fx, "root-a", true)
	ro := driveOpenAt(t, fx, "root-a", false)
	missing := driveOpenAt(t, fx, "missing", true)
	dangling := driveOpenAt(t, fx, "dangle", true)

	emptyToken := vfs.NewTokenHolder(mount.Credential{})
	srv := testhttp.New(t, fx)
	noHolder, err := vfs.Drive(vfs.NewGoogleDrive(emptyToken, srv.URL+"/", srv.Client()), nil)(ctx, "s", mount.Binding{
		Params: map[string]string{mount.ParamFolderID: "root-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	bindingToken, err := vfs.Drive(vfs.NewGoogleDrive(vfs.NewTokenHolder(mount.Credential{Token: "t"}), srv.URL+"/", srv.Client()), nil)(ctx, "s", mount.Binding{
		Auth: mount.Credential{Token: "t"}, Params: map[string]string{mount.ParamFolderID: "root-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	// A host DriveAPI that embeds the Google client has no Find, so a path
	// resolves by listing whole folders.
	noFind, err := vfs.Drive(struct{ vfs.DriveAPI }{vfs.NewGoogleDrive(vfs.NewTokenHolder(mount.Credential{Token: "t"}), srv.URL+"/", srv.Client())}, nil)(ctx, "s", mount.Binding{
		Auth: mount.Credential{Token: "t"}, Params: map[string]string{mount.ParamFolderID: "root-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	expiredHolder := vfs.NewTokenHolder(mount.Credential{Token: "t"})
	expired, err := testdrive.Open(t, fx, expiredHolder)(ctx, "s", mount.Binding{Params: map[string]string{mount.ParamFolderID: "root-a"}})
	if err != nil {
		t.Fatal(err)
	}
	expiredHolder.Set(mount.Credential{})
	_, openErr := testdrive.Open(t, fx, nil)(canceled, "s", mount.Binding{})
	stat := func(p vfs.Provider, ctx context.Context, name string) error {
		_, err := p.Stat(ctx, name)
		return err
	}
	open := func(p vfs.Provider, ctx context.Context, name string, flag int) error {
		_, err := p.OpenFile(ctx, name, flag, 0)
		return err
	}
	readDir := func(p vfs.Provider, ctx context.Context, name string) error {
		_, err := p.ReadDir(ctx, name)
		return err
	}

	for name, c := range map[string]struct {
		err, want error
	}{
		"build with a canceled context":        {openErr, context.Canceled},
		"validate My Drive":                    {driveOpenAt(t, fx, "", true).Validate(ctx), nil},
		"validate with a canceled context":     {rw.Validate(canceled), context.Canceled},
		"validate a missing folder":            {missing.Validate(ctx), vfs.ErrNotExist},
		"validate a shortcut to a gone file":   {dangling.Validate(ctx), vfs.ErrNotExist},
		"stat a shortcut to itself":            {stat(rw, ctx, "loop"), vfs.ErrNotExist},
		"stat a path out of the mount":         {stat(rw, ctx, "../x"), vfs.ErrInvalidPath},
		"stat below a gone root":               {stat(dangling, ctx, "x"), vfs.ErrNotExist},
		"stat below a file":                    {stat(rw, ctx, "nda.pdf/x"), vfs.ErrNotExist},
		"stat with a canceled context":         {stat(rw, canceled, "nda.pdf"), context.Canceled},
		"stat with the binding token":          {stat(bindingToken, ctx, "nda.pdf"), nil},
		"stat with a token Drive refuses":      {stat(noHolder, ctx, "nda.pdf"), vfs.ErrAuthExpired},
		"stat with an expired holder":          {stat(expired, ctx, "nda.pdf"), vfs.ErrAuthExpired},
		"stat through a DriveAPI with no Find": {stat(noFind, ctx, "nda.pdf"), nil},
		"open with a canceled context":         {open(rw, canceled, "nda.pdf", os.O_RDONLY), context.Canceled},
		"open read-only for write":             {open(ro, ctx, "nda.pdf", os.O_WRONLY), vfs.ErrReadOnly},
		"open writable for write":              {open(rw, ctx, "nda.pdf", os.O_RDWR), vfs.ErrNotSupported},
		"read dir with a canceled context":     {readDir(rw, canceled, ""), context.Canceled},
		"read a missing dir":                   {readDir(rw, ctx, "missing"), vfs.ErrNotExist},
		"remove with a canceled context":       {rw.Remove(canceled, "nda.pdf"), context.Canceled},
		"remove read-only a missing file":      {ro.Remove(ctx, "missing"), vfs.ErrNotExist},
		"remove read-only a file":              {ro.Remove(ctx, "nda.pdf"), vfs.ErrReadOnly},
		"remove a path out of the mount":       {rw.Remove(ctx, "../x"), vfs.ErrInvalidPath},
		"remove the root":                      {rw.Remove(ctx, ""), vfs.ErrInvalidPath},
		"remove below a missing root":          {missing.Remove(ctx, "x"), vfs.ErrNotExist},
		"remove below a gone root":             {dangling.Remove(ctx, "x"), vfs.ErrNotExist},
		"remove a nested file":                 {rw.Remove(ctx, "acme/note.md"), nil},
		"remove below a missing folder":        {rw.Remove(ctx, "missing/x"), vfs.ErrNotExist},
		"remove below a file":                  {rw.Remove(ctx, "nda.pdf/x"), vfs.ErrNotExist},
		"mkdir with a canceled context":        {rw.MkdirAll(canceled, "x", 0), context.Canceled},
		"mkdir a path out of the mount":        {rw.MkdirAll(ctx, "../x", 0), vfs.ErrInvalidPath},
		"mkdir the root":                       {rw.MkdirAll(ctx, "", 0), nil},
		"mkdir read-only":                      {ro.MkdirAll(ctx, "x", 0), vfs.ErrReadOnly},
		"mkdir below a file":                   {rw.MkdirAll(ctx, "nda.pdf/x", 0), vfs.ErrNotSupported},
		"mkdir below a missing root":           {missing.MkdirAll(ctx, "x", 0), vfs.ErrNotExist},
		"mkdir below a gone root":              {dangling.MkdirAll(ctx, "x", 0), vfs.ErrNotExist},
		"write with a canceled context":        {vfs.Route{Provider: rw, Rel: "x.txt"}.WriteFile(canceled, nil), context.Canceled},
		"write read-only":                      {vfs.Route{Provider: ro, Rel: "x.txt"}.WriteFile(ctx, nil), vfs.ErrReadOnly},
		"write a path out of the mount":        {vfs.Route{Provider: rw, Rel: "../x"}.WriteFile(ctx, nil), vfs.ErrInvalidPath},
		"write the root":                       {vfs.Route{Provider: rw, Rel: ""}.WriteFile(ctx, nil), vfs.ErrInvalidPath},
		"write below a file":                   {vfs.Route{Provider: rw, Rel: "nda.pdf/x.txt"}.WriteFile(ctx, nil), vfs.ErrNotSupported},
		"write with Drive answering an error":  {driveFailedWrite(t), vfs.ErrPermission},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if !errors.Is(c.err, c.want) {
				t.Fatalf("err = %v, want %v", c.err, c.want)
			}
		})
	}
}

// driveFailedWrite writes a new file through a Drive server that refuses
// the upload.
func driveFailedWrite(t *testing.T) error {
	fx := testdrive.Tree()
	fx.Fail(testdrive.Fault{Call: "files.create", Status: http.StatusForbidden})
	return vfs.Route{Provider: driveOpenAt(t, fx, "root-a", true), Rel: "new.txt"}.WriteFile(t.Context(), []byte("x"))
}

// driveMount mounts fx at /workspace/contracts on folder root-a, after it
// plans faults.
func driveMount(t *testing.T, fx *testdrive.FX, writable bool, faults ...testdrive.Fault) *vfs.MountSession {
	t.Helper()
	ms := mountDrive(t, fx, writable)
	for _, f := range faults {
		fx.Fail(f)
	}
	return ms
}

// driveHostText is a host's own Textual document: plain text that is not
// a vfs checkout.
type driveHostText struct{ path, text string }

func (d driveHostText) Path() string                     { return d.path }
func (driveHostText) MediaType() string                  { return "text/plain" }
func (driveHostText) Encoding() string                   { return "utf-8" }
func (d driveHostText) Text() string                     { return d.text }
func (driveHostText) LineCount() int                     { return 1 }
func (d driveHostText) Line(int) (string, error)         { return d.text, nil }
func (d driveHostText) Lines(int, int) ([]string, error) { return []string{d.text}, nil }
func (driveHostText) SetText(string) error               { return vfs.ErrProjected }
func (driveHostText) SetLine(int, string) error          { return vfs.ErrProjected }
func (driveHostText) ReplaceLines(int, int, []string) error {
	return vfs.ErrProjected
}

// A Drive call that fails at the server ends the read or write that made
// it with the error of the call. A reply that the connection cuts short
// ends it with an unexpected EOF.
func TestDrive_aFailedDriveCallEndsTheOperation(t *testing.T) {
	t.Parallel()
	refused := func(call, id string) testdrive.Fault {
		return testdrive.Fault{Call: call, ID: id, Status: http.StatusForbidden}
	}
	cut := func(call, id string) testdrive.Fault { return testdrive.Fault{Call: call, ID: id, Cut: true} }
	readText := func(ms *vfs.MountSession, name string) error {
		_, err := ms.Route(t.Context(), "/workspace/contracts/"+name).ReadText(t.Context())
		return err
	}
	for name, c := range map[string]struct {
		writable bool
		fault    testdrive.Fault
		op       func(*vfs.MountSession) error
		want     error
	}{
		"list a folder": {false, refused("files.list", "root-a"), func(ms *vfs.MountSession) error {
			_, err := ms.Route(t.Context(), "/workspace/contracts").ReadDir(t.Context())
			return err
		}, vfs.ErrPermission},
		"find a name": {false, refused("files.list", "root-a"), func(ms *vfs.MountSession) error {
			_, err := ms.Route(t.Context(), "/workspace/contracts/nda.pdf").Stat(t.Context())
			return err
		}, vfs.ErrPermission},
		"read file bytes":     {false, refused("files.media", "note"), func(ms *vfs.MountSession) error { return readText(ms, "acme/note.md") }, vfs.ErrPermission},
		"read cut file bytes": {false, cut("files.media", "note"), func(ms *vfs.MountSession) error { return readText(ms, "acme/note.md") }, io.ErrUnexpectedEOF},
		"export a Doc":        {false, refused("files.export", "doc1"), func(ms *vfs.MountSession) error { return readText(ms, "Spec") }, vfs.ErrPermission},
		"export a cut Doc":    {false, cut("files.export", "doc1"), func(ms *vfs.MountSession) error { return readText(ms, "Spec") }, io.ErrUnexpectedEOF},
		"export a Sheet":      {false, refused("files.export", "sheet1"), func(ms *vfs.MountSession) error { return readText(ms, "Budget") }, vfs.ErrPermission},
		"get a Doc":           {true, refused("documents.get", "doc1"), func(ms *vfs.MountSession) error { return readText(ms, "Spec") }, vfs.ErrPermission},
		"trash a file": {true, refused("files.update", "nda"), func(ms *vfs.MountSession) error {
			return ms.Route(t.Context(), "/workspace/contracts/nda.pdf").Remove(t.Context())
		}, vfs.ErrPermission},
		"upload over a file": {true, refused("files.update", "nda"), func(ms *vfs.MountSession) error {
			return ms.Route(t.Context(), "/workspace/contracts/nda.pdf").WriteFile(t.Context(), []byte("x"))
		}, vfs.ErrPermission},
		"resolve an upload": {true, refused("files.get", "root-a"), func(ms *vfs.MountSession) error {
			return ms.Route(t.Context(), "/workspace/contracts/nda.pdf").WriteFile(t.Context(), []byte("x"))
		}, vfs.ErrPermission},
		"make a folder": {true, refused("files.create", ""), func(ms *vfs.MountSession) error {
			return ms.Route(t.Context(), "/workspace/contracts/new").MkdirAll(t.Context())
		}, vfs.ErrPermission},
		"find in a nested folder": {true, refused("files.list", "sub"), func(ms *vfs.MountSession) error {
			return ms.Route(t.Context(), "/workspace/contracts/acme/x").MkdirAll(t.Context())
		}, vfs.ErrPermission},
		"resolve a document": {true, refused("files.get", "root-a"), func(ms *vfs.MountSession) error {
			return ms.WriteDocument(t.Context(), vfs.NewTextDocument("/workspace/contracts/x.txt", "text/plain", "utf-8", "x"))
		}, vfs.ErrPermission},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ms := driveMount(t, testdrive.Tree(), c.writable, c.fault)
			if err := c.op(ms); !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

// A Drive mount that the host authenticates, with a read-only binding,
// refuses every write. A canceled write is not written. A file over the
// read cap and a Google file with no codec do not read.
func TestDrive_writesAndReadsThatTheProviderRefuses(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	fx := testdrive.Tree()
	hostRO, err := vfs.Tree(vfs.At("contracts", testdrive.Open(t, fx, nil)))(ctx, "s", vfs.Request{Bindings: []mount.Binding{{
		Params: map[string]string{mount.ParamName: "contracts", mount.ParamFolderID: "root-a"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	rw := mountDrive(t, fx, true)
	route := func(ms *vfs.MountSession, name string) vfs.Route { return ms.Route(ctx, "/workspace/contracts/"+name) }
	readText := func(name string) error {
		_, err := route(rw, name).ReadText(ctx)
		return err
	}
	text := vfs.NewTextDocument("/workspace/contracts/x.txt", "text/plain", "utf-8", "x")

	for name, c := range map[string]struct {
		err, want error
	}{
		"write file":        {route(hostRO, "x.txt").WriteFile(ctx, []byte("x")), vfs.ErrReadOnly},
		"write document":    {hostRO.WriteDocument(ctx, text), vfs.ErrReadOnly},
		"remove":            {route(hostRO, "nda.pdf").Remove(ctx), vfs.ErrReadOnly},
		"mkdir":             {route(hostRO, "new").MkdirAll(ctx), vfs.ErrReadOnly},
		"canceled document": {route(rw, "x.txt").WriteDocument(canceled, text), context.Canceled},
		"canceled read":     {func() error { _, err := route(rw, "nda.pdf").OpenDocument(canceled); return err }(), context.Canceled},
		"file over the cap": {readText("huge.bin"), vfs.ErrTooLarge},
		"Google Slides":     {readText("Deck"), vfs.ErrNoCodec},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if !errors.Is(c.err, c.want) {
				t.Fatalf("err = %v, want %v", c.err, c.want)
			}
		})
	}
}

// driveSpecDoc is testdrive.Tree with Spec as a heading and a paragraph.
func driveSpecDoc() *testdrive.FX {
	fx := testdrive.Tree()
	fx.SeedDoc("doc1", "R0", []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 8, Kind: "heading", Level: 1, NamedStyle: "HEADING_1", Text: "Spec"},
		{StartIndex: 8, EndIndex: 14, Kind: "paragraph", Text: "Hello"},
	}, nil)
	return fx
}

// driveCheckout reads the Google Doc at name and replaces its blocks.
func driveCheckout(t *testing.T, ms *vfs.MountSession, name string, blocks ...vfs.Block) vfs.Textual {
	t.Helper()
	doc, err := ms.Route(t.Context(), "/workspace/contracts/"+name).ReadText(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	rd, _ := vfs.AsRich(doc)
	rd.SetBlocks(blocks)
	return doc
}

// Setting the blocks of a Google Doc sends deletes, inserts, styles, a
// table and its cells in turn. A Docs call that fails at any step ends
// the write with the error of that call.
func TestDrive_aFailedDocsCallEndsASetBlocksWrite(t *testing.T) {
	t.Parallel()
	for name, f := range map[string]testdrive.Fault{
		"read before the inserts":   {Call: "documents.get", Skip: 0},
		"insert the paragraph":      {Call: "documents.batchUpdate", Skip: 1},
		"style the paragraph":       {Call: "documents.batchUpdate", Skip: 2},
		"read the inserted table":   {Call: "documents.get", Skip: 1},
		"fill the table":            {Call: "documents.batchUpdate", Skip: 4},
		"read after the table":      {Call: "documents.get", Skip: 2},
		"read the written document": {Call: "documents.get", Skip: 3},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fx := driveSpecDoc()
			ms := mountDrive(t, fx, true)
			doc := driveCheckout(t, ms, "Spec",
				vfs.Block{Kind: vfs.BlockKindParagraph, Text: "**bold** text"},
				vfs.Block{Kind: vfs.BlockKindTable, Text: "A\tB"},
			)
			f.Status = http.StatusForbidden
			fx.Fail(f)

			err := ms.WriteDocument(t.Context(), doc)

			if !errors.Is(err, vfs.ErrPermission) {
				t.Fatalf("err = %v, want ErrPermission", err)
			}
		})
	}
}

// driveTabs is testdrive.Tree with Spec in two tabs. Tab t.b has an
// image and an empty last paragraph.
func driveTabs() *testdrive.FX {
	fx := testdrive.Tree()
	fx.SeedDoc("doc1", "R0", []vfs.DocsSpan{
		{TabID: "t.a", StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{TabID: "t.a", StartIndex: 2, EndIndex: 8, Kind: "paragraph", Text: "Alpha"},
		{TabID: "t.a", StartIndex: 8, EndIndex: 9, Kind: "tableOfContents"},
		{TabID: "t.b", StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{TabID: "t.b", StartIndex: 2, EndIndex: 7, Kind: "paragraph", Text: "Beta"},
		{TabID: "t.b", StartIndex: 7, EndIndex: 8, Kind: "image", ObjectID: "kix.pic"},
		{TabID: "t.b", StartIndex: 8, EndIndex: 9, Kind: "paragraph"},
	}, []vfs.DocTab{{ID: "t.a", Title: "A"}, {ID: "t.b", Title: "B", Index: 1}})
	return fx
}

// A write to a Google Doc that the provider or Docs refuses leaves the
// Doc as it was: no blocks, blocks with no tab on a Doc with tabs, a new
// image, a table with no valid shape, no Docs client, a file that is not
// a Doc, and a Drive or Docs call that fails while the Doc is created.
func TestDrive_googleDocWritesThatAreRefused(t *testing.T) {
	t.Parallel()
	const doc = "application/vnd.google-apps.document"
	table := func(text string, attrs ...string) vfs.Block {
		b := vfs.Block{Kind: vfs.BlockKindTable, Text: text, Style: vfs.StyleMeta{Attributes: map[string]string{}}}
		for i := 0; i+1 < len(attrs); i += 2 {
			b.Style.Attributes[attrs[i]] = attrs[i+1]
		}
		return b
	}
	set := func(fx *testdrive.FX, blocks ...vfs.Block) error {
		ms := mountDrive(t, fx, true)
		return ms.WriteDocument(t.Context(), driveCheckout(t, ms, "Spec", blocks...))
	}
	create := func(name string, faults ...testdrive.Fault) error {
		ms := driveMount(t, testdrive.Tree(), true, faults...)
		return ms.WriteDocument(t.Context(), vfs.NewRichDocument("/workspace/contracts/"+name, doc, []vfs.Block{{Kind: vfs.BlockKindParagraph, Text: "New"}}))
	}
	refused := func(call string) testdrive.Fault { return testdrive.Fault{Call: call, Status: http.StatusForbidden} }
	noDocs := func() error {
		fx := testdrive.Tree()
		srv := testhttp.New(t, fx)
		holder := vfs.NewTokenHolder(mount.Credential{Token: "t"})
		open := vfs.DriveWith(vfs.NewGoogleDrive(holder, srv.URL+"/", srv.Client()), nil, vfs.NewGoogleSheets(holder, srv.URL+"/", srv.Client()), holder)
		ms, err := vfs.Tree(vfs.At("contracts", open))(t.Context(), "s", vfs.Request{Bindings: []mount.Binding{{
			Writable: true, Auth: mount.Credential{Token: "t"}, Params: map[string]string{mount.ParamName: "contracts", mount.ParamFolderID: "root-a"},
		}}})
		if err != nil {
			t.Fatal(err)
		}
		return ms.WriteDocument(t.Context(), vfs.NewRichDocument("/workspace/contracts/Spec", doc, nil))
	}

	for name, c := range map[string]struct {
		write func() error
		want  error
	}{
		"no blocks":                   {func() error { return set(driveSpecDoc()) }, vfs.ErrEmptyReplace},
		"no tab on a Doc with tabs":   {func() error { return set(driveTabs(), vfs.Block{Kind: vfs.BlockKindParagraph, Text: "x"}) }, vfs.ErrTabIDRequired},
		"a new image":                 {func() error { return set(driveSpecDoc(), vfs.Block{Kind: vfs.BlockKindImage}) }, vfs.ErrNotSupported},
		"a ragged table":              {func() error { return set(driveSpecDoc(), table("a\tb\nc", "rows", "2", "cols", "2")) }, vfs.ErrNotSupported},
		"a table with no shape":       {func() error { return set(driveSpecDoc(), table("")) }, vfs.ErrNotSupported},
		"an empty table with a shape": {func() error { return set(driveSpecDoc(), table("", "rows", "1", "cols", "1")) }, vfs.ErrNotSupported},
		"no Docs client":              {noDocs, vfs.ErrNotSupported},
		"a Sheet":                     {func() error { return create("Budget") }, vfs.ErrNotSupported},
		"resolve the path": {func() error {
			return create("New", testdrive.Fault{Call: "files.get", ID: "root-a", Status: http.StatusForbidden})
		}, vfs.ErrPermission},
		"create below a file": {func() error { return create("nda.pdf/New") }, vfs.ErrNotSupported},
		"create the file":     {func() error { return create("New", refused("files.create")) }, vfs.ErrPermission},
		"read the new Doc":    {func() error { return create("New", refused("documents.get")) }, vfs.ErrPermission},
		"insert into the new Doc": {func() error {
			fx := testdrive.Tree()
			ms := mountDrive(t, fx, true)
			fx.FailDocBatch()
			return ms.WriteDocument(t.Context(), vfs.NewRichDocument("/workspace/contracts/New", doc, []vfs.Block{{Kind: vfs.BlockKindParagraph, Text: "New"}}))
		}, vfs.ErrInvalidWrite},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := c.write(); !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

// driveDetached is a transport that sends each request to the end even
// when its context ends first, as a reply already on the wire does.
type driveDetached struct{ base http.RoundTripper }

func (d driveDetached) RoundTrip(r *http.Request) (*http.Response, error) {
	return d.base.RoundTrip(r.WithContext(context.WithoutCancel(r.Context())))
}

// A context that ends while a new Google Doc waits to try its insert
// again after a revision conflict ends the write.
func TestDrive_aNewGoogleDocStopsWaitingWhenTheContextEnds(t *testing.T) {
	t.Parallel()
	const doc = "application/vnd.google-apps.document"
	ctx := t.Context()
	fx := testdrive.Tree()
	srv := testhttp.New(t, fx)
	hc := &http.Client{Transport: driveDetached{srv.Client().Transport}}
	holder := vfs.NewTokenHolder(mount.Credential{Token: "t"})
	open := vfs.DriveWith(vfs.NewGoogleDrive(holder, srv.URL+"/", hc), vfs.NewGoogleDocs(holder, srv.URL+"/", hc), nil, holder)
	detached, err := vfs.Tree(vfs.At("contracts", open))(ctx, "s", vfs.Request{Bindings: []mount.Binding{{
		Writable: true, Auth: mount.Credential{Token: "t"}, Params: map[string]string{mount.ParamName: "contracts", mount.ParamFolderID: "root-a"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	fx.Fail(testdrive.Fault{Call: "documents.batchUpdate", Status: http.StatusBadRequest, Message: "requiredRevisionId must be the latest revision", Then: cancel})

	err = detached.Route(ctx, "/workspace/contracts/Waiting").WriteDocument(canceled, vfs.NewRichDocument("/workspace/contracts/Waiting", doc, []vfs.Block{{Kind: vfs.BlockKindParagraph, Text: "x"}}))

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// Setting the blocks of one tab of a Google Doc rewrites that tab: it
// keeps the image the blocks keep, appends after a table and leaves the
// other tab as it was, and a later replace rewrites the cells of the
// table. Blocks that name a tab write into a Doc that Docs returns with
// no tabs.
// changes no text sends nothing.
func TestDrive_googleDocTabsAndReplaces(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	inTab := func(tab string, b vfs.Block) vfs.Block {
		if b.Style.Attributes == nil {
			b.Style.Attributes = map[string]string{}
		}
		b.Style.Attributes["tab_id"] = tab
		return b
	}
	texts := func(ms *vfs.MountSession) []string {
		got, err := ms.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, b := range got.(vfs.Structured).Blocks() {
			out = append(out, b.Style.Attributes["tab_id"]+":"+b.Kind+":"+b.Text)
		}
		return out
	}

	tabs := mountDrive(t, driveTabs(), true)
	doc := driveCheckout(t, tabs, "Spec",
		inTab("t.b", vfs.Block{Kind: vfs.BlockKindParagraph, Text: "Gamma"}),
		inTab("t.b", vfs.Block{Kind: vfs.BlockKindListItem, Text: "item", Style: vfs.StyleMeta{Level: 1}}),
		inTab("t.b", vfs.Block{Kind: vfs.BlockKindTable, Text: "C\tD\nE\tF"}),
		inTab("t.b", vfs.Block{Kind: vfs.BlockKindImage, Style: vfs.StyleMeta{Attributes: map[string]string{"object_id": "kix.pic"}}}),
		inTab("t.b", vfs.Block{Kind: vfs.BlockKindTable, Text: "\t"}),
	)
	if err := tabs.WriteDocument(ctx, doc); err != nil {
		t.Fatal(err)
	}
	got := texts(tabs)
	for _, want := range []string{"t.a:paragraph:Alpha", "t.b:paragraph:Gamma", "t.b:list_item:item", "t.b:table:C\tD\nE\tF", "t.b:image:", "t.b:table:\t"} {
		if !slices.Contains(got, want) {
			t.Fatalf("blocks = %q, want %q", got, want)
		}
	}
	if slices.Contains(got, "t.b:paragraph:Beta") {
		t.Fatalf("blocks = %q, want Beta replaced", got)
	}
	cells, err := tabs.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rd, _ := vfs.AsRich(cells)
	for _, b := range rd.Blocks() {
		if b.Text == "C\tD\nE\tF" {
			if err := rd.ReplaceBlock(b.ID, "G\tH\nI\tJ", false); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tabs.WriteDocument(ctx, cells); err != nil {
		t.Fatal(err)
	}
	if got := texts(tabs); !slices.Contains(got, "t.b:table:G\tH\nI\tJ") {
		t.Fatalf("blocks = %q, want the replaced cells", got)
	}

	listed := testdrive.Tree()
	listed.SeedDoc("doc1", "R0", []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 4, Kind: "list_item", ListID: "kix.l", Text: "a"},
		{StartIndex: 4, EndIndex: 5, Kind: "image", ObjectID: "kix.pic"},
	}, nil)
	untabbed := mountDrive(t, listed, true)
	if err := untabbed.WriteDocument(ctx, driveCheckout(t, untabbed, "Spec", inTab("t.x", vfs.Block{Kind: vfs.BlockKindParagraph, Text: "Tabbed"}))); err != nil {
		t.Fatal(err)
	}
	if got := texts(untabbed); !slices.Contains(got, ":paragraph:Tabbed") {
		t.Fatalf("blocks = %q, want the tabbed paragraph", got)
	}

	fx := driveTabs()
	replaces := mountDrive(t, fx, true)
	filled, err := replaces.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rd, _ = vfs.AsRich(filled)
	blocks := rd.Blocks()
	if err := rd.ReplaceBlock(blocks[len(blocks)-1].ID, "Filled", false); err != nil {
		t.Fatal(err)
	}
	if err := replaces.WriteDocument(ctx, filled); err != nil {
		t.Fatal(err)
	}
	if got := texts(replaces); !slices.Contains(got, "t.b:paragraph:Filled") {
		t.Fatalf("blocks = %q, want the filled paragraph", got)
	}

	empty := testdrive.Tree()
	empty.SeedDoc("doc1", "R0", []vfs.DocsSpan{
		{StartIndex: 1, EndIndex: 2, Kind: "sectionBreak"},
		{StartIndex: 2, EndIndex: 7, Kind: "table", Text: "\t", Cells: []vfs.DocsCell{
			{Row: 0, Col: 0, StartIndex: 4, EndIndex: 5}, {Row: 0, Col: 1, StartIndex: 5, EndIndex: 6},
		}},
	}, nil)
	same := mountDrive(t, empty, true)
	unchanged, err := same.Route(ctx, "/workspace/contracts/Spec").ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rd, _ = vfs.AsRich(unchanged)
	if err := rd.ReplaceBlock(rd.Blocks()[0].ID, "\t", false); err != nil {
		t.Fatal(err)
	}
	if err := same.WriteDocument(ctx, unchanged); err != nil {
		t.Fatal(err)
	}
	if got := texts(same); !slices.Equal(got, []string{":table:\t"}) {
		t.Fatalf("blocks = %q, want the empty table", got)
	}
}

// driveGrid is a host's spreadsheet checkout at name.
func driveGrid(t *testing.T, name string, sheets ...vfs.Sheet) *vfs.IR {
	t.Helper()
	g, err := vfs.NewTabularDocument("/workspace/contracts/"+name, "application/vnd.google-apps.spreadsheet", sheets, nil)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

// driveCells is a sheet of one row with these cell inputs.
func driveCells(title string, inputs ...string) vfs.Sheet {
	row := make([]vfs.Cell, len(inputs))
	for i, in := range inputs {
		row[i] = vfs.Cell{Input: in, Value: in}
	}
	return vfs.Sheet{Title: title, Rows: 1, Cols: len(inputs), Cells: [][]vfs.Cell{row}}
}

// A host's grid written to a new path is a new Google Sheet: its first
// sheet takes the place of the sheet that Sheets makes. A grid with an
// untitled sheet and no sheet ID writes into Sheet1 of an existing
// Google Sheet, with each wrap and border style, and skips an empty
// sheet.
func TestDrive_aHostGridWritesAGoogleSheet(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fx := testdrive.Tree()
	ms := mountDrive(t, fx, true)
	styled := driveCells("", "bold", "clip", "overflow", "medium", "thick", "dashed", "dotted", "double", "cleared")
	formats := []vfs.CellFormat{
		{Bold: true}, {Wrap: "clip", Border: &vfs.CellBorder{Style: "medium"}}, {Wrap: "overflow", Border: &vfs.CellBorder{Style: "thick"}},
		{Border: &vfs.CellBorder{Style: "medium", Edges: "top"}}, {Border: &vfs.CellBorder{Style: "thick", Edges: "left"}},
		{Border: &vfs.CellBorder{Style: "dashed"}}, {Border: &vfs.CellBorder{Style: "dotted"}}, {Border: &vfs.CellBorder{Style: "double"}}, {},
	}
	formats[8].ApplyPatch(vfs.FormatPatch{Border: &vfs.CellBorder{}})
	for i, f := range formats {
		styled.Cells[0][i].Format = f
	}

	created := ms.WriteDocument(ctx, driveGrid(t, "Ledger", driveCells("Data", "A", "B")))
	overlaid := ms.WriteDocument(ctx, driveGrid(t, "Budget", styled, vfs.Sheet{Title: "Empty"}))

	if created != nil || overlaid != nil {
		t.Fatalf("create = %v, overlay = %v", created, overlaid)
	}
	doc, err := ms.Route(ctx, "/workspace/contracts/Ledger").ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	g, _ := vfs.AsGrid(doc)
	if a1, err := g.Cell("Sheet1", "A1"); err != nil || a1.Input != "A" {
		t.Fatalf("Ledger Sheet1!A1 = %+v err=%v", a1, err)
	}
	budget, err := ms.Route(ctx, "/workspace/contracts/Budget").ReadText(ctx)
	if err != nil {
		t.Fatal(err)
	}
	g, _ = vfs.AsGrid(budget)
	stored := fx.SheetSnap("sheet1").Sheets[0].Cells[0]
	border := func(f vfs.CellFormat) string {
		if f.Border == nil {
			return ""
		}
		return f.Border.Style + ":" + f.Border.Edges
	}
	for i, want := range formats {
		cell, err := g.Cell("Sheet1", vfs.FormatA1(1, i+1))
		if err != nil || cell.Input != styled.Cells[0][i].Input || stored[i].Input != cell.Input ||
			cell.Format.Bold != want.Bold || cell.Format.Wrap != want.Wrap || border(cell.Format) != border(want) {
			t.Fatalf("Sheet1 cell %d = %+v err=%v, stored %+v, want %+v", i, cell, err, stored[i], want)
		}
	}
}

// A grid write that the provider or Sheets refuses ends with its error:
// no Sheets client, a file that is not a Sheet, a failed Drive or Sheets
// call.
func TestDrive_googleSheetWritesThatAreRefused(t *testing.T) {
	t.Parallel()
	refused := func(call, id string) testdrive.Fault {
		return testdrive.Fault{Call: call, ID: id, Status: http.StatusForbidden}
	}
	bold := driveCells("Budget", "v")
	bold.Cells[0][0].Format = vfs.CellFormat{Bold: true}
	write := func(name string, sh vfs.Sheet, faults ...testdrive.Fault) error {
		ms := driveMount(t, testdrive.Tree(), true, faults...)
		return ms.WriteDocument(t.Context(), driveGrid(t, name, sh))
	}
	noSheets := func() error {
		fx := testdrive.Tree()
		srv := testhttp.New(t, fx)
		holder := vfs.NewTokenHolder(mount.Credential{Token: "t"})
		open := vfs.DriveWith(vfs.NewGoogleDrive(holder, srv.URL+"/", srv.Client()), vfs.NewGoogleDocs(holder, srv.URL+"/", srv.Client()), nil, holder)
		ms, err := vfs.Tree(vfs.At("contracts", open))(t.Context(), "s", vfs.Request{Bindings: []mount.Binding{{
			Writable: true, Auth: mount.Credential{Token: "t"}, Params: map[string]string{mount.ParamName: "contracts", mount.ParamFolderID: "root-a"},
		}}})
		if err != nil {
			t.Fatal(err)
		}
		return ms.WriteDocument(t.Context(), driveGrid(t, "Budget", bold))
	}
	for name, c := range map[string]struct {
		write func() error
		want  error
	}{
		"no Sheets client":       {noSheets, vfs.ErrNotSupported},
		"a Doc":                  {func() error { return write("Spec", bold) }, vfs.ErrNotSupported},
		"resolve the path":       {func() error { return write("Budget", bold, refused("files.get", "root-a")) }, vfs.ErrPermission},
		"write the values":       {func() error { return write("Budget", bold, refused("spreadsheets.values.batchUpdate", "sheet1")) }, vfs.ErrPermission},
		"write the formats":      {func() error { return write("Budget", bold, refused("spreadsheets.batchUpdate", "sheet1")) }, vfs.ErrPermission},
		"read the written Sheet": {func() error { return write("Budget", bold, refused("spreadsheets.get", "sheet1")) }, vfs.ErrPermission},
		"create below a file":    {func() error { return write("nda.pdf/Ledger", bold) }, vfs.ErrNotSupported},
		"create the file":        {func() error { return write("Ledger", bold, refused("files.create", "")) }, vfs.ErrPermission},
		"read the new Sheet":     {func() error { return write("Ledger", bold, refused("spreadsheets.get", "")) }, vfs.ErrPermission},
		"write the new Sheet":    {func() error { return write("Ledger", bold, refused("spreadsheets.values.batchUpdate", "")) }, vfs.ErrPermission},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := c.write(); !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

// driveDocsReply is a Docs server that answers every documents.get with
// body and accepts every batchUpdate.
func driveDocsReply(t *testing.T, body ...any) *vfs.MountSession {
	t.Helper()
	docs := testhttp.New(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			_, _ = w.Write([]byte(`{}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"documentId": "doc1", "revisionId": "R0", "body": map[string]any{"content": body}})
	}))
	drive := testhttp.New(t, testdrive.Tree())
	holder := vfs.NewTokenHolder(mount.Credential{Token: "t"})
	open := vfs.DriveWith(vfs.NewGoogleDrive(holder, drive.URL+"/", drive.Client()), vfs.NewGoogleDocs(holder, docs.URL+"/", docs.Client()), nil, holder)
	ms, err := vfs.Tree(vfs.At("contracts", open))(t.Context(), "s", vfs.Request{Bindings: []mount.Binding{{
		Writable: true, Auth: mount.Credential{Token: "t"}, Params: map[string]string{mount.ParamName: "contracts", mount.ParamFolderID: "root-a"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

// driveElement is a Docs structural element from start to end.
func driveElement(start, end int, kind string, value any) map[string]any {
	return map[string]any{"startIndex": start, "endIndex": end, kind: value}
}

// driveTable is a Docs table whose rows have these numbers of empty cells,
// from start.
func driveTable(start int, rows ...int) map[string]any {
	idx := start + 1
	var trs []any
	for _, n := range rows {
		var cells []any
		for range n {
			cells = append(cells, map[string]any{"startIndex": idx, "endIndex": idx + 2, "content": []any{
				driveElement(idx+1, idx+2, "paragraph", map[string]any{"elements": []any{map[string]any{"textRun": map[string]any{"content": "\n"}}}}),
			}})
			idx += 2
		}
		trs = append(trs, map[string]any{"tableCells": cells})
	}
	return driveElement(start, idx+1, "table", map[string]any{"tableRows": trs})
}

// A write to a Google Doc whose Docs reply does not match the write ends
// with ErrNotSupported: an inserted table that the Doc does not show, a
// table with more rows than the write, and a replace beside a ragged
// table or a table with no rows. A table with no paragraph after it
// writes.
func TestDrive_docsRepliesThatDoNotMatchTheWrite(t *testing.T) {
	t.Parallel()
	section := driveElement(1, 2, "sectionBreak", map[string]any{})
	hello := driveElement(2, 8, "paragraph", map[string]any{"elements": []any{map[string]any{"textRun": map[string]any{"content": "Hello\n"}}}})
	setTable := func(ms *vfs.MountSession) error {
		return ms.WriteDocument(t.Context(), driveCheckout(t, ms, "Spec", vfs.Block{Kind: vfs.BlockKindTable, Text: "A"}))
	}
	replaceHello := func(ms *vfs.MountSession) error {
		doc, err := ms.Route(t.Context(), "/workspace/contracts/Spec").ReadText(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		rd, _ := vfs.AsRich(doc)
		if err := rd.ReplaceBlock(rd.Blocks()[0].ID, "Bye", false); err != nil {
			t.Fatal(err)
		}
		return ms.WriteDocument(t.Context(), doc)
	}
	for name, c := range map[string]struct {
		body  []any
		write func(*vfs.MountSession) error
		want  error
	}{
		"no inserted table":           {[]any{section, hello}, setTable, vfs.ErrNotSupported},
		"more rows than written":      {[]any{section, hello, driveTable(8, 1, 1)}, setTable, vfs.ErrNotSupported},
		"a replace by a ragged table": {[]any{section, hello, driveTable(8, 1, 2)}, replaceHello, vfs.ErrNotSupported},
		"a replace by an empty table": {[]any{section, hello, driveTable(8)}, replaceHello, vfs.ErrNotSupported},
		"a table with no paragraph":   {[]any{section, driveTable(2, 1)}, setTable, nil},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := c.write(driveDocsReply(t, c.body...)); !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

// Content at a size cap does not load: a host document too large to
// encode, a file that grew after Drive reported its size, a Doc export
// over the export cap or that unzips to more than the read cap, a sheet
// with more cells than the cap, and a workbook that a write takes over
// the cap. The test is serial: each step holds tens of megabytes, and in
// parallel with the other tests they take the race run out of memory.
func TestDrive_contentAtTheSizeCapsDoesNotLoad(t *testing.T) {
	ctx := t.Context()
	readText := func(ms *vfs.MountSession, name string) error {
		_, err := ms.Route(ctx, "/workspace/contracts/"+name).ReadText(ctx)
		return err
	}
	for name, step := range map[string]func() error{
		"a host document too large to encode": func() error {
			return mountDrive(t, testdrive.Tree(), true).WriteDocument(ctx, driveHostText{"/workspace/contracts/big.txt", strings.Repeat("a", vfs.MaxReadFileBytes+1)})
		},
		"a file that grew": func() error {
			fx := testdrive.Tree()
			fx.Add("root-a", vfs.DriveMeta{ID: "grown", Name: "grown.txt", MimeType: "text/plain", Size: 1}, nil)
			fx.Nodes["grown"].Body = fx.Nodes["huge"].Body
			return readText(mountDrive(t, fx, true), "grown.txt")
		},
		"an export over the export cap": func() error {
			fx := testdrive.Tree()
			fx.Nodes["doc1"].Export = bytesRepeat(vfs.MaxDocsExportBytes + 2)
			return readText(mountDrive(t, fx, false), "Spec")
		},
		"an export that unzips over the read cap": func() error {
			fx := testdrive.Tree()
			var zipped bytes.Buffer
			zw := zip.NewWriter(&zipped)
			w, err := zw.Create("Spec.html")
			if err != nil {
				t.Fatal(err)
			}
			chunk := bytes.Repeat([]byte("a"), 1<<20)
			for range vfs.MaxReadFileBytes/len(chunk) + 1 {
				if _, err := w.Write(chunk); err != nil {
					t.Fatal(err)
				}
			}
			if err := zw.Close(); err != nil {
				t.Fatal(err)
			}
			fx.Nodes["doc1"].Export = zipped.Bytes()
			return readText(mountDrive(t, fx, false), "Spec")
		},
		"a sheet over the cell cap": func() error {
			fx := testdrive.Tree()
			row := slices.Repeat([]vfs.Cell{{Input: "x", Value: "x"}}, vfs.MaxSheetCells+1)
			fx.SeedSheet("sheet1", vfs.SheetsSnapshot{Sheets: []vfs.Sheet{{ID: "1", Title: "Budget", Cells: [][]vfs.Cell{row}}}})
			return readText(mountDrive(t, fx, true), "Budget")
		},
		"a write and a read over the cell cap": func() error {
			// The big sheet is 10 cells under the cap; a write of 11
			// cells to the small sheet takes the workbook over it.
			fx := testdrive.Tree()
			rows := make([][]vfs.Cell, vfs.MaxSheetCells/10-1)
			for i := range rows {
				rows[i] = slices.Repeat([]vfs.Cell{{Input: "x", Value: "x"}}, 10)
			}
			fx.SeedSheet("sheet1", vfs.SheetsSnapshot{Sheets: []vfs.Sheet{
				{ID: "1", Title: "Big", Rows: len(rows), Cols: 10, Cells: rows},
				{ID: "2", Title: "Small", Index: 1, Rows: 1, Cols: 1, Cells: [][]vfs.Cell{{{Input: "s", Value: "s"}}}},
			}})
			ms := mountDrive(t, fx, true)
			if err := ms.WriteDocument(ctx, driveGrid(t, "Budget", driveCells("Small", slices.Repeat([]string{"y"}, 11)...))); !errors.Is(err, vfs.ErrTooLarge) {
				t.Fatalf("write = %v, want ErrTooLarge", err)
			}
			return readText(ms, "Budget")
		},
	} {
		debug.FreeOSMemory() // the bytes of the step before are garbage
		if err := step(); !errors.Is(err, vfs.ErrTooLarge) {
			t.Fatalf("%s: err = %v, want ErrTooLarge", name, err)
		}
	}
}
