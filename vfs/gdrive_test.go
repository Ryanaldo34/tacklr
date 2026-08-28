package vfs_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ryanaldo34/tacklr/vfs"
)

func driveTree() *memDrive {
	d := newMemDrive()
	d.add("", vfs.DriveMeta{ID: "root-a", Name: "Contracts", MimeType: "application/vnd.google-apps.folder"}, nil)
	d.add("", vfs.DriveMeta{ID: "root-b", Name: "Notes", MimeType: "application/vnd.google-apps.folder"}, nil)
	d.add("root-a", vfs.DriveMeta{ID: "nda", Name: "nda.pdf", MimeType: "application/pdf", Size: 4}, []byte("%PDF"))
	d.add("root-a", vfs.DriveMeta{ID: "sub", Name: "acme", MimeType: "application/vnd.google-apps.folder"}, nil)
	d.add("sub", vfs.DriveMeta{ID: "note", Name: "note.md", MimeType: "text/markdown", Size: 6}, []byte("# hi\n\n"))
	d.add("root-a", vfs.DriveMeta{ID: "dup1", Name: "dup.txt", MimeType: "text/plain", Size: 1}, []byte("a"))
	d.add("root-a", vfs.DriveMeta{ID: "dup2", Name: "dup.txt", MimeType: "text/plain", Size: 1}, []byte("b"))
	d.add("root-a", vfs.DriveMeta{ID: "doc1", Name: "Spec", MimeType: "application/vnd.google-apps.document"}, nil)
	d.add("root-a", vfs.DriveMeta{ID: "sheet1", Name: "Budget", MimeType: "application/vnd.google-apps.spreadsheet"}, nil)
	d.add("root-a", vfs.DriveMeta{ID: "slides1", Name: "Deck", MimeType: "application/vnd.google-apps.presentation"}, nil)
	d.add("root-b", vfs.DriveMeta{ID: "readme", Name: "readme.txt", MimeType: "text/plain", Size: 5}, []byte("hello"))
	d.add("root-b", vfs.DriveMeta{
		ID: "sc-folder", Name: "alias", MimeType: "application/vnd.google-apps.shortcut",
		TargetID: "sub", TargetMime: "application/vnd.google-apps.folder",
	}, nil)
	d.add("root-a", vfs.DriveMeta{ID: "huge", Name: "huge.bin", MimeType: "application/octet-stream", Size: int64(vfs.MaxReadFileBytes) + 1}, make([]byte, vfs.MaxReadFileBytes+1))
	return d
}

func TestMountSession_gdriveReadOnlySession(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	holder := vfs.NewTokenHolder(vfs.Credential{Token: "tok"})
	ms, err := vfs.Tree(
		vfs.At("contracts", vfs.Drive(api)),
		vfs.At("notes", vfs.Drive(api)),
	)(ctx, "sess-gd", vfs.Request{Bindings: []vfs.Binding{
		{Provider: vfs.ProviderGoogleDrive, Params: map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root-a"}, Auth: vfs.Credential{Token: "tok"}, Live: holder},
		{Provider: vfs.ProviderGoogleDrive, Params: map[string]string{vfs.ParamName: "notes", vfs.ParamFolderID: "root-b"}, Auth: vfs.Credential{Token: "tok"}, Live: holder},
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	specs := ms.Specs()
	if len(specs) != 1 || specs[0].Point != vfs.WorkspacePoint || len(specs[0].Members) != 2 {
		t.Fatalf("Specs = %+v", specs)
	}
	for _, m := range specs[0].Members {
		if !m.ReadOnly || m.Params[vfs.ParamName] == "" {
			t.Fatalf("member = %+v", m)
		}
	}

	st, err := ms.Stat(ctx, "/workspace/contracts/nda.pdf")
	if err != nil || st.IsDir || st.MediaType != "application/pdf" || st.Size != 4 {
		t.Fatalf("Stat nda = %+v err=%v", st, err)
	}
	raw, err := ms.ReadFile(ctx, "/workspace/contracts/nda.pdf")
	if err != nil || string(raw) != "%PDF" {
		t.Fatalf("ReadFile nda = %q err=%v", raw, err)
	}
	text, err := ms.ReadText(ctx, "/workspace/contracts/acme/note.md")
	if err != nil || !strings.Contains(text.Text(), "# hi") {
		t.Fatalf("ReadText note = %v err=%v", text, err)
	}

	ents, err := ms.ReadDir(ctx, "/workspace/contracts")
	if err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{}
	var acmeDir bool
	for _, e := range ents {
		present[e.Name] = true
		if e.Name == "acme" {
			acmeDir = e.IsDir
		}
	}
	if !present["acme"] || !acmeDir || !present["nda.pdf"] || !present["Spec"] {
		t.Fatalf("ReadDir names = %+v", ents)
	}

	if _, err := ms.ReadFile(ctx, "/workspace/contracts/dup.txt"); !errors.Is(err, vfs.ErrAmbiguous) {
		t.Fatalf("collision: %v", err)
	}
	if _, err := ms.ReadFile(ctx, "/workspace/contracts/missing"); !errors.Is(err, vfs.ErrNotExist) {
		t.Fatalf("missing: %v", err)
	}
	if err := ms.WriteFile(ctx, "/workspace/contracts/nda.pdf", []byte("x")); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("write: %v", err)
	}
	if err := ms.MkdirAll(ctx, "/workspace/contracts/new"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("mkdir: %v", err)
	}
	if err := ms.Remove(ctx, "/workspace/contracts/nda.pdf"); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("remove: %v", err)
	}

	// Shortcut to a folder is walkable by the shortcut name.
	got, err := ms.ReadFile(ctx, "/workspace/notes/alias/note.md")
	if err != nil || string(got) != "# hi\n\n" {
		t.Fatalf("shortcut walk = %q err=%v", got, err)
	}

	if _, err := ms.ReadFile(ctx, "/workspace/contracts/huge.bin"); !errors.Is(err, vfs.ErrTooLarge) {
		t.Fatalf("huge: %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ms.Stat(canceled, "/workspace/contracts/nda.pdf"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}

	// Near-expiry token refreshes before the provider request.
	holder.Set(vfs.Credential{Token: "expiring", ExpiresAt: time.Now().Add(10 * time.Second)})
	proactiveRefreshes := 0
	holder.SetRefresh(func(context.Context) (vfs.Credential, error) {
		proactiveRefreshes++
		return vfs.Credential{Token: "proactive", ExpiresAt: time.Now().Add(time.Hour)}, nil
	})
	raw, err = ms.ReadFile(ctx, "/workspace/notes/readme.txt")
	if err != nil || string(raw) != "hello" || proactiveRefreshes != 1 {
		t.Fatalf("proactive refresh read = %q refreshes=%d err=%v", raw, proactiveRefreshes, err)
	}

	// 401 + one reactive refresh succeeds.
	api.once["GetMedia"] = vfs.ErrAuthExpired
	holder.SetRefresh(func(context.Context) (vfs.Credential, error) {
		return vfs.Credential{Token: "fresh"}, nil
	})
	raw, err = ms.ReadFile(ctx, "/workspace/notes/readme.txt")
	if err != nil || string(raw) != "hello" {
		t.Fatalf("refresh read = %q err=%v", raw, err)
	}

	// 401 without refresh stays expired.
	api.fail["GetMeta"] = vfs.ErrAuthExpired
	holder.SetRefresh(nil)
	if _, err := ms.Stat(ctx, "/workspace/notes/readme.txt"); !errors.Is(err, vfs.ErrAuthExpired) {
		t.Fatalf("expired: %v", err)
	}
}

func TestDrive_requiresClient(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	_ = vfs.Drive(nil)
}

func TestMountSession_gdriveDirectoryAndWriteDocument(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	ms, err := vfs.Tree(vfs.At("contracts", vfs.Drive(api)))(ctx, "s", vfs.Request{Bindings: []vfs.Binding{{
		Provider: "gdrive",
		Params:   map[string]string{vfs.ParamName: "contracts", vfs.ParamFolderID: "root-a"},
		Auth:     vfs.Credential{Token: "t"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })
	if _, err := ms.ReadFile(ctx, "/workspace/contracts/acme"); err == nil {
		t.Fatal("ReadFile on directory")
	}
	if _, err := ms.ReadText(ctx, "/workspace/contracts/acme"); err == nil {
		t.Fatal("ReadText on directory")
	}
	if _, err := ms.ReadDir(ctx, "/workspace/contracts/nda.pdf"); err == nil {
		t.Fatal("ReadDir on file")
	}
	doc, err := ms.OpenDocument(ctx, "/workspace/contracts/acme/note.md", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ms.WriteDocument(ctx, doc); !errors.Is(err, vfs.ErrReadOnly) {
		t.Fatalf("WriteDocument: %v", err)
	}
}

func TestDrive_validateRejectsFileID(t *testing.T) {
	ctx := t.Context()
	api := driveTree()
	open := vfs.Drive(api)
	folder, err := open(ctx, "s", vfs.Binding{
		Auth: vfs.Credential{Token: "t"}, Params: map[string]string{vfs.ParamFolderID: "root-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := folder.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	file, err := open(ctx, "s", vfs.Binding{
		Auth: vfs.Credential{Token: "t"}, Params: map[string]string{vfs.ParamFolderID: "nda"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Validate(ctx); err == nil {
		t.Fatal("file id must fail Validate")
	}
}
