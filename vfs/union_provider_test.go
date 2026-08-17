package vfs

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"testing"
)

// probe injects member errors that a live local/S3 backend will not produce
// on demand (Validate fail, ReadDir fail, Stat fail other than not-exist).
type probe struct {
	stat, readDir, validate error
	exists                  string
}

func (p probe) Validate(context.Context) error { return p.validate }
func (p probe) Stat(_ context.Context, name string) (FileInfo, error) {
	if p.stat != nil {
		return FileInfo{}, p.stat
	}
	if name == "" || name == p.exists {
		return FileInfo{Name: name, IsDir: name == ""}, nil
	}
	return FileInfo{}, ErrNotExist
}
func (p probe) OpenFile(context.Context, string, int, fs.FileMode) (File, error) {
	return nil, ErrNotSupported
}
func (p probe) ReadDir(_ context.Context, _ string) ([]DirEntry, error) {
	if p.readDir != nil {
		return nil, p.readDir
	}
	return nil, nil
}
func (p probe) Remove(context.Context, string) error                { return ErrReadOnly }
func (p probe) MkdirAll(context.Context, string, fs.FileMode) error { return ErrReadOnly }

func TestUnionProvider_rejectsWrites(t *testing.T) {
	u := unionProvider{}
	if err := u.Remove(t.Context(), "x"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Remove = %v", err)
	}
	if err := u.MkdirAll(t.Context(), "x", 0o755); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("MkdirAll = %v", err)
	}
	if err := u.WriteDocument(t.Context(), "x", nil); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("WriteDocument = %v", err)
	}
	if _, err := u.OpenFile(t.Context(), "x", os.O_WRONLY, 0); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("OpenFile = %v", err)
	}
}

func TestUnionProvider_memberFailures(t *testing.T) {
	ctx := t.Context()
	t.Run("validate", func(t *testing.T) {
		u := unionProvider{members: []Provider{probe{validate: errors.New("bad")}}}
		if err := u.Validate(ctx); err == nil || err.Error() != "bad" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("list root", func(t *testing.T) {
		u := unionProvider{members: []Provider{probe{readDir: errors.New("list")}}}
		if _, err := u.ReadDir(ctx, ""); err == nil || err.Error() != "list" {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("stat other", func(t *testing.T) {
		boom := errors.New("stat")
		u := unionProvider{members: []Provider{probe{stat: boom}}}
		if _, err := u.Stat(ctx, "x"); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("no document IR", func(t *testing.T) {
		u := unionProvider{members: []Provider{probe{exists: "doc"}}}
		if _, err := u.OpenDocument(ctx, "doc", nil); !errors.Is(err, ErrNotSupported) {
			t.Fatalf("err = %v", err)
		}
		if _, err := u.OpenFile(ctx, "doc", os.O_RDONLY, 0); !errors.Is(err, ErrNotSupported) {
			t.Fatalf("OpenFile = %v", err)
		}
		if _, err := u.ReadDir(ctx, "doc"); err != nil {
			t.Fatalf("ReadDir = %v", err)
		}
	})
}

func TestUnionProvider_invalidPath(t *testing.T) {
	u := unionProvider{members: []Provider{probe{}}}
	ctx := t.Context()
	if _, err := u.Stat(ctx, ".."); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("Stat = %v", err)
	}
	if _, err := u.OpenFile(ctx, "..", os.O_RDONLY, 0); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("OpenFile = %v", err)
	}
	if _, err := u.OpenFile(ctx, "", os.O_RDONLY, 0); err == nil {
		t.Fatal("OpenFile root")
	}
	if _, err := u.ReadDir(ctx, ".."); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("ReadDir = %v", err)
	}
	if _, err := u.OpenDocument(ctx, "..", nil); !errors.Is(err, ErrInvalidPath) {
		t.Fatalf("OpenDocument = %v", err)
	}
	if _, err := u.OpenDocument(ctx, "", nil); err == nil {
		t.Fatal("OpenDocument root")
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	reg := NewBackendRegistry()
	if err := reg.Register(LocalFactory{ID: "a", Base: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.openUnion(canceled, "s", MountSpec{Profile: "skills", Members: []MountSpec{{Profile: "a"}}}); err == nil {
		t.Fatal("openUnion canceled")
	}
}
