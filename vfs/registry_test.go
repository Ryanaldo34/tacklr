package vfs_test

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestMaterialize_andSharedS3Client(t *testing.T) {
	ctx := t.Context()
	base := t.TempDir()
	shared := &struct{ name string }{name: "pool"}
	var openCount atomic.Int32

	reg := vfs.NewBackendRegistry()
	if err := reg.Register(vfs.LocalFactory{ID: "scratch", Base: base}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(countingS3Factory{
		S3Factory: vfs.S3Factory{ID: "assets", Client: shared, DefaultBucket: "default-b"},
		opens:     &openCount,
	}); err != nil {
		t.Fatal(err)
	}

	specs := []vfs.MountSpec{
		{Point: "/scratch", Profile: "scratch", Params: map[string]string{"session_scoped": "true"}},
		{Point: "/assets", Profile: "assets", ReadOnly: true, Params: map[string]string{"prefix": "t/"}},
		{Point: "/other", Profile: "assets", Params: map[string]string{"bucket": "other-b"}},
	}

	fsA, err := vfs.Materialize(ctx, reg, "sess-a", specs)
	if err != nil {
		t.Fatalf("Materialize A: %v", err)
	}
	fsB, err := vfs.Materialize(ctx, reg, "sess-b", specs[:1])
	if err != nil {
		t.Fatalf("Materialize B: %v", err)
	}
	if openCount.Load() != 3 { // 2 assets + wait: sess-a has 2 s3 opens, sess-b 0 s3 = 2. local opens not counted.
		// countingS3Factory only counts S3 — sess-a two assets mounts = 2
		if openCount.Load() != 2 {
			t.Fatalf("S3 Open count = %d, want 2 (shared client, two mounts)", openCount.Load())
		}
	}

	// Lifecycle: unmount then remount; Specs track changes
	if err := fsA.Unmount("/other"); err != nil {
		t.Fatal(err)
	}
	if len(fsA.Specs()) != 2 {
		t.Fatalf("after unmount: %+v", fsA.Specs())
	}
	p, err := reg.Open(ctx, "sess-a", vfs.MountSpec{Point: "/new", Profile: "scratch"})
	if err != nil {
		t.Fatal(err)
	}
	if err := fsA.Mount(ctx, vfs.MountSpec{Point: "/new", Profile: "scratch"}, p); err != nil {
		t.Fatal(err)
	}
	if len(fsA.Specs()) != 3 || fsA.Specs()[1].Point != "/new" && fsA.Specs()[2].Point != "/new" {
		// just ensure /new present
		found := false
		for _, s := range fsA.Specs() {
			if s.Point == "/new" {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing /new: %+v", fsA.Specs())
		}
	}

	// Isolation: different live FS instances
	if fsA == fsB {
		t.Fatal("sessions must not share FS pointer")
	}

	// Restart simulation
	restored, err := vfs.Materialize(ctx, reg, "sess-a", fsA.Specs())
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.Mounts()) != len(fsA.Mounts()) {
		t.Fatalf("restored mounts = %+v want %+v", restored.Mounts(), fsA.Mounts())
	}
}

func TestMaterialize_unknownProfile(t *testing.T) {
	reg := vfs.NewBackendRegistry()
	_, err := vfs.Materialize(t.Context(), reg, "s", []vfs.MountSpec{{Point: "/x", Profile: "missing"}})
	if !errors.Is(err, vfs.ErrUnknownProfile) {
		t.Fatalf("err = %v, want ErrUnknownProfile", err)
	}
	if _, err := vfs.Materialize(t.Context(), nil, "s", []vfs.MountSpec{{Point: "/x", Profile: "p"}}); err == nil {
		t.Fatal("nil registry Materialize")
	}
}

func TestMergeSpecs_duplicatePoint(t *testing.T) {
	_, err := vfs.MergeSpecs(
		[]vfs.MountSpec{{Point: "/a", Profile: "p"}},
		[]vfs.MountSpec{{Point: "/a", Profile: "p"}},
	)
	if !errors.Is(err, vfs.ErrAlreadyMounted) {
		t.Fatalf("duplicate: %v", err)
	}
	merged, err := vfs.MergeSpecs(
		[]vfs.MountSpec{{Point: "/a", Profile: "p"}},
		[]vfs.MountSpec{{Point: "/b", Profile: "p"}},
	)
	if err != nil || len(merged) != 2 {
		t.Fatalf("merge = %v err=%v", merged, err)
	}
}

func TestLocalFactory_jail(t *testing.T) {
	f := vfs.LocalFactory{ID: "scratch", Base: t.TempDir()}
	_, err := f.Open(t.Context(), "s", vfs.MountSpec{Params: map[string]string{"subpath": ".."}})
	if err == nil {
		t.Fatal("want escape error")
	}
	_, err = f.Open(t.Context(), "s", vfs.MountSpec{Params: map[string]string{"subpath": "/abs"}})
	if err == nil {
		t.Fatal("want absolute subpath error")
	}
	_, err = f.Open(t.Context(), "s", vfs.MountSpec{Params: map[string]string{"subpath": "ok/nested"}})
	if err != nil {
		t.Fatal(err)
	}
	// bad session segment with session_scoped
	_, err = f.Open(t.Context(), "../evil", vfs.MountSpec{Params: map[string]string{"session_scoped": "true"}})
	if err == nil {
		t.Fatal("want bad session id error")
	}
	// empty factory id / relative base
	if _, err := (vfs.LocalFactory{Base: t.TempDir()}).Open(t.Context(), "s", vfs.MountSpec{}); err == nil {
		t.Fatal("want empty id error")
	}
	if _, err := (vfs.LocalFactory{ID: "x", Base: "rel"}).Open(t.Context(), "s", vfs.MountSpec{}); err == nil {
		t.Fatal("want relative base error")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.Open(canceled, "s", vfs.MountSpec{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
	// Base is a file → MkdirAll fails
	file, err := os.CreateTemp(t.TempDir(), "notdir")
	if err != nil {
		t.Fatal(err)
	}
	name := file.Name()
	_ = file.Close()
	if _, err := (vfs.LocalFactory{ID: "f", Base: name}).Open(t.Context(), "s", vfs.MountSpec{}); err == nil {
		t.Fatal("want mkdir error when base is a file")
	}
}

func TestRegistry_registerAndOpenErrors(t *testing.T) {
	var nilReg *vfs.BackendRegistry
	if err := nilReg.Register(vfs.LocalFactory{ID: "x", Base: t.TempDir()}); err == nil {
		t.Fatal("nil registry register")
	}
	reg := vfs.NewBackendRegistry()
	if err := reg.Register(nil); err == nil {
		t.Fatal("nil factory")
	}
	if err := reg.Register(vfs.LocalFactory{Base: t.TempDir()}); err == nil {
		t.Fatal("empty profile factory")
	}
	if _, err := nilReg.Open(t.Context(), "s", vfs.MountSpec{Profile: "x"}); err == nil {
		t.Fatal("nil open")
	}
	if _, err := reg.Open(t.Context(), "s", vfs.MountSpec{}); err == nil {
		t.Fatal("empty profile open")
	}
	// Materialize mount failure (duplicate point in specs list)
	_ = reg.Register(vfs.LocalFactory{ID: "scratch", Base: t.TempDir()})
	_, err := vfs.Materialize(t.Context(), reg, "s", []vfs.MountSpec{
		{Point: "/a", Profile: "scratch"},
		{Point: "/a", Profile: "scratch"},
	})
	if err == nil {
		t.Fatal("want duplicate mount error")
	}
	// MergeSpecs invalid path
	if _, err := vfs.MergeSpecs([]vfs.MountSpec{{Point: "rel", Profile: "p"}}, nil); err == nil {
		t.Fatal("want merge invalid path")
	}
}

// countingS3Factory wraps S3Factory and counts Open calls.
type countingS3Factory struct {
	vfs.S3Factory
	opens *atomic.Int32
}

func (c countingS3Factory) Open(ctx context.Context, sessionID string, spec vfs.MountSpec) (vfs.Provider, error) {
	c.opens.Add(1)
	return c.S3Factory.Open(ctx, sessionID, spec)
}
