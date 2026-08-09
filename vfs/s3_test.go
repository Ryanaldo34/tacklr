package vfs_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestS3Provider_validateAndMount(t *testing.T) {
	ctx := t.Context()
	p := vfs.S3Provider{Client: struct{}{}, Bucket: "my-bucket", Prefix: "runs/123/"}
	if err := p.Validate(ctx); err != nil {
		t.Fatal(err)
	}
	fs := vfs.New()
	if err := fs.Mount(ctx, vfs.MountSpec{Point: "/artifacts", Profile: "s3", ReadOnly: true}, p); err != nil {
		t.Fatal(err)
	}
	info, rel, err := fs.Lookup("/artifacts/out.json")
	if err != nil || !info.ReadOnly || rel != "out.json" {
		t.Fatalf("Lookup = %+v rel=%q err=%v", info, rel, err)
	}
}

func TestS3Provider_validateFailures(t *testing.T) {
	ctx := t.Context()
	cases := []vfs.S3Provider{
		{Bucket: "b"},
		{Client: struct{}{}, Bucket: ""},
		{Client: struct{}{}, Bucket: "b", Prefix: "a/../b"},
		{Client: struct{}{}, Bucket: "b", Prefix: "a//b"},
	}
	for _, p := range cases {
		if err := p.Validate(ctx); err == nil {
			t.Fatalf("Validate(%+v): want error", p)
		}
	}
	for _, prefix := range []string{"", "/", "///", "/ok/prefix"} {
		if err := (vfs.S3Provider{Client: struct{}{}, Bucket: "b", Prefix: prefix}).Validate(ctx); err != nil {
			t.Fatalf("prefix %q: %v", prefix, err)
		}
	}
	fs := vfs.New()
	if err := fs.Mount(ctx, vfs.MountSpec{Point: "/s3", Profile: "s3"}, vfs.S3Provider{Bucket: "b"}); err == nil {
		t.Fatal("nil client mount: want error")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := (vfs.S3Provider{Client: struct{}{}, Bucket: "b"}).Validate(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}

func TestS3Factory_open(t *testing.T) {
	client := struct{}{}
	f := vfs.S3Factory{ID: "assets", Client: client, DefaultBucket: "def"}
	p, err := f.Open(t.Context(), "s", vfs.MountSpec{Params: map[string]string{"prefix": "x/"}})
	if err != nil {
		t.Fatal(err)
	}
	sp, ok := p.(vfs.S3Provider)
	if !ok || sp.Bucket != "def" || sp.Client != client {
		t.Fatalf("provider = %#v", p)
	}
	// failures
	if _, err := (vfs.S3Factory{Client: client}).Open(t.Context(), "s", vfs.MountSpec{}); err == nil {
		t.Fatal("empty id")
	}
	if _, err := (vfs.S3Factory{ID: "a"}).Open(t.Context(), "s", vfs.MountSpec{}); err == nil {
		t.Fatal("nil client")
	}
	if _, err := (vfs.S3Factory{ID: "a", Client: client}).Open(t.Context(), "s", vfs.MountSpec{}); err == nil {
		t.Fatal("no bucket")
	}
	if _, err := f.Open(t.Context(), "s", vfs.MountSpec{Params: map[string]string{"prefix": "a/../b"}}); err == nil {
		t.Fatal("bad prefix")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.Open(canceled, "s", vfs.MountSpec{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
}
