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
		t.Fatalf("Validate: %v", err)
	}

	fs := vfs.New()
	if err := fs.Mount(ctx, "/artifacts", p, true); err != nil {
		t.Fatalf("Mount: %v", err)
	}
	info, rel, err := fs.Lookup("/artifacts/out.json")
	if err != nil || !info.ReadOnly || info.Point != "/artifacts" || rel != "out.json" {
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
	if err := fs.Mount(ctx, "/s3", vfs.S3Provider{Bucket: "b"}, false); !errors.Is(err, vfs.ErrInvalidProvider) {
		t.Fatalf("Mount nil client: %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := (vfs.S3Provider{Client: struct{}{}, Bucket: "b"}).Validate(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Validate canceled: %v", err)
	}
}
