package builtins

import (
	"testing"

	"github.com/ryanaldo34/tacklr/vfs"
)

func TestVFSConstructors_openLocalAndMemory(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	ms, err := vfs.Tree(
		vfs.At("work", Local(dir)),
		vfs.At("mem", Memory()),
	)(ctx, "sess", vfs.Request{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ms.Close() })

	if err := ms.WriteFile(ctx, "/workspace/work/note.txt", []byte("hi")); err != nil {
		t.Fatal(err)
	}
	body, err := ms.ReadFile(ctx, "/workspace/work/note.txt")
	if err != nil || string(body) != "hi" {
		t.Fatalf("read = %q err=%v", body, err)
	}
	if err := ms.WriteFile(ctx, "/workspace/mem/scratch.txt", []byte("tmp")); err != nil {
		t.Fatal(err)
	}
}

func TestObjectStoreConstructors_requireClient(t *testing.T) {
	ctx := t.Context()
	if _, err := S3(nil, "")(ctx, "s", vfs.Binding{}); err == nil {
		t.Fatal("S3 accepted a nil client")
	}
	if _, err := Blob(nil, "")(ctx, "s", vfs.Binding{}); err == nil {
		t.Fatal("Blob accepted a nil client")
	}
	if _, err := NewGoogleDrive(ctx, nil); err == nil {
		t.Fatal("NewGoogleDrive accepted a nil holder")
	}
	if _, err := NewGoogleDocs(ctx, nil); err == nil {
		t.Fatal("NewGoogleDocs accepted a nil holder")
	}
	if _, err := NewGoogleSheets(ctx, nil); err == nil {
		t.Fatal("NewGoogleSheets accepted a nil holder")
	}
	api, err := NewGraph(nil, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = Graph(api, nil, "")
	_ = AWSS3{}
	_ = AzureBlob{}
}
