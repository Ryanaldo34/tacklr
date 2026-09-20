package server

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryWireStore_putGetDelete(t *testing.T) {
	w := NewMemoryWireStore()
	if _, err := w.Get(context.Background(), "missing"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing get: %v", err)
	}
	if err := w.Put(context.Background(), "k", []byte(`{"cwd":"/proj"}`)); err != nil {
		t.Fatal(err)
	}
	got, err := w.Get(context.Background(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"cwd":"/proj"}` {
		t.Fatalf("get = %s", got)
	}
	if err := w.Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Get(context.Background(), "k"); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("after delete: %v", err)
	}
}
