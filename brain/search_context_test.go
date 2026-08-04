package brain

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSearchContext_putGetExportRestore(t *testing.T) {
	ctx := context.Background()
	sc := NewSearchContext()

	if err := sc.Put(ctx, ResultSet{}); err == nil {
		t.Fatal("nil id")
	}
	id := uuid.New()
	ids := []uuid.UUID{uuid.New(), uuid.New()}
	if err := sc.Put(ctx, ResultSet{ID: id, ObjectIDs: ids, Offset: 1}); err != nil {
		t.Fatal(err)
	}
	got, err := sc.Get(ctx, id)
	if err != nil || got.Offset != 1 || len(got.ObjectIDs) != 2 {
		t.Fatalf("%+v %v", got, err)
	}
	// Isolation: mutating returned slice must not clobber store.
	got.ObjectIDs[0] = uuid.Nil
	got2, _ := sc.Get(ctx, id)
	if got2.ObjectIDs[0] == uuid.Nil {
		t.Fatal("store mutated")
	}

	raw, err := sc.Export()
	if err != nil || len(raw) == 0 {
		t.Fatalf("export %v %s", err, raw)
	}
	sc2 := NewSearchContext()
	if err := sc2.Restore(raw); err != nil {
		t.Fatal(err)
	}
	got3, err := sc2.Get(ctx, id)
	if err != nil || len(got3.ObjectIDs) != 2 {
		t.Fatalf("restore: %+v %v", got3, err)
	}
	if err := sc2.Restore(nil); err != nil {
		t.Fatal(err)
	}
	if _, err := sc2.Get(ctx, id); err == nil {
		t.Fatal("cleared")
	}
	if err := sc2.Restore([]byte(`{"id":"00000000-0000-0000-0000-000000000000"}`)); err != nil {
		t.Fatal(err)
	}
	if err := sc2.Restore([]byte(`{`)); err == nil {
		t.Fatal("bad json")
	}
	// CreatedAt filled when zero.
	id2 := uuid.New()
	_ = sc.Put(ctx, ResultSet{ID: id2, CreatedAt: time.Time{}})
	g, _ := sc.Get(ctx, id2)
	if g.CreatedAt.IsZero() {
		t.Fatal("CreatedAt")
	}
	empty, err := NewSearchContext().Export()
	if err != nil || empty != nil {
		t.Fatalf("empty export: %v %v", empty, err)
	}

	// Namespace is part of the retrieval session surface.
	ns := uuid.New()
	sc.SetNamespace(ns)
	gotNS, ok := sc.Namespace()
	if !ok || gotNS != ns {
		t.Fatal("namespace")
	}
	rawNS, err := sc.Export()
	if err != nil || len(rawNS) == 0 {
		t.Fatal(err)
	}
	sc3 := NewSearchContext()
	if err := sc3.Restore(rawNS); err != nil {
		t.Fatal(err)
	}
	gotNS, ok = sc3.Namespace()
	if !ok || gotNS != ns {
		t.Fatal("restore namespace")
	}
	sc3.ClearNamespace()
	if _, ok := sc3.Namespace(); ok {
		t.Fatal("clear")
	}
	// Legacy ResultSet-only JSON still restores.
	legacy, _ := json.Marshal(ResultSet{ID: id, ObjectIDs: ids, Offset: 2})
	sc4 := NewSearchContext()
	if err := sc4.Restore(legacy); err != nil {
		t.Fatal(err)
	}
	got4, err := sc4.Get(ctx, id)
	if err != nil || got4.Offset != 2 {
		t.Fatalf("%+v %v", got4, err)
	}
}
