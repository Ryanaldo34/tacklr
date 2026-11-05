package brain_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

// TestKindSpecNormalize_rejectsInvalidFields: a kind with no name, a path in
// its name, or an invalid field is rejected with the reason.
func TestKindSpecNormalize_rejectsInvalidFields(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		spec brain.KindSpec
		want string
	}{
		{"empty kind", brain.KindSpec{}, "kind is required"},
		{"slash in kind", brain.KindSpec{Kind: "Deal/Person"}, "'/' or '..'"},
		{"dotdot in kind", brain.KindSpec{Kind: "Deal..x"}, "'/' or '..'"},
		{"core key collision", brain.KindSpec{
			Kind:   "Document",
			Fields: []brain.FieldSpec{{Name: "created_after", Type: brain.FieldTypeString}},
		}, "core filter"},
		{"object column collision", brain.KindSpec{
			Kind:   "Document",
			Fields: []brain.FieldSpec{{Name: "content", Type: brain.FieldTypeString}},
		}, "object column"},
		{"enum on number", brain.KindSpec{
			Kind:   "Document",
			Fields: []brain.FieldSpec{{Name: "stage", Type: brain.FieldTypeNumber, Enum: []string{"1"}}},
		}, "enum is only valid on string"},
		{"empty enum value", brain.KindSpec{
			Kind:   "Document",
			Fields: []brain.FieldSpec{{Name: "stage", Type: brain.FieldTypeString, Enum: []string{"open", " "}}},
		}, "enum value is required"},
		{"duplicate field", brain.KindSpec{
			Kind: "Document",
			Fields: []brain.FieldSpec{
				{Name: "stage", Type: brain.FieldTypeString},
				{Name: "stage", Type: brain.FieldTypeNumber},
			},
		}, "duplicate"},
		{"invalid property key", brain.KindSpec{
			Kind:   "Document",
			Fields: []brain.FieldSpec{{Name: "bad-key!", Type: brain.FieldTypeString}},
		}, "not a valid property key"},
		{"unknown type", brain.KindSpec{
			Kind:   "Document",
			Fields: []brain.FieldSpec{{Name: "stage", Type: "uuid"}},
		}, "unknown type"},
		{"field with no name", brain.KindSpec{
			Kind:   "Document",
			Fields: []brain.FieldSpec{{Name: " ", Type: brain.FieldTypeString}},
		}, "name is required"},
		{"field with no type", brain.KindSpec{
			Kind:   "Document",
			Fields: []brain.FieldSpec{{Name: "stage"}},
		}, "type is required"},
		{"duplicate enum value", brain.KindSpec{
			Kind:   "Document",
			Fields: []brain.FieldSpec{{Name: "stage", Type: brain.FieldTypeString, Enum: []string{"open", "open"}}},
		}, "duplicate enum value"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := tc.spec.Normalize()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want containing %q", err, tc.want)
			}
		})
	}
}

// TestFilterValidateAgainst_catalogRules: under a catalog, a filter is valid
// only when it names registered kinds and fields of the right type; each case
// is one return path.
func TestFilterValidateAgainst_catalogRules(t *testing.T) {
	t.Parallel()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithKinds(
		brain.KindSpec{
			Kind: "Document", IsParent: true,
			Fields: []brain.FieldSpec{
				{Name: "stage", Type: brain.FieldTypeString},
				{Name: "amount", Type: brain.FieldTypeNumber},
				{Name: "signed", Type: brain.FieldTypeBoolean},
				{Name: "due", Type: brain.FieldTypeDateTime},
			},
		},
		brain.KindSpec{
			Kind: "Deal", IsParent: true,
			Fields: []brain.FieldSpec{{Name: "amount", Type: brain.FieldTypeNumber}},
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	cat := eng.Catalog()

	// Each case is a distinct return path under a non-empty catalog.
	cases := []struct {
		name    string
		filters brain.Filter
		wantErr string // empty ⇒ expect success
	}{
		{"property requires kind", storetest.Filter(t, map[string]any{"stage": "open"}), "filter \"stage\" needs a kind; kinds: Deal, Document"},
		{"unregistered kind", storetest.Filter(t, map[string]any{"kind": "Orphan"}), "unknown kind \"Orphan\"; kinds: Deal, Document"},
		{"unknown property", storetest.Filter(t, map[string]any{"kind": "Document", "unknown": "x"}), "unknown filter \"unknown\" for Document; filterable: amount, due, signed, stage"},
		{"wrong type", storetest.Filter(t, map[string]any{"kind": "Document", "stage": 1}), "want string"},
		{"a number field given text", storetest.Filter(t, map[string]any{"kind": "Document", "amount": "many"}), "want number"},
		{"a boolean field given text", storetest.Filter(t, map[string]any{"kind": "Document", "signed": "yes"}), "want boolean"},
		{"a boolean field given a list", storetest.Filter(t, map[string]any{"kind": "Document", "signed": []any{true}}), "list values"},
		{"a datetime field given a number", storetest.Filter(t, map[string]any{"kind": "Document", "due": 5}), "want datetime"},
		{"a datetime field given an empty string", storetest.Filter(t, map[string]any{"kind": "Document", "due": ""}), "empty time"},
		{"a list item of the wrong type", storetest.Filter(t, map[string]any{"kind": "Document", "stage": []any{"open", 2}}), "list[1]"},
		{"valid datetime and boolean", storetest.Filter(t, map[string]any{"kind": "Document", "due": "2026-01-02", "signed": true}), ""},
		{"kind list intersection", storetest.Filter(t, map[string]any{"kind": []any{"Document", "Deal"}, "stage": "open"}), "unknown filter \"stage\" for Deal; filterable: amount"},
		{"valid single kind", storetest.Filter(t, map[string]any{"kind": "Document", "stage": "open"}), ""},
		{"valid list filter", storetest.Filter(t, map[string]any{"kind": "Document", "stage": []any{"open", "closed"}}), ""},
		{"valid shared field on one kind", storetest.Filter(t, map[string]any{"kind": "Deal", "amount": 42}), ""},
		{"valid multi-kind shared field", storetest.Filter(t, map[string]any{"kind": []any{"Document", "Deal"}, "amount": 1.5}), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.filters.ValidateAgainst(cat)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, brain.ErrInvalid) || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %v, want ErrInvalid containing %q", err, tc.wantErr)
			}
		})
	}
}

// TestSearch_catalogRestrictsKindsAndFreezesIt: under a catalog, a search
// returns exactly the registered record whose part matches, not the record of
// an unregistered kind with the same words, and the first search freezes the
// catalog against more migrations.
func TestSearch_catalogRestrictsKindsAndFreezesIt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := brain.NewMemoryStore()
	scope := brain.Scope{Namespace: tenant(t)}
	docID, orphanID := uuid.New(), uuid.New()
	for _, obj := range []brain.Object{
		{ID: docID, Kind: "Document", Title: "Deal memo"},
		{ID: uuid.New(), Kind: "Chunk", Content: "negotiation terms", ParentID: &docID, Position: new(1)},
		{ID: orphanID, Kind: "OrphanKind", Title: "orphan parent"},
		{ID: uuid.New(), Kind: "OrphanKind", Content: "negotiation terms", ParentID: &orphanID, Position: new(1)},
	} {
		obj.Namespace = scope.Namespace
		if err := storetest.Put(store, obj); err != nil {
			t.Fatal(err)
		}
	}
	eng, err := brain.NewEngine(store, brain.WithKinds(
		brain.KindSpec{Kind: "Document", IsParent: true},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	))
	if err != nil {
		t.Fatal(err)
	}

	page, searchErr := eng.Search(ctx, scope, brain.SearchRequest{Query: "negotiation"})
	applyErr := eng.ApplyKinds(ctx, brain.KindSpec{Kind: "Deal", IsParent: true})

	if got := slices.Collect(page.Results.Objects()); searchErr != nil || !slices.Equal(idsOf(got), []uuid.UUID{docID}) {
		t.Errorf("search under a catalog = %v %v, want the Document alone, through its Chunk", titlesOf(got), searchErr)
	}
	if !errors.Is(applyErr, brain.ErrInvalid) || !strings.Contains(applyErr.Error(), "frozen") {
		t.Errorf("apply kinds after a search = %v, want the frozen catalog error", applyErr)
	}
}

// TestApplyKinds_migrationAddAndModify: each migration replaces the catalog
// and the durable kinds, a new engine loads them from the store, PersistKinds
// and SyncKindsToStore write kinds, and a search freezes the catalog.
func TestApplyKinds_migrationAddAndModify(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}

	// V1 migration: Document + Chunk
	if err := eng.ApplyKinds(ctx,
		brain.KindSpec{
			Kind: "Document", IsParent: true, Description: "docs v1",
			Fields: []brain.FieldSpec{{Name: "stage", Type: brain.FieldTypeString}},
		},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	); err != nil {
		t.Fatal(err)
	}
	if eng.Catalog().Empty() || len(eng.Catalog().Names()) != 2 {
		t.Fatalf("catalog after v1: %v", eng.Catalog().Names())
	}
	row, err := storetest.Kind(ctx, store, "Document")
	if err != nil || row.Description != "docs v1" {
		t.Fatalf("durable v1: %+v err=%v", row, err)
	}

	// V2 migration: modify Document fields, add Deal (desired process set)
	if err := eng.ApplyKinds(ctx,
		brain.KindSpec{
			Kind: "Document", IsParent: true, Description: "docs v2",
			Fields: []brain.FieldSpec{
				{Name: "stage", Type: brain.FieldTypeString},
				{Name: "amount", Type: brain.FieldTypeNumber},
			},
		},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
		brain.KindSpec{Kind: "Deal", IsParent: true, Fields: []brain.FieldSpec{
			{Name: "stage", Type: brain.FieldTypeString},
		}},
	); err != nil {
		t.Fatal(err)
	}
	doc, ok := eng.Catalog().Get("Document")
	if !ok || doc.Description != "docs v2" || len(doc.Fields) != 2 {
		t.Fatalf("catalog document v2: %+v", doc)
	}
	if _, ok := eng.Catalog().Get("Deal"); !ok {
		t.Fatal("deal missing from catalog")
	}
	// Process catalog is exactly the apply set (not a merge of history).
	if len(eng.Catalog().Names()) != 3 {
		t.Fatalf("names: %v", eng.Catalog().Names())
	}
	row, err = storetest.Kind(ctx, store, "Document")
	if err != nil || row.Description != "docs v2" {
		t.Fatalf("durable v2: %+v err=%v", row, err)
	}

	// New process adopts durable state via LoadKindsFromStore.
	eng2, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng2.LoadKindsFromStore(ctx); err != nil {
		t.Fatal(err)
	}
	spec, ok := eng2.Catalog().Get("Document")
	if !ok || len(spec.Fields) != 2 || spec.Description != "docs v2" {
		t.Fatalf("loaded: %+v ok=%v", spec, ok)
	}

	// PersistKinds works without Engine (custom bootstrap).
	if err := brain.PersistKinds(ctx, store, brain.KindSpec{
		Kind: "Note", IsParent: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := storetest.Kind(ctx, store, "Note"); err != nil {
		t.Fatal(err)
	}

	// SyncKindsToStore flushes WithKinds catalog to a durable writer.
	mem := brain.NewMemoryStore()
	engSync, err := brain.NewEngine(mem, brain.WithKinds(
		brain.KindSpec{Kind: "Synced", IsParent: true, Description: "via sync"},
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := engSync.SyncKindsToStore(ctx); err != nil {
		t.Fatal(err)
	}
	row, err = storetest.Kind(ctx, mem, "Synced")
	if err != nil || row.Description != "via sync" {
		t.Fatalf("sync: %+v err=%v", row, err)
	}

	// A search freezes the catalog: loading kinds again fails.
	if _, err := eng2.Search(ctx, brain.Scope{Namespace: storetest.Namespace(t, "org", "freeze")}, brain.SearchRequest{Query: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := eng2.LoadKindsFromStore(ctx); err == nil {
		t.Fatal("want frozen load error")
	}
}

// TestApplyKinds_invalidBatchFailsClosed: a migration with an invalid or a
// repeated kind changes neither the catalog nor the store, PersistKinds
// refuses an invalid kind, a store refuses a kind with no name, and loading
// a stored kind that is invalid or has fields that do not parse fails and
// keeps the catalog.
func TestApplyKinds_invalidBatchFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	loadStored := func(row brain.ObjectKind) error {
		s := brain.NewMemoryStore()
		if err := s.PutKind(ctx, row); err != nil {
			t.Fatal(err)
		}
		e, err := brain.NewEngine(s)
		if err != nil {
			t.Fatal(err)
		}
		err = e.LoadKindsFromStore(ctx)
		if !e.Catalog().Empty() {
			t.Errorf("catalog after a failed load of %+v = %v, want it empty", row, e.Catalog().Names())
		}
		return err
	}

	invalidErr := eng.ApplyKinds(ctx, brain.KindSpec{Kind: "Document", IsParent: true}, brain.KindSpec{})
	repeatedErr := eng.ApplyKinds(ctx, brain.KindSpec{Kind: "Document"}, brain.KindSpec{Kind: "Document"})
	persistErr := brain.PersistKinds(ctx, store, brain.KindSpec{Kind: "Deal/x"})
	nameErr := store.PutKind(ctx, brain.ObjectKind{})
	badNameErr := loadStored(brain.ObjectKind{Kind: "Deal/x"})
	badFieldsErr := loadStored(brain.ObjectKind{Kind: "Deal", FilterableFields: []byte(`{"not":"a list"}`)})

	_, storedErr := storetest.Kind(ctx, store, "Document")
	if invalidErr == nil || !strings.Contains(repeatedErr.Error(), "duplicate kind") || !eng.Catalog().Empty() || !errors.Is(storedErr, brain.ErrNotFound) {
		t.Errorf("invalid batch %v, repeated kind %v, then catalog %v and stored %v, want both refused and nothing written", invalidErr, repeatedErr, eng.Catalog().Names(), storedErr)
	}
	if persistErr == nil || nameErr == nil {
		t.Errorf("persist an invalid kind %v, store a kind with no name %v, want both refused", persistErr, nameErr)
	}
	if badNameErr == nil || badFieldsErr == nil || !strings.Contains(badFieldsErr.Error(), "filterable_fields") {
		t.Errorf("load a stored kind with a bad name %v, with fields that do not parse %v, want both to fail", badNameErr, badFieldsErr)
	}
}
