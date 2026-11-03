package brain_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
)

func TestKindSpecNormalize_rejectsInvalidFields(t *testing.T) {
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.spec.Normalize()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestFilterValidateAgainst_catalogRules(t *testing.T) {
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

func TestOpen_takesTheKindFromTheCatalogOverTheStore(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	// Store has different description; catalog wins when non-empty.
	_ = store.PutKind(ctx, brain.ObjectKind{Kind: "Document", Description: "store copy", IsParent: true})
	eng, err := brain.NewEngine(store, brain.WithKinds(
		brain.KindSpec{
			Kind: "Document", Description: "catalog copy", IsParent: true,
			Fields: []brain.FieldSpec{{Name: "stage", Type: brain.FieldTypeString}},
		},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	))
	if err != nil {
		t.Fatal(err)
	}

	scope := brain.Scope{Namespace: tenant(t)}
	doc := save(t, eng, scope, brain.Object{Kind: "Document", Title: "d"})
	one, err := eng.Open(ctx, scope, doc.ID)
	if err != nil || one.Kind == nil || one.Kind.Description != "catalog copy" {
		t.Fatalf("catalog should win: %+v %v", one.Kind, err)
	}
	var fields []brain.FieldSpec
	if err := json.Unmarshal(one.Kind.FilterableFields, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 1 || fields[0].Name != "stage" {
		t.Fatalf("fields: %+v", fields)
	}
}

func TestSearch_catalogRestrictsKindsAndAllowsFilteredHit(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := storetest.Namespace(t, "id", uuid.NewString())
	now := time.Now().UTC()
	docID := uuid.New()
	orphanParent := uuid.New()
	pos := 1

	if err := storetest.Put(store, brain.Object{
		ID: docID, Kind: "Document", Title: "Deal memo", Namespace: ns, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := storetest.Put(store, brain.Object{
		ID: uuid.New(), Kind: "Chunk", Title: "chunk", Content: "negotiation terms",
		Namespace: ns, UpdatedAt: now, ParentID: &docID, Position: &pos,
	}); err != nil {
		t.Fatal(err)
	}
	if err := storetest.Put(store, brain.Object{
		ID: orphanParent, Kind: "OrphanKind", Title: "orphan parent", Namespace: ns, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := storetest.Put(store, brain.Object{
		ID: uuid.New(), Kind: "OrphanKind", Title: "orphan part", Content: "zzzxorphanonlyphrase",
		Namespace: ns, UpdatedAt: now, ParentID: &orphanParent, Position: &pos,
	}); err != nil {
		t.Fatal(err)
	}

	eng, err := brain.NewEngine(store, brain.WithKinds(
		brain.KindSpec{Kind: "Document", IsParent: true},
		brain.KindSpec{Kind: "Chunk", IsPart: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: ns}

	page, err := eng.Search(ctx, scope, brain.SearchRequest{Query: "negotiation", Kind: "Document"})
	if err != nil {
		t.Fatal(err)
	}
	if len(slices.Collect(page.Results.Objects())) != 1 || slices.Collect(page.Results.Objects())[0].ID != docID {
		t.Fatalf("want the Document through its Chunk's hit, got %+v", slices.Collect(page.Results.Objects()))
	}

	// Orphan-only phrase: catalog allow-list yields no hits (only registered kinds searched).
	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "zzzxorphanonlyphrase"})
	if err != nil {
		t.Fatal(err)
	}
	if len(slices.Collect(page.Results.Objects())) != 0 {
		t.Fatalf("want no hits for orphan-only content under catalog, got %+v", slices.Collect(page.Results.Objects()))
	}

	// Successful search freezes the catalog; further ApplyKinds fails.
	if err := eng.ApplyKinds(ctx, brain.KindSpec{Kind: "Deal", IsParent: true}); err == nil {
		t.Fatal("want freeze after first successful search")
	}
}

func TestApplyKinds_migrationAddAndModify(t *testing.T) {
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

func TestApplyKinds_invalidBatchFailsClosed(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	err = eng.ApplyKinds(ctx,
		brain.KindSpec{Kind: "Document", IsParent: true},
		brain.KindSpec{}, // invalid
	)
	if err == nil {
		t.Fatal("want error")
	}
	if !eng.Catalog().Empty() {
		t.Fatal("catalog must stay empty on failed migration")
	}
	if _, err := storetest.Kind(ctx, store, "Document"); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("no partial durable write on batch validate fail: %v", err)
	}
}
