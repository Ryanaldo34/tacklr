package brain_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestNormalizeKindSpec_rejectsInvalidFields(t *testing.T) {
	cases := []struct {
		name string
		spec brain.KindSpec
		want string
	}{
		{"empty kind", brain.KindSpec{}, "kind is required"},
		{"core key collision", brain.KindSpec{
			Kind:   "Document",
			Fields: []brain.FieldSpec{{Name: "created_after", Type: brain.FieldTypeString}},
		}, "core filter"},
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
			_, err := brain.NormalizeKindSpec(tc.spec)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestValidateFiltersAgainst_catalogRules(t *testing.T) {
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithKinds(
		brain.KindSpec{
			Kind: "Document", IsParent: true,
			Fields: []brain.FieldSpec{
				{Name: "stage", Type: brain.FieldTypeString},
				{Name: "amount", Type: brain.FieldTypeNumber},
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
		filters brain.Filters
		wantErr string // empty ⇒ expect success
	}{
		{"property requires kind", brain.Filters{"stage": "open"}, "require a kind"},
		{"unregistered kind", brain.Filters{"kind": "Orphan"}, "not registered"},
		{"unknown property", brain.Filters{"kind": "Document", "unknown": "x"}, "not filterable"},
		{"wrong type", brain.Filters{"kind": "Document", "stage": 1}, "wants string"},
		{"kind list intersection", brain.Filters{"kind": []any{"Document", "Deal"}, "stage": "open"}, "not filterable"},
		{"valid single kind", brain.Filters{"kind": "Document", "stage": "open"}, ""},
		{"valid shared field on one kind", brain.Filters{"kind": "Deal", "amount": 42}, ""},
		{"valid multi-kind shared field", brain.Filters{"kind": []any{"Document", "Deal"}, "amount": 1.5}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := brain.ValidateFiltersAgainst(tc.filters, cat)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestEmptyCatalog_openFiltersAndStoreSchema(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	if err := store.PutKind(ctx, brain.ObjectKind{
		Kind: "Document", Description: "from store", IsParent: true,
		FilterableFields: json.RawMessage(`[{"name":"stage","type":"string"}]`),
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if !eng.Catalog().Empty() {
		t.Fatal("catalog should be empty")
	}
	res, err := eng.Schema(ctx, "Document")
	if err != nil || len(res.Kinds) != 1 || res.Kinds[0].Description != "from store" {
		t.Fatalf("store schema: %+v err=%v", res, err)
	}
	if err := brain.ValidateFiltersAgainst(brain.Filters{"stage": "open"}, eng.Catalog()); err != nil {
		t.Fatal(err)
	}
}

func TestSchema_prefersCatalog(t *testing.T) {
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

	all, err := eng.Schema(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all.Kinds) != 2 || all.Kinds[0].Kind != "Chunk" || all.Kinds[1].Kind != "Document" {
		t.Fatalf("schema list: %+v", all.Kinds)
	}
	one, err := eng.Schema(ctx, "Document")
	if err != nil {
		t.Fatal(err)
	}
	if one.Kinds[0].Description != "catalog copy" {
		t.Fatalf("catalog should win: %+v", one)
	}
	var fields []brain.FieldSpec
	if err := json.Unmarshal(one.Kinds[0].FilterableFields, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 1 || fields[0].Name != "stage" {
		t.Fatalf("fields: %+v", fields)
	}
	if _, err := eng.Schema(ctx, "Missing"); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func TestSearch_catalogRestrictsKindsAndAllowsFilteredHit(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	now := time.Now().UTC()
	docID := uuid.New()
	orphanParent := uuid.New()
	pos := 1

	if err := store.Put(brain.Object{
		ID: docID, Kind: "Document", Title: "Deal memo", NamespaceID: ns, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(brain.Object{
		ID: uuid.New(), Kind: "Chunk", Title: "chunk", Content: "negotiation terms",
		NamespaceID: ns, UpdatedAt: now, ParentID: &docID, Position: &pos,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(brain.Object{
		ID: orphanParent, Kind: "OrphanKind", Title: "orphan parent", NamespaceID: ns, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(brain.Object{
		ID: uuid.New(), Kind: "OrphanKind", Title: "orphan part", Content: "negotiation secret",
		NamespaceID: ns, UpdatedAt: now, ParentID: &orphanParent, Position: &pos,
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
	sc := brain.NewSearchContext()
	scope := brain.Scope{Namespace: &ns}

	page, err := eng.Search(ctx, scope, brain.SearchRequest{
		Query: "negotiation", Filters: brain.Filters{"kind": "Chunk"},
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != docID {
		t.Fatalf("want Document parent via Chunk hit, got %+v", page.Objects)
	}

	// Implicit kind allow-list: free-text search must not surface unregistered kinds.
	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "negotiation secret"}, sc)
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range page.Objects {
		if o.Kind == "OrphanKind" || o.ID == orphanParent {
			t.Fatalf("unregistered kind surfaced: %+v", o)
		}
	}

	// Successful search freezes the catalog.
	if err := eng.RegisterKinds(ctx, brain.KindSpec{Kind: "Deal", IsParent: true}); err == nil {
		t.Fatal("want freeze after first successful search")
	}
}

func TestSyncAndLoadKinds_memoryRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	eng, err := brain.NewEngine(store, brain.WithKinds(
		brain.KindSpec{
			Kind: "Deal", IsParent: true,
			Fields: []brain.FieldSpec{{Name: "stage", Type: brain.FieldTypeString}},
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.SyncKindsToStore(ctx); err != nil {
		t.Fatal(err)
	}
	row, err := store.GetKind(ctx, "Deal")
	if err != nil || row.Kind != "Deal" || !row.IsParent {
		t.Fatalf("stored: %+v err=%v", row, err)
	}

	eng2, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := eng2.LoadKindsFromStore(ctx); err != nil {
		t.Fatal(err)
	}
	spec, ok := eng2.Catalog().Get("Deal")
	if !ok || len(spec.Fields) != 1 || spec.Fields[0].Name != "stage" {
		t.Fatalf("loaded: %+v ok=%v", spec, ok)
	}
	eng2.FreezeCatalog()
	if err := eng2.LoadKindsFromStore(ctx); err == nil {
		t.Fatal("want frozen load error")
	}
}

func TestWithKinds_invalidFailsNewEngine(t *testing.T) {
	_, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(brain.KindSpec{}))
	if err == nil {
		t.Fatal("want error")
	}
}
