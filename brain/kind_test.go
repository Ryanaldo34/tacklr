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
		filters brain.Filter
		wantErr string // empty ⇒ expect success
	}{
		{"property requires kind", mustFilter(t, map[string]any{"stage": "open"}), "require a kind"},
		{"unregistered kind", mustFilter(t, map[string]any{"kind": "Orphan"}), "not registered"},
		{"unknown property", mustFilter(t, map[string]any{"kind": "Document", "unknown": "x"}), "not filterable"},
		{"wrong type", mustFilter(t, map[string]any{"kind": "Document", "stage": 1}), "want string"},
		{"kind list intersection", mustFilter(t, map[string]any{"kind": []any{"Document", "Deal"}, "stage": "open"}), "not filterable"},
		{"valid single kind", mustFilter(t, map[string]any{"kind": "Document", "stage": "open"}), ""},
		{"valid list filter", mustFilter(t, map[string]any{"kind": "Document", "stage": []any{"open", "closed"}}), ""},
		{"valid shared field on one kind", mustFilter(t, map[string]any{"kind": "Deal", "amount": 42}), ""},
		{"valid multi-kind shared field", mustFilter(t, map[string]any{"kind": []any{"Document", "Deal"}, "amount": 1.5}), ""},
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
	if err := brain.ValidateFiltersAgainst(mustFilter(t, map[string]any{"stage": "open"}), eng.Catalog()); err != nil {
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
	// Agents must see which tools share filterable_fields (incl. find_objects).
	if len(one.FilterUsage.Tools) < 3 || !strings.Contains(strings.Join(one.FilterUsage.Tools, ","), "find_objects") {
		t.Fatalf("filter_usage.tools: %+v", one.FilterUsage)
	}
	if !strings.Contains(one.FilterUsage.Note, "filterable_fields") || !strings.Contains(one.FilterUsage.Note, "columns") {
		t.Fatalf("filter_usage.note: %s", one.FilterUsage.Note)
	}
	if got := one.Kinds[0].Columns; len(got) != 3 || got[0].Name != "title" || !got[0].Filter || got[1].Name != "summary" || got[2].Name != "content" {
		t.Fatalf("columns: %+v", got)
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
	ns := mustNS(t, "id", uuid.NewString())
	now := time.Now().UTC()
	docID := uuid.New()
	orphanParent := uuid.New()
	pos := 1

	if err := store.Put(context.Background(), brain.Object{
		ID: docID, Kind: "Document", Title: "Deal memo", Namespace: ns, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), brain.Object{
		ID: uuid.New(), Kind: "Chunk", Title: "chunk", Content: "negotiation terms",
		Namespace: ns, UpdatedAt: now, ParentID: &docID, Position: &pos,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), brain.Object{
		ID: orphanParent, Kind: "OrphanKind", Title: "orphan parent", Namespace: ns, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(context.Background(), brain.Object{
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
	sc := brain.NewSearchContext()
	scope := brain.Scope{Namespace: ns}

	page, err := eng.Search(ctx, scope, brain.SearchRequest{
		Query: "negotiation", Filters: mustFilter(t, map[string]any{"kind": "Chunk"}),
	}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != docID {
		t.Fatalf("want Document parent via Chunk hit, got %+v", page.Objects)
	}

	// Orphan-only phrase: catalog allow-list yields no hits (only registered kinds searched).
	page, err = eng.Search(ctx, scope, brain.SearchRequest{Query: "zzzxorphanonlyphrase"}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 0 {
		t.Fatalf("want no hits for orphan-only content under catalog, got %+v", page.Objects)
	}

	// Successful search freezes the catalog; further RegisterKinds fails.
	if err := eng.RegisterKinds(ctx, brain.KindSpec{Kind: "Deal", IsParent: true}); err == nil {
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
	row, err := store.GetKind(ctx, "Document")
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
	row, err = store.GetKind(ctx, "Document")
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
	if _, err := store.GetKind(ctx, "Note"); err != nil {
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
	row, err = mem.GetKind(ctx, "Synced")
	if err != nil || row.Description != "via sync" {
		t.Fatalf("sync: %+v err=%v", row, err)
	}

	eng2.FreezeCatalog()
	if err := eng2.ApplyKinds(ctx, brain.KindSpec{Kind: "X", IsParent: true}); err == nil {
		t.Fatal("want frozen apply error")
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
	if _, err := store.GetKind(ctx, "Document"); !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("no partial durable write on batch validate fail: %v", err)
	}
}

func TestWithKinds_invalidFailsNewEngine(t *testing.T) {
	_, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(brain.KindSpec{}))
	if err == nil {
		t.Fatal("want error")
	}
}

func TestObjectKindFromSpec_roundTrip(t *testing.T) {
	row, err := brain.ObjectKindFromSpec(brain.KindSpec{
		Kind: "Doc", IsParent: true, Description: "d",
		Fields: []brain.FieldSpec{{Name: "stage", Type: brain.FieldTypeString}},
	})
	if err != nil || row.Kind != "Doc" || !row.IsParent {
		t.Fatalf("%+v err=%v", row, err)
	}
	spec, err := brain.KindSpecFromObjectKind(row)
	if err != nil || len(spec.Fields) != 1 || spec.Fields[0].Name != "stage" {
		t.Fatalf("%+v err=%v", spec, err)
	}
}
