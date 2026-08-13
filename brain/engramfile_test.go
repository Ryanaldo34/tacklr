package brain_test

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestParseFormatEngram_roundTripFieldsAndBody(t *testing.T) {
	id := uuid.MustParse("7c2a4e90-1111-2222-3333-444455556666")
	src := brain.EngramFile{
		ID:    id,
		Kind:  "Deal",
		Slug:  "acme",
		Title: "Acme renewal",
		Properties: map[string]any{
			"amount": 120000,
			"stage":  "open",
		},
		Body: "Narrative body.\n\n## Risks\n\nPenalty clause.\n",
	}
	raw, err := brain.FormatEngram(src)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	if !strings.HasPrefix(text, "---\n") || !strings.Contains(text, "id: "+id.String()) {
		t.Fatalf("front matter:\n%s", text)
	}
	if !strings.Contains(text, "domain: Deal") || !strings.Contains(text, "slug: acme") {
		t.Fatalf("reserved keys:\n%s", text)
	}
	// Deterministic key order: id, domain, slug, title, then amount, stage.
	idAt := strings.Index(text, "id:")
	domainAt := strings.Index(text, "domain:")
	amountAt := strings.Index(text, "amount:")
	stageAt := strings.Index(text, "stage:")
	if !(idAt < domainAt && domainAt < amountAt && amountAt < stageAt) {
		t.Fatalf("key order:\n%s", text)
	}
	raw2, err := brain.FormatEngram(src)
	if err != nil || string(raw2) != text {
		t.Fatal("format must be deterministic")
	}

	got, err := brain.ParseEngram(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != id || got.Kind != "Deal" || got.Slug != "acme" || got.Title != "Acme renewal" {
		t.Fatalf("header: %+v", got)
	}
	if got.Properties["stage"] != "open" {
		t.Fatalf("props: %+v", got.Properties)
	}
	if !strings.Contains(got.Body, "Narrative body.") || !strings.Contains(got.Body, "Penalty clause.") {
		t.Fatalf("body: %q", got.Body)
	}

	obj := brain.ObjectFromEngram(got)
	if obj.Content != got.Body || obj.Properties[brain.PropSlug] != "acme" || obj.ContentType != "text/markdown" {
		t.Fatalf("object: %+v", obj)
	}
	back := brain.EngramFromObject(obj)
	if back.Slug != "acme" || back.Kind != "Deal" {
		t.Fatalf("from object: %+v", back)
	}
}

func TestParseEngram_bodyMayContainFence(t *testing.T) {
	raw := []byte("---\ndomain: Note\nslug: n\n---\n\nSee ---\nmore.\n")
	got, err := brain.ParseEngram(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Body, "See ---") {
		t.Fatalf("body lost inner fence: %q", got.Body)
	}
}

func TestValidateObject_engramReservedAndOpenCatalog(t *testing.T) {
	cat := mustCatalog(t, brain.KindSpec{
		Kind: "Deal", IsParent: true,
		Fields: []brain.FieldSpec{
			{Name: "stage", Type: brain.FieldTypeString, Required: true},
		},
	})
	ns := uuid.New()
	ok := brain.Object{
		Kind: "Deal", NamespaceID: ns,
		Properties: map[string]any{
			"stage":    "open",
			"slug":     "acme",
			"vfs_path": "/engram/deal/acme.md",
		},
	}
	if err := brain.ValidateObject(ok, cat); err != nil {
		t.Fatal(err)
	}

	open := brain.Object{
		Kind: "Deal", NamespaceID: ns,
		Properties: map[string]any{"anything": "goes"},
	}
	if err := brain.ValidateObject(open, nil); err != nil {
		t.Fatal(err)
	}
}

func mustCatalog(t *testing.T, specs ...brain.KindSpec) *brain.KindCatalog {
	t.Helper()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithKinds(specs...))
	if err != nil {
		t.Fatal(err)
	}
	return eng.Catalog()
}
