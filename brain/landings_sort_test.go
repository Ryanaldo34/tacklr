package brain_test

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

// TestLandingIDs_partsAndParents: unique landing ids for parts vs parents.
func TestLandingIDs_partsAndParents(t *testing.T) {
	parent := uuid.New()
	part := uuid.New()
	other := uuid.New()
	nilParent := uuid.Nil
	objs := []brain.RichObject{
		{ID: part, ParentID: &parent},
		{ID: uuid.New(), ParentID: &parent}, // same landing
		{ID: other},
		{ID: uuid.New(), ParentID: &nilParent}, // skip nil parent
		{ID: uuid.Nil},                         // skip nil id
	}
	ids := brain.LandingIDs(objs)
	if len(ids) != 2 {
		t.Fatalf("ids=%v want parent+other", ids)
	}
	if ids[0] != parent || ids[1] != other {
		t.Fatalf("order/ids=%v", ids)
	}
	if brain.LandingIDs(nil) != nil {
		t.Fatal("empty")
	}
	pageIDs := brain.LandingIDsFromPage(brain.SearchPage{Objects: objs})
	if len(pageIDs) != 2 {
		t.Fatalf("from page: %v", pageIDs)
	}
}

// TestSortRichObjects_keys: title/position/props/times and desc.
func TestSortRichObjects_keys(t *testing.T) {
	p0, p1, p2 := 0, 1, 2
	now := time.Now().UTC()
	objs := []brain.RichObject{
		{ID: uuid.New(), Title: "b", Position: &p1, UpdatedAt: now.Add(-time.Hour),
			Properties: map[string]any{"score": 2.0, "flag": false, "n": 2}},
		{ID: uuid.New(), Title: "a", Position: &p2, UpdatedAt: now,
			Properties: map[string]any{"score": 10.0, "flag": true, "n": int64(10), "when": now}},
		{ID: uuid.New(), Title: "c", Position: &p0, UpdatedAt: now.Add(-2 * time.Hour),
			Properties: map[string]any{"score": 1.0}},
	}
	// title asc
	cp := append([]brain.RichObject(nil), objs...)
	brain.SortRichObjects(cp, "title", false)
	if cp[0].Title != "a" || cp[2].Title != "c" {
		t.Fatalf("title asc: %v %v %v", cp[0].Title, cp[1].Title, cp[2].Title)
	}
	// title desc
	cp = append([]brain.RichObject(nil), objs...)
	brain.SortRichObjects(cp, "title", true)
	if cp[0].Title != "c" {
		t.Fatalf("title desc: %s", cp[0].Title)
	}
	// position
	cp = append([]brain.RichObject(nil), objs...)
	brain.SortRichObjects(cp, "position", false)
	if *cp[0].Position != 0 || *cp[2].Position != 2 {
		t.Fatalf("position: %v", []*int{cp[0].Position, cp[1].Position, cp[2].Position})
	}
	// updated_at
	cp = append([]brain.RichObject(nil), objs...)
	brain.SortRichObjects(cp, "updated_at", false)
	if !cp[0].UpdatedAt.Before(cp[2].UpdatedAt) {
		t.Fatal("updated_at order")
	}
	// property flag (bool → "0"/"1") — true sorts after false ascending
	cp = append([]brain.RichObject(nil), objs...)
	brain.SortRichObjects(cp, "flag", false)
	// false ("0") before true ("1"); missing flag sorts as ""
	if cp[len(cp)-1].Properties["flag"] != true {
		t.Fatalf("flag asc last should be true: %+v", cp[len(cp)-1].Properties)
	}
	// created_at + empty key no-op
	cp = append([]brain.RichObject(nil), objs...)
	brain.SortRichObjects(cp, "created_at", false)
	brain.SortRichObjects(cp, "", false)
	brain.SortRichObjects(cp[:1], "title", false)
	// nil position sorts first via posOrNeg
	nilPos := []brain.RichObject{{Title: "z"}, {Title: "a", Position: &p1}}
	brain.SortRichObjects(nilPos, "position", false)
	if nilPos[0].Position != nil {
		t.Fatal("nil position should sort as -1 first")
	}
	// prop types
	typed := []brain.RichObject{
		{Properties: map[string]any{"k": "b"}},
		{Properties: map[string]any{"k": true}},
		{Properties: map[string]any{"k": false}},
		{Properties: map[string]any{"k": int(3)}},
		{Properties: map[string]any{"k": int32(2)}},
		{Properties: map[string]any{"k": float32(1.5)}},
		{Properties: map[string]any{"k": time.Now()}},
		{Properties: map[string]any{"k": struct{}{}}},
		{Properties: nil},
	}
	brain.SortRichObjects(typed, "k", false)
}
