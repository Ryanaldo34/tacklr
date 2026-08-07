package brain

import (
	"testing"
	"time"
)

func TestSortRichObjects(t *testing.T) {
	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	pos0, pos1 := 0, 1
	objs := []RichObject{
		{Title: "b", UpdatedAt: t1, CreatedAt: t2, Position: &pos1, Properties: map[string]any{"stage": "z"}},
		{Title: "a", UpdatedAt: t2, CreatedAt: t1, Position: &pos0, Properties: map[string]any{"stage": "a"}},
	}
	SortRichObjects(objs, "title", false)
	if objs[0].Title != "a" {
		t.Fatalf("title asc: %+v", objs)
	}
	SortRichObjects(objs, "updated_at", true)
	if objs[0].UpdatedAt != t2 {
		t.Fatalf("updated_at desc: %+v", objs)
	}
	SortRichObjects(objs, "position", false)
	if objs[0].Position == nil || *objs[0].Position != 0 {
		t.Fatalf("position: %+v", objs)
	}
	SortRichObjects(objs, "stage", false)
	if objs[0].Properties["stage"] != "a" {
		t.Fatalf("prop: %+v", objs)
	}
	// no-ops
	SortRichObjects(objs[:1], "title", false)
	SortRichObjects(objs, "", false)
}
