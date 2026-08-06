package brain_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

func TestLandingIDs_promotesParents(t *testing.T) {
	parent := uuid.New()
	part := uuid.New()
	other := uuid.New()
	objs := []brain.RichObject{
		{ID: part, ParentID: &parent},
		{ID: other},
		{ID: uuid.New(), ParentID: &parent}, // same parent again
	}
	got := brain.LandingIDs(objs)
	if len(got) != 2 {
		t.Fatalf("want 2 landings, got %v", got)
	}
	if got[0] != parent || got[1] != other {
		t.Fatalf("order/ids: %v", got)
	}
	page := brain.SearchPage{Objects: objs}
	if len(brain.LandingIDsFromPage(page)) != 2 {
		t.Fatal("LandingIDsFromPage")
	}
}
