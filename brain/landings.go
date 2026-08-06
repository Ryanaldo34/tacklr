package brain

import "github.com/google/uuid"

// LandingIDs returns unique first-class object ids suitable for graph expand / link
// endpoints from rich hits (search, find_exact, find_objects).
// Parts use ParentID; parents use their own ID. Nil / empty parent pointers are skipped.
//
// Use after corpus search so Phase 1 can land on chunks while Phase 2 expands from
// the dual-written parent entity on Helix.
func LandingIDs(objects []RichObject) []uuid.UUID {
	if len(objects) == 0 {
		return nil
	}
	out := make([]uuid.UUID, 0, len(objects))
	seen := make(map[uuid.UUID]struct{}, len(objects))
	for _, o := range objects {
		id := o.ID
		if o.ParentID != nil {
			if *o.ParentID == uuid.Nil {
				continue
			}
			id = *o.ParentID
		}
		if id == uuid.Nil {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// LandingIDsFromPage is LandingIDs(page.Objects).
func LandingIDsFromPage(page SearchPage) []uuid.UUID {
	return LandingIDs(page.Objects)
}
