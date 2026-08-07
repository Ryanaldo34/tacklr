package agentbench

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

// ApplySeed puts objects and links under scope. Fills nil IDs with new UUIDs in-place
// on a copy of the seed so gold ids in Cases that set fixed UUIDs remain stable.
func ApplySeed(ctx context.Context, eng *brain.Engine, scope brain.Scope, world SeedWorld) error {
	idMap := make(map[uuid.UUID]uuid.UUID) // old→new when we rewrite; usually identity
	for _, o := range world.Objects {
		id := o.ID
		if id == uuid.Nil {
			id = uuid.New()
		}
		idMap[o.ID] = id
		if o.ID == uuid.Nil {
			idMap[id] = id
		}
		obj := brain.Object{
			ID:          id,
			Kind:        o.Kind,
			Title:       o.Title,
			Summary:     o.Summary,
			Content:     o.Content,
			Properties:  o.Props,
			ParentID:    o.ParentID,
			Position:    o.Position,
			NamespaceID: uuid.Nil,
		}
		if scope.Namespace != nil {
			obj.NamespaceID = *scope.Namespace
		}
		if o.ParentID != nil {
			if mapped, ok := idMap[*o.ParentID]; ok {
				pid := mapped
				obj.ParentID = &pid
			}
		}
		if _, err := eng.Put(ctx, scope, obj); err != nil {
			return fmt.Errorf("seed put %s %q: %w", o.Kind, o.Title, err)
		}
	}
	for _, e := range world.Edges {
		from, to := e.From, e.To
		if m, ok := idMap[from]; ok {
			from = m
		}
		if m, ok := idMap[to]; ok {
			to = m
		}
		meta := brain.EdgeMeta{Note: e.Note}
		if err := eng.LinkWith(ctx, scope, from, to, e.Relation, meta); err != nil {
			return fmt.Errorf("seed link %s: %w", e.Relation, err)
		}
	}
	return nil
}

// fixed IDs for multihop evidence gold (stable across runs).
var (
	IDProjectOrion = uuid.MustParse("11111111-1111-1111-1111-111111111101")
	IDPersonAlex   = uuid.MustParse("11111111-1111-1111-1111-111111111102")
	IDPersonSam    = uuid.MustParse("11111111-1111-1111-1111-111111111103")
	IDNoteAsync    = uuid.MustParse("11111111-1111-1111-1111-111111111104")
	IDNoteLegal    = uuid.MustParse("11111111-1111-1111-1111-111111111105")
	IDNoteNoise    = uuid.MustParse("11111111-1111-1111-1111-111111111106")
	IDChunkLegal   = uuid.MustParse("11111111-1111-1111-1111-111111111107")
)

// WorldMeetingPrep is a small “notes app” graph for multihop / domain cases.
func WorldMeetingPrep() SeedWorld {
	pos := 1
	parent := IDNoteLegal
	return SeedWorld{
		Objects: []SeedObject{
			{ID: IDProjectOrion, Kind: "Project", Title: "Website redesign Orion", Summary: "Q3 website redesign project", Content: "Launch target end of month. Owner coordination required."},
			{ID: IDPersonAlex, Kind: "Person", Title: "Alex Chen", Summary: "Engineering manager", Content: "Prefers async written updates over status meetings."},
			{ID: IDPersonSam, Kind: "Person", Title: "Sam Rivera", Summary: "Product designer", Content: "Owns marketing pages copy review."},
			{ID: IDNoteAsync, Kind: "Document", Title: "1:1 notes with Alex", Summary: "Manager preferences", Content: "Alex asked for async updates in Slack threads instead of weekly status meetings."},
			{ID: IDNoteLegal, Kind: "Document", Title: "Orion legal blocker", Summary: "Copy blocked on legal", Content: "Website redesign Orion copy is blocked on legal review until Friday."},
			{ID: IDChunkLegal, Kind: "Chunk", Title: "legal detail", Content: "Counsel needs trademark checklist before homepage launch for Orion.", ParentID: &parent, Position: &pos},
			{ID: IDNoteNoise, Kind: "Document", Title: "Office snacks poll", Summary: "Unrelated", Content: "Team voted for better coffee beans next quarter."},
		},
		Edges: []SeedEdge{
			{From: IDNoteAsync, To: IDPersonAlex, Relation: "about", Note: "manager preference note"},
			{From: IDNoteLegal, To: IDProjectOrion, Relation: "about", Note: "project risk"},
			{From: IDPersonSam, To: IDProjectOrion, Relation: "works_on", Note: "designer on Orion"},
			{From: IDPersonAlex, To: IDProjectOrion, Relation: "owns", Note: "EM for Orion"},
		},
	}
}
