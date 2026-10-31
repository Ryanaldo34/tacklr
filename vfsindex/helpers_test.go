package vfsindex_test

import (
	"context"
	"slices"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
)

// listChildren pages through id's children, in order.
func listChildren(ctx context.Context, eng *brain.Engine, scope brain.Scope, id uuid.UUID) ([]brain.RichObject, error) {
	page, err := eng.Related(ctx, scope, brain.RelatedRequest{ID: id, Relation: "contains", Limit: 50})
	if err != nil {
		return nil, err
	}
	out := slices.Collect(page.Results.Objects())
	for page.More {
		if page, err = eng.Next(ctx, scope, page.Rest, 50); err != nil {
			return nil, err
		}
		out = append(out, slices.Collect(page.Results.Objects())...)
	}
	return out, nil
}
