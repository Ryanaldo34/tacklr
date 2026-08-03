package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/streaming"
)

// brainTools closes over the engine and SearchContext (namespace + result set).
type brainTools struct {
	engine *brain.Engine
	sc     *brain.SearchContext
}

type readObjectArgs struct {
	ObjectID string `json:"object_id" desc:"UUID of the object to read in full."`
}

func (b brainTools) newReadObjectTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "read",
		DisplayName: "Read Object",
		Description: `Read the full contents of a knowledge-base object by id.

Use after search, find_exact, or expand when you need the complete body (content) of a known object. Pass the object UUID from a prior rich result. Do not invent ids.`,
		Category: streaming.ToolCategoryRead,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args readObjectArgs, runtime HarnessRuntime) (string, error) {
			id, err := parseUUID(args.ObjectID, "object_id")
			if err != nil {
				return "", fmt.Errorf("read: %w", err)
			}
			runtime.EmitUpdate("Reading knowledge object…")
			obj, err := b.engine.Read(ctx, b.sc.Scope(), id)
			if err != nil {
				if errors.Is(err, brain.ErrNotFound) {
					return "", fmt.Errorf("read: object %s not found", id)
				}
				return "", fmt.Errorf("read: %w", err)
			}
			return formatBrainJSON(obj)
		},
	})
}

type schemaArgs struct {
	Kind string `json:"kind,omitempty" desc:"Optional object kind to describe (e.g. Document, Chunk). Omit to list all registered kinds."`
}

func (b brainTools) newSchemaTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "schema",
		DisplayName: "Knowledge Schema",
		Description: `Discover structured filter fields and kind documentation for the knowledge base.

Call with a kind to see filterable fields, types, and operators for that kind. Call with no kind to list registered kinds. Prefer this before inventing property names in filters.`,
		Category: streaming.ToolCategoryThink,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args schemaArgs, runtime HarnessRuntime) (string, error) {
			runtime.EmitUpdate("Loading knowledge schema…")
			res, err := b.engine.Schema(ctx, args.Kind)
			if err != nil {
				if errors.Is(err, brain.ErrNotFound) {
					return "", fmt.Errorf("schema: kind %q not found", strings.TrimSpace(args.Kind))
				}
				return "", fmt.Errorf("schema: %w", err)
			}
			return formatBrainJSON(res)
		},
	})
}

type queryArgs struct {
	Query   string         `json:"query" desc:"Search query text."`
	Filters map[string]any `json:"filters,omitempty" desc:"Optional field→value equality filters (e.g. kind, title, property keys, updated_after)."`
	Limit   int            `json:"limit,omitempty" desc:"Max results for this page (default 10, max 50)."`
}

func (b brainTools) newSearchTool() *Tool {
	return b.newQueryTool(ToolConfig{
		Name:        "search",
		DisplayName: "Knowledge Search",
		Description: `Search the knowledge base by concept. Returns ranked parent objects with evidence snippets.

Use when you know what you want but not where it is. Prefer schema() before inventing filter keys. Use continue with the returned result_set_id for the next page. Use read for full content of a hit.`,
		Category: streaming.ToolCategorySearch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
	}, "Searching knowledge base…", "search", false)
}

func (b brainTools) newFindExactTool() *Tool {
	return b.newQueryTool(ToolConfig{
		Name:        "find_exact",
		DisplayName: "Find Exact",
		Description: `Find knowledge objects by exact or near-exact match (UUID, title, path-like phrases).

Prefer this over search when you have a precise string. Returns ranked parents with evidence. Use continue for more pages; use read for full content.`,
		Category: streaming.ToolCategorySearch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
	}, "Finding exact matches…", "find_exact", true)
}

func (b brainTools) newQueryTool(cfg ToolConfig, status, errLabel string, exact bool) *Tool {
	cfg.Handler = func(ctx context.Context, args queryArgs, runtime HarnessRuntime) (string, error) {
		runtime.EmitUpdate(status)
		req := brain.SearchRequest{
			Query:   args.Query,
			Filters: brain.Filters(args.Filters),
			Limit:   args.Limit,
		}
		var (
			page brain.SearchPage
			err  error
		)
		if exact {
			page, err = b.engine.FindExact(ctx, b.sc.Scope(), req, b.sc)
		} else {
			page, err = b.engine.Search(ctx, b.sc.Scope(), req, b.sc)
		}
		if err != nil {
			return "", fmt.Errorf("%s: %w", errLabel, err)
		}
		return formatBrainJSON(page)
	}
	return NewTool(cfg)
}

type continueArgs struct {
	ResultSetID string `json:"result_set_id" desc:"Result set id from a prior search, find_exact, or expand."`
	Limit       int    `json:"limit,omitempty" desc:"Max results for this page (default 10, max 50)."`
}

func (b brainTools) newContinueTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "continue",
		DisplayName: "Continue Results",
		Description: `Return the next page of a prior ranked result set from search, find_exact, or expand.

Pass the result_set_id from the previous call. Each new search, find_exact, or large expand replaces the active result set — older result_set_id values stop working.`,
		Category: streaming.ToolCategorySearch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args continueArgs, runtime HarnessRuntime) (string, error) {
			id, err := parseUUID(args.ResultSetID, "result_set_id")
			if err != nil {
				return "", fmt.Errorf("continue: %w", err)
			}
			runtime.EmitUpdate("Loading more results…")
			page, err := b.engine.Continue(ctx, b.sc.Scope(), id, args.Limit, b.sc)
			if err != nil {
				if errors.Is(err, brain.ErrResultSetNotFound) {
					return "", fmt.Errorf("continue: result set not found; run search or find_exact again")
				}
				return "", fmt.Errorf("continue: %w", err)
			}
			return formatBrainJSON(page)
		},
	})
}

type expandArgs struct {
	ObjectID      string   `json:"object_id" desc:"UUID of the object to expand."`
	RelationTypes []string `json:"relation_types,omitempty" desc:"Optional relation types. Omit for containment (children or parent+siblings). Named types use the graph backend (e.g. references)."`
	Limit         int      `json:"limit,omitempty" desc:"Page size when results are paginated (default 10, max 50)."`
}

func (b brainTools) newExpandTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "expand",
		DisplayName: "Expand Object",
		Description: `Show objects structurally connected to this one.

From a parent: ordered children (containment). From a part: parent and nearby siblings. Omit relation_types for containment only; use contains/part_of explicitly if mixed with graph labels. Other relation_types need a graph backend (Helix). Large lists return result_set_id and replace any prior result set — use continue for more pages. Use read for full content.`,
		Category: streaming.ToolCategoryFetch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args expandArgs, runtime HarnessRuntime) (string, error) {
			id, err := parseUUID(args.ObjectID, "object_id")
			if err != nil {
				return "", fmt.Errorf("expand: %w", err)
			}
			runtime.EmitUpdate("Expanding knowledge object…")
			res, err := b.engine.Expand(ctx, b.sc.Scope(), brain.ExpandRequest{
				ObjectID:      id,
				RelationTypes: args.RelationTypes,
				Limit:         args.Limit,
			}, b.sc)
			if err != nil {
				return "", fmt.Errorf("expand: %w", err)
			}
			return formatBrainJSON(res)
		},
	})
}

func formatBrainJSON(v any) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", fmt.Errorf("brain tool: marshal result: %w", err)
	}
	return string(b), nil
}

func parseUUID(raw, field string) (uuid.UUID, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return uuid.Nil, fmt.Errorf("%s is required", field)
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%s must be a UUID: %w", field, err)
	}
	if id == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%s must not be the nil UUID", field)
	}
	return id, nil
}

// newBrainTools requires non-nil engine and search context.
func newBrainTools(engine *brain.Engine, sc *brain.SearchContext) []*Tool {
	if engine == nil || sc == nil {
		return nil
	}
	b := brainTools{engine: engine, sc: sc}
	return []*Tool{
		b.newReadObjectTool(),
		b.newSchemaTool(),
		b.newSearchTool(),
		b.newFindExactTool(),
		b.newContinueTool(),
		b.newExpandTool(),
	}
}
