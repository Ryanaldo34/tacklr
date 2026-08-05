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

Use after search, find_exact, find_objects, or expand when you need the complete body of a known object. Pass the object UUID from a prior rich result. Do not invent ids. Prefer reading only objects you will use or cite.`,
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

Call with a kind to see filterable fields, types, and operators for that kind. Call with no kind to list registered kinds. Prefer this before inventing property names in filters. When kinds are registered by the host, only those kinds are searchable and property filters require a kind key.`,
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
	Query    string         `json:"query" desc:"Search query text. Prefer a semantic rewrite of the user ask when helpful."`
	Filters  map[string]any `json:"filters,omitempty" desc:"Optional field→value filters (kind, title, property keys, updated_after). Prefer schema() first. All content filters belong here."`
	Limit    int            `json:"limit,omitempty" desc:"Max results for this page (default 10, max 50)."`
	ScopeIDs []string       `json:"scope_ids,omitempty" desc:"Optional UUIDs of parents (or objects) to restrict hits to this neighborhood after expand/find_objects."`
}

func (b brainTools) newSearchTool() *Tool {
	return b.newQueryTool(ToolConfig{
		Name:        "search",
		DisplayName: "Knowledge Search",
		Description: `Search stored content (documents, notes, chunks) in the knowledge corpus. Returns ranked parent objects with evidence snippets.

Use for open questions and document-style evidence. Prefer schema() before inventing filter keys; property filters require kind when kinds are registered. All structured filters belong on this tool (there is no separate filtered-search tool). Rewrite the user ask into a good retrieval query when helpful.

Do not use this only to discover relationships—use expand once you have an id. Prefer find_objects when you need a tracked entity (e.g. a deal, fact, or memory as an object), not a passage. Use continue for more pages; read for full body of a hit.`,
		Category: streaming.ToolCategorySearch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
	}, "Searching knowledge base…", b.engine.Search)
}

func (b brainTools) newFindExactTool() *Tool {
	return b.newQueryTool(ToolConfig{
		Name:        "find_exact",
		DisplayName: "Find Exact",
		Description: `Find knowledge objects by exact or near-exact match (UUID, title, path-like phrases) in the content store.

Prefer this over search when you have a precise string or UUID. Returns ranked parents with evidence. When kinds are registered, property filters require kind. Use continue for more pages; use read for full content. For meaning-based entity lookup without an exact string, use find_objects when available.`,
		Category: streaming.ToolCategorySearch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
	}, "Finding exact matches…", b.engine.FindExact)
}

func (b brainTools) newQueryTool(
	cfg ToolConfig,
	status string,
	query func(context.Context, brain.Scope, brain.SearchRequest, brain.ResultSetStore) (brain.SearchPage, error),
) *Tool {
	name := cfg.Name
	cfg.Handler = func(ctx context.Context, args queryArgs, runtime HarnessRuntime) (string, error) {
		runtime.EmitUpdate(status)
		scopeIDs, err := parseOptionalUUIDList(args.ScopeIDs, "scope_ids")
		if err != nil {
			return "", fmt.Errorf("%s: %w", name, err)
		}
		page, err := query(ctx, b.sc.Scope(), brain.SearchRequest{
			Query:    args.Query,
			Filters:  brain.Filters(args.Filters),
			Limit:    args.Limit,
			ScopeIDs: scopeIDs,
		}, b.sc)
		if err != nil {
			return "", fmt.Errorf("%s: %w", name, err)
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
		Description: `Return the next page of a prior ranked result set from search, find_exact, find_objects, or large expand.

Pass the result_set_id from the previous call. Each new search, find_exact, find_objects, or large expand replaces the active result set — older result_set_id values stop working.`,
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
		Description: `Show objects structurally connected to a known object_id—not an open search.

From a parent: ordered children (containment). From a part: parent and nearby siblings. Omit relation_types for containment only; use contains/part_of if mixed with graph labels. Other relation_types need a graph backend (e.g. about, references, blocked_by). Prefer expand first when the active entity id is already known (e.g. "risks on this deal"). Large lists return result_set_id — use continue for more pages. Use read for full content.`,
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

type saveObjectArgs struct {
	Title       string         `json:"title" desc:"Short title for the object."`
	Summary     string         `json:"summary,omitempty" desc:"Optional short abstract."`
	Content     string         `json:"content,omitempty" desc:"Optional full body text."`
	ContentType string         `json:"content_type,omitempty" desc:"Optional MIME type (e.g. text/plain, text/markdown)."`
	Properties  map[string]any `json:"properties,omitempty" desc:"Kind-defined fields from schema(). Prefer schema() first."`
	ParentID    string         `json:"parent_id,omitempty" desc:"Optional parent UUID when this is a part object."`
	ObjectID    string         `json:"object_id,omitempty" desc:"Optional existing UUID to update; omit to create."`
}

func (b brainTools) newSaveTool(name, display, kind, roleDesc string) *Tool {
	return NewTool(ToolConfig{
		Name:        name,
		DisplayName: display,
		Description: `Save a ` + roleDesc + ` to the knowledge base as kind ` + kind + `.

Prefer schema() for this kind before inventing property keys. Write a clear title and summary so search and find_objects can retrieve this later. Uses the host search namespace when set. Pass object_id to update an existing object. Returns the rich object reference.`,
		Category: streaming.ToolCategoryEdit,
		Access:   ToolWriteAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args saveObjectArgs, runtime HarnessRuntime) (string, error) {
			runtime.EmitUpdate("Saving knowledge object…")
			obj, err := b.putFromArgs(ctx, kind, args)
			if err != nil {
				return "", fmt.Errorf("%s: %w", name, err)
			}
			return formatBrainJSON(brain.RichFromObject(obj, true))
		},
	})
}

type linkArgs struct {
	FromID       string `json:"from_id" desc:"UUID of the source object."`
	ToID         string `json:"to_id" desc:"UUID of the target object."`
	RelationType string `json:"relation_type" desc:"Non-containment relation label (e.g. references, about)."`
}

type findObjectsArgs struct {
	Query string   `json:"query" desc:"Semantic or keyword query for whole knowledge objects (entities)."`
	Kinds []string `json:"kinds,omitempty" desc:"Optional host kind names to restrict results (e.g. Deal, Fact). Prefer schema() for valid kinds."`
	Limit int      `json:"limit,omitempty" desc:"Max results for this page (default 10, max 50)."`
}

func (b brainTools) newFindObjectsTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "find_objects",
		DisplayName: "Find Objects",
		Description: `Find knowledge objects as entities (whole objects of given kinds), not long document ranking.

Use to resolve which tracked object matches an ask, or to find similar saved objects (facts, discoveries, memories, deals as host kinds). Rewrite the user ask into a good semantic query (e.g. risk themes, blockers). Prefer expand first when the active entity id is already known. For bulk document/note evidence or property filters on content, use search instead. After an id, use expand for relationships and read only for bodies you need. Use continue when has_more.`,
		Category: streaming.ToolCategorySearch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args findObjectsArgs, runtime HarnessRuntime) (string, error) {
			runtime.EmitUpdate("Finding knowledge objects…")
			page, err := b.engine.FindObjects(ctx, b.sc.Scope(), brain.FindObjectsRequest{
				Query: args.Query,
				Kinds: args.Kinds,
				Limit: args.Limit,
			}, b.sc)
			if err != nil {
				return "", fmt.Errorf("find_objects: %w", err)
			}
			return formatBrainJSON(page)
		},
	})
}

func (b brainTools) newLinkTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "link",
		DisplayName: "Link Objects",
		Description: `Create a non-containment relationship between two knowledge objects (graph edge).

Both objects should already exist (e.g. via save_* or host ingest). Use expand with the same relation_type to traverse. Containment (parent/child) uses parent_id on save, not this tool.`,
		Category: streaming.ToolCategoryEdit,
		Access:   ToolWriteAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args linkArgs, runtime HarnessRuntime) (string, error) {
			from, err := parseUUID(args.FromID, "from_id")
			if err != nil {
				return "", fmt.Errorf("link: %w", err)
			}
			to, err := parseUUID(args.ToID, "to_id")
			if err != nil {
				return "", fmt.Errorf("link: %w", err)
			}
			runtime.EmitUpdate("Linking knowledge objects…")
			if err := b.engine.Link(ctx, from, to, args.RelationType); err != nil {
				return "", fmt.Errorf("link: %w", err)
			}
			return formatBrainJSON(map[string]string{
				"from_id":       from.String(),
				"to_id":         to.String(),
				"relation_type": strings.TrimSpace(args.RelationType),
				"status":        "linked",
			})
		},
	})
}

func (b brainTools) putFromArgs(ctx context.Context, kind string, args saveObjectArgs) (brain.Object, error) {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return brain.Object{}, fmt.Errorf("kind is not configured")
	}
	obj := brain.Object{
		Kind:        kind,
		Title:       strings.TrimSpace(args.Title),
		Summary:     strings.TrimSpace(args.Summary),
		Content:     args.Content,
		ContentType: strings.TrimSpace(args.ContentType),
		Properties:  args.Properties,
	}
	id, err := parseOptionalUUID(args.ObjectID, "object_id")
	if err != nil {
		return brain.Object{}, err
	}
	if id != nil {
		obj.ID = *id
	}
	parent, err := parseOptionalUUID(args.ParentID, "parent_id")
	if err != nil {
		return brain.Object{}, err
	}
	obj.ParentID = parent
	return b.engine.Put(ctx, b.sc.Scope(), obj)
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

// parseOptionalUUID returns nil when raw is empty.
func parseOptionalUUID(raw, field string) (*uuid.UUID, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	id, err := parseUUID(raw, field)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

func parseOptionalUUIDList(raw []string, field string) ([]uuid.UUID, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]uuid.UUID, 0, len(raw))
	for i, s := range raw {
		if strings.TrimSpace(s) == "" {
			continue
		}
		id, err := parseUUID(s, fmt.Sprintf("%s[%d]", field, i))
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

// newBrainTools builds knowledge tools. Caller must pass a non-nil engine and SearchContext.
// save_* tools are registered only for non-empty WriteKinds fields; link only with GraphWriter;
// find_objects only when GraphObjectSearcher is available.
func newBrainTools(engine *brain.Engine, sc *brain.SearchContext, kinds brain.WriteKinds) []*Tool {
	b := brainTools{engine: engine, sc: sc}
	tools := []*Tool{
		b.newReadObjectTool(),
		b.newSchemaTool(),
		b.newSearchTool(),
		b.newFindExactTool(),
		b.newContinueTool(),
		b.newExpandTool(),
	}
	if engine.HasObjectSearch() {
		tools = append(tools, b.newFindObjectsTool())
	}
	for _, s := range []struct {
		name, display, kind, role string
	}{
		{"save_discovery", "Save Discovery", kinds.Discovery, "working discovery or finding"},
		{"save_fact", "Save Fact", kinds.Fact, "durable fact"},
		{"save_memory", "Save Memory", kinds.Memory, "preference or durable memory"},
	} {
		if kind := strings.TrimSpace(s.kind); kind != "" {
			tools = append(tools, b.newSaveTool(s.name, s.display, kind, s.role))
		}
	}
	if engine.HasGraphWriter() {
		tools = append(tools, b.newLinkTool())
	}
	return tools
}
