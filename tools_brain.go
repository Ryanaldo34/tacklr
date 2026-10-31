package tacklr

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
)

// brainTools closes over the engine and the session's SearchContext (the
// host namespace ceiling, the principals, and each tool's ranked rest for
// more).
type brainTools struct {
	engine    *brain.Engine
	sc        *brain.SearchContext
	sessionID string
}

type namespaceArg struct {
	Namespace []brain.Attr `json:"namespace,omitempty" desc:"Isolation attributes for this call. Extra attrs narrow the host SearchNamespace; host values cannot be changed."`
}

func (b brainTools) scope(call []brain.Attr) (brain.Scope, error) {
	for _, attr := range call {
		if strings.TrimSpace(attr.Name) == brain.SessionTailName {
			return brain.Scope{}, fmt.Errorf("%w: namespace attr %q is reserved", brain.ErrInvalid, brain.SessionTailName)
		}
	}
	scope := b.sc.Scope()
	ns, err := scope.Namespace.Bind(call)
	if err != nil {
		return brain.Scope{}, err
	}
	scope.Namespace, scope.SessionID = ns, b.sessionID
	return scope, nil
}

// ref names an object by UUID or by an indexed file's virtual path.
func (b brainTools) ref(ctx context.Context, scope brain.Scope, idOrPath, field string) (uuid.UUID, error) {
	s := strings.TrimSpace(idOrPath)
	if s == "" {
		return uuid.Nil, fmt.Errorf("%s is required (a UUID or an indexed file path)", field)
	}
	if id, err := uuid.Parse(s); err == nil {
		return id, nil
	}
	p, err := vfs.CleanPath(s)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%s is neither a UUID nor a path: %w", field, err)
	}
	return b.engine.Resolve(ctx, scope, brain.Ref{Path: p})
}

// page returns the next page of the tool's previous call when more is set,
// else runs call, and keeps the page's rest for the next more.
func (b brainTools) page(ctx context.Context, tool string, scope brain.Scope, more bool, limit int, call func() (brain.Page, error)) (string, error) {
	var page brain.Page
	var err error
	if more {
		rest := b.sc.Rest(tool)
		if rest.Empty() {
			return "", fmt.Errorf("no more results; call %s again without more", tool)
		}
		page, err = b.engine.Next(ctx, scope, rest, limit)
	} else {
		page, err = call()
	}
	if err != nil {
		return "", err
	}
	b.sc.SetRest(tool, page.Rest)
	return formatBrainJSON(page)
}

type searchArgs struct {
	namespaceArg
	Query   string         `json:"query,omitempty" desc:"What to find, in plain words. Put text in double quotes to match it exactly; a UUID returns that object. Omit it, with a kind or filters, to list those records newest first."`
	Kind    string         `json:"kind,omitempty" desc:"Optional kind of record to find."`
	Within  string         `json:"within,omitempty" desc:"Optional: UUID or indexed file path of one record (or one of its parts). Searches only that record's parts, for where in it the query matches."`
	Filters map[string]any `json:"filters,omitempty" desc:"Optional filters on the records: title, created_after, created_before, updated_after, updated_before, or a kind's fields. The facets of a result and open show the fields and values that exist."`
	Limit   int            `json:"limit,omitempty" desc:"Max results (default 10, max 50)."`
	More    bool           `json:"more,omitempty" desc:"Return the next page of the previous search instead of searching again."`
}

func (b brainTools) newSearchTool() *Tool {
	t := NewTool(ToolConfig{
		Name:        "search",
		DisplayName: "Search knowledge: {query}",
		Description: `Search saved records, indexed files, links between records, and earlier messages of this session. Call when you need facts for the task.

Returns one ranked list. Each result is an object (with evidence snippets when its parts matched, and vfs_path for a file) or a link whose note matched (with both ends). With within, each result is a part of that record with the snippet that matched. Without a query, a kind or filters list the matching records, newest first. facets count the kinds and field values among the results: narrow a second search with them. more is true when another page exists. Fails on an unknown kind or filter, with the valid choices.`,
		Category: ToolCategorySearch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args searchArgs, runtime HarnessRuntime) (string, error) {
			runtime.EmitUpdate("Searching knowledge…")
			scope, err := b.scope(args.Namespace)
			if err != nil {
				return "", fmt.Errorf("search: %w", err)
			}
			filters, err := brain.DecodeFilter(args.Filters)
			if err != nil {
				return "", fmt.Errorf("search: %w", err)
			}
			out, err := b.page(ctx, "search", scope, args.More, args.Limit, func() (brain.Page, error) {
				req := brain.SearchRequest{Query: args.Query, Kind: args.Kind, Filters: filters, Limit: args.Limit}
				if strings.TrimSpace(args.Within) != "" {
					if req.Within, err = b.ref(ctx, scope, args.Within, "within"); err != nil {
						return brain.Page{}, err
					}
				}
				return b.engine.Search(ctx, scope, req)
			})
			if err != nil {
				return "", fmt.Errorf("search: %w", err)
			}
			return out, nil
		},
	})
	setEnum(t, "kind", b.engine.Catalog().RecordKinds())
	return t
}

type openArgs struct {
	namespaceArg
	Ref string `json:"ref" desc:"UUID of a record, part or earlier message, or the virtual path of an indexed file."`
}

func (b brainTools) newOpenTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "open",
		DisplayName: "Open {ref}",
		Description: `Open one record, part, indexed file or earlier message of this session in full. Call when a search or related result needs its whole content, or to learn its kind's fields before filtering.

Returns the object with its content, its kind (with the fields a search can filter on), and its first children. Fails if ref is not visible in this session.`,
		Category: ToolCategoryRead,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args openArgs, runtime HarnessRuntime) (string, error) {
			runtime.EmitUpdate("Opening knowledge…")
			scope, err := b.scope(args.Namespace)
			if err != nil {
				return "", fmt.Errorf("open: %w", err)
			}
			id, err := b.ref(ctx, scope, args.Ref, "ref")
			if err != nil {
				return "", fmt.Errorf("open: %w", err)
			}
			opened, err := b.engine.Open(ctx, scope, id)
			if err != nil {
				return "", fmt.Errorf("open: %s: %w", id, err)
			}
			return formatBrainJSON(opened)
		},
	})
}

type relatedArgs struct {
	namespaceArg
	Ref      string `json:"ref" desc:"UUID of a record or part, or the virtual path of an indexed file."`
	Relation string `json:"relation,omitempty" desc:"Optional: only this relation (\"contains\" for children only). Omit for children and every relation."`
	Depth    int    `json:"depth,omitempty" desc:"Relation hops to follow (default 1)."`
	Limit    int    `json:"limit,omitempty" desc:"Max results (default 10, max 50)."`
	More     bool   `json:"more,omitempty" desc:"Return the next page of the previous related call."`
}

func (b brainTools) newRelatedTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "related",
		DisplayName: "Related to {ref}",
		Description: `List what is related to one record: its children (or a part's parent and nearby parts) and the records its links reach. Call when you have a record and need its context.

Returns one list; each result's relation says how it is related (child, parent, sibling, or a link's relation, direction, depth and note). more is true when another page exists. Fails if ref is not visible in this session.`,
		Category: ToolCategoryFetch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args relatedArgs, runtime HarnessRuntime) (string, error) {
			runtime.EmitUpdate("Listing related knowledge…")
			scope, err := b.scope(args.Namespace)
			if err != nil {
				return "", fmt.Errorf("related: %w", err)
			}
			out, err := b.page(ctx, "related", scope, args.More, args.Limit, func() (brain.Page, error) {
				id, err := b.ref(ctx, scope, args.Ref, "ref")
				if err != nil {
					return brain.Page{}, err
				}
				return b.engine.Related(ctx, scope, brain.RelatedRequest{ID: id, Relation: args.Relation, Depth: args.Depth, Limit: args.Limit})
			})
			if err != nil {
				return "", fmt.Errorf("related: %w", err)
			}
			return out, nil
		},
	})
}

type saveArgs struct {
	namespaceArg
	Kind       string         `json:"kind" desc:"Kind of record to save."`
	Title      string         `json:"title" desc:"Short title; search matches it."`
	Summary    string         `json:"summary,omitempty" desc:"Optional one-line abstract; search matches it."`
	Content    string         `json:"content,omitempty" desc:"Optional full body."`
	Properties map[string]any `json:"properties,omitempty" desc:"Optional fields of the kind (open a record of the kind to see them)."`
	ID         string         `json:"id,omitempty" desc:"UUID of a record to update; omit to create one."`
}

func (b brainTools) newSaveTool(kinds []string) *Tool {
	t := NewTool(ToolConfig{
		Name:        "save",
		DisplayName: "Save {kind}: {title}",
		Description: `Save a finding, fact or memory as a durable record that later steps and sessions can search. Call when something you learned should survive this step.

Returns the saved record. Fails if title is empty, kind is not one of the kinds you may save, or a field is invalid (the error names the valid choices).`,
		Category: ToolCategoryEdit,
		Access:   ToolWriteAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args saveArgs, runtime HarnessRuntime) (string, error) {
			runtime.EmitUpdate("Saving knowledge…")
			kind := strings.TrimSpace(args.Kind)
			if !slices.Contains(kinds, kind) {
				return "", fmt.Errorf("save: kind %q cannot be saved; kinds: %s", kind, strings.Join(kinds, ", "))
			}
			if strings.TrimSpace(args.Title) == "" {
				return "", fmt.Errorf("save: title is required")
			}
			scope, err := b.scope(args.Namespace)
			if err != nil {
				return "", fmt.Errorf("save: %w", err)
			}
			obj := brain.Object{
				Kind:       kind,
				Title:      strings.TrimSpace(args.Title),
				Summary:    strings.TrimSpace(args.Summary),
				Content:    args.Content,
				Properties: args.Properties,
			}
			if s := strings.TrimSpace(args.ID); s != "" {
				if obj.ID, err = uuid.Parse(s); err != nil {
					return "", fmt.Errorf("save: id must be a UUID: %w", err)
				}
			}
			saved, err := b.engine.Save(ctx, scope, obj)
			if err != nil {
				return "", fmt.Errorf("save: %w", err)
			}
			return formatBrainJSON(saved.Rich(true))
		},
	})
	setEnum(t, "kind", kinds)
	return t
}

type linkArgs struct {
	namespaceArg
	From     string `json:"from" desc:"UUID or indexed file path of the source record."`
	To       string `json:"to" desc:"UUID or indexed file path of the target record."`
	Relation string `json:"relation" desc:"Relation label, e.g. references, about."`
	Note     string `json:"note,omitempty" desc:"Why they are related; search finds links by this note."`
	Remove   bool   `json:"remove,omitempty" desc:"Remove the link instead of creating it."`
}

// linkResult is the link tool's payload.
type linkResult struct {
	From     string `json:"from"`
	To       string `json:"to"`
	Relation string `json:"relation"`
	Linked   bool   `json:"linked"`
	Note     string `json:"note,omitempty"`
}

func (b brainTools) newLinkTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "link",
		DisplayName: "Link {from} → {to}",
		Description: `Create, update or remove a relationship between two records. Call to record how they relate, so related and search can follow it.

Linking the same pair again updates the note. Removing a link that is absent succeeds. Fails if an end is missing or is a part rather than a record.`,
		Category: ToolCategoryEdit,
		Access:   ToolWriteAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args linkArgs, runtime HarnessRuntime) (string, error) {
			runtime.EmitUpdate("Linking knowledge…")
			scope, err := b.scope(args.Namespace)
			if err != nil {
				return "", fmt.Errorf("link: %w", err)
			}
			from, err := b.ref(ctx, scope, args.From, "from")
			if err != nil {
				return "", fmt.Errorf("link: %w", err)
			}
			to, err := b.ref(ctx, scope, args.To, "to")
			if err != nil {
				return "", fmt.Errorf("link: %w", err)
			}
			req := brain.LinkRequest{From: from, To: to, Relation: args.Relation, Meta: brain.EdgeMeta{Note: args.Note}, Remove: args.Remove}
			if err := b.engine.Link(ctx, scope, req); err != nil {
				return "", fmt.Errorf("link: %w", err)
			}
			return formatBrainJSON(linkResult{From: from.String(), To: to.String(), Relation: strings.TrimSpace(args.Relation), Linked: !args.Remove, Note: strings.TrimSpace(args.Note)})
		},
	})
}

// setEnum makes the tool's string property prop one of values; no values
// leaves it free. An optional (nullable) property also takes null.
func setEnum(t *Tool, prop string, values []string) {
	if len(values) == 0 {
		return
	}
	props, _ := t.parameters["properties"].(map[string]any)
	schema, _ := props[prop].(map[string]any) // every caller names a property the tool has
	enum := make([]any, 0, len(values)+1)
	for _, v := range values {
		enum = append(enum, v)
	}
	if types, ok := schema["type"].([]any); ok && slices.Contains(types, any("null")) {
		enum = append(enum, nil)
	}
	schema["enum"] = enum
}

func formatBrainJSON(v any) (string, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", fmt.Errorf("brain tool: marshal result: %w", err)
	}
	return string(b), nil
}

// newBrainTools builds the knowledge tools: search, open and related; save
// when writeKinds is not empty; link when the engine has a graph. Caller
// must pass a non-nil engine and SearchContext.
func newBrainTools(engine *brain.Engine, sc *brain.SearchContext, sessionID string, writeKinds []string) []*Tool {
	b := brainTools{engine: engine, sc: sc, sessionID: sessionID}
	tools := []*Tool{b.newSearchTool(), b.newOpenTool(), b.newRelatedTool()}
	var kinds []string
	for _, k := range writeKinds {
		if k = strings.TrimSpace(k); k != "" && !slices.Contains(kinds, k) {
			kinds = append(kinds, k)
		}
	}
	if len(kinds) > 0 {
		tools = append(tools, b.newSaveTool(kinds))
	}
	if engine.HasGraph() {
		tools = append(tools, b.newLinkTool())
	}
	return tools
}
