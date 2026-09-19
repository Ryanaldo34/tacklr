package tacklr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/vfs"
	"github.com/ryanaldo34/tacklr/vfsindex"
)

// brainToolDeps optional VFS+index wiring so save_* / path-native graph can use files.
type brainToolDeps struct {
	VFS     *vfs.MountSession
	Indexer *vfsindex.MountIndexer
}

// brainTools closes over the engine and SearchContext (host ceiling + result set).
type brainTools struct {
	engine *brain.Engine
	sc     *brain.SearchContext
	deps   brainToolDeps
}

type namespaceArg struct {
	Namespace []brain.Attr `json:"namespace,omitempty" desc:"Isolation attributes for this call. Extra attrs narrow the host SearchNamespace; host values cannot be changed."`
}

func (b brainTools) scope(call []brain.Attr) (brain.Scope, error) {
	ceiling, _ := b.sc.Namespace()
	ns, err := ceiling.Bind(call)
	if err != nil {
		return brain.Scope{}, err
	}
	return brain.Scope{Namespace: ns}, nil
}

func (b brainTools) brainMountForKind(kind string) (vfs.MountSpec, bool) {
	if b.deps.VFS == nil {
		return vfs.MountSpec{}, false
	}
	return brain.MountForKind(b.deps.VFS.Specs(), kind)
}

type readObjectArgs struct {
	namespaceArg
	ObjectID string `json:"object_id" desc:"UUID of the object to read in full."`
}

func (b brainTools) newReadObjectTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "read_object",
		DisplayName: "Read object {object_id}",
		Description: `Read a knowledge record by UUID and return its full stored body as JSON (id, kind, title, summary, content, properties). Call when you have an id from a prior knowledge result and need the complete record. Fails if object_id is missing, invalid, or not visible in the current namespace.`,
		Category:    ToolCategoryRead,
		Access:      ToolReadAccess,
		Timeout:     30 * time.Second,
		Handler: func(ctx context.Context, args readObjectArgs, runtime HarnessRuntime) (string, error) {
			id, err := parseUUID(args.ObjectID, "object_id")
			if err != nil {
				return "", fmt.Errorf("read_object: %w", err)
			}
			runtime.EmitUpdate("Reading knowledge object…")
			scope, err := b.scope(args.Namespace)
			if err != nil {
				return "", fmt.Errorf("read_object: %w", err)
			}
			obj, err := b.engine.Read(ctx, scope, id)
			if err != nil {
				return "", fmt.Errorf("read_object: object %s: %w", id, err)
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
		DisplayName: "Schema {kind}",
		Description: `Return kind documentation, columns, and filterable_fields. Call before inventing property names. Core filter keys are kind, title, created_after, created_before, updated_after, updated_before; other keys are kind properties. When kinds are registered, property filters require a kind. Fails if the named kind is unknown.`,
		Category:    ToolCategoryRead,
		Access:      ToolReadAccess,
		Timeout:     30 * time.Second,
		Handler: func(ctx context.Context, args schemaArgs, runtime HarnessRuntime) (string, error) {
			runtime.EmitUpdate("Loading knowledge schema…")
			res, err := b.engine.Schema(ctx, args.Kind)
			if err != nil {
				return "", fmt.Errorf("schema: %w", err)
			}
			return formatBrainJSON(res)
		},
	})
}

type queryArgs struct {
	namespaceArg
	Query    string         `json:"query" desc:"Query text. Semantic rewrite of the ask for search; UUID, title, or path-like phrase for find_exact."`
	Filters  map[string]any `json:"filters,omitempty" desc:"Optional field→value filters (kind, title, property keys, updated_after). All content filters belong here."`
	Limit    int            `json:"limit,omitempty" desc:"Max results for this page (default 10, max 50)."`
	ScopeIDs []string       `json:"scope_ids,omitempty" desc:"Optional UUIDs of parents (or objects) to restrict hits to this neighborhood after expand/find_objects."`
}

func (b brainTools) newSearchTool() *Tool {
	return b.newQueryTool(ToolConfig{
		Name:        "search",
		DisplayName: "Search knowledge: {query}",
		Description: `Search notes and indexed files by meaning. Call when starting a new plan to find durable facts related to the task, and after a handoff when a needed detail is missing from the current notes.

Returns a page of ranked parent records with evidence snippets (optional vfs_path, start_line, block_id), a result_set_id, and has_more. Replaces the active result set. Fails on invalid filters or namespace.`,
		Category: ToolCategorySearch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
	}, "Searching knowledge base…", b.engine.Search)
}

func (b brainTools) newFindExactTool() *Tool {
	return b.newQueryTool(ToolConfig{
		Name:        "find_exact",
		DisplayName: "Find exact: {query}",
		Description: `Find a record by exact or near-exact string. Call when you already have the identifier or a precise phrase.

Returns a page of ranked parent records with evidence, a result_set_id, and has_more. A UUID query loads that object directly. Replaces the active result set. Fails on invalid filters or namespace.`,
		Category: ToolCategorySearch,
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
		filters, err := brain.DecodeFilter(args.Filters)
		if err != nil {
			return "", fmt.Errorf("%s: %w", name, err)
		}
		scope, err := b.scope(args.Namespace)
		if err != nil {
			return "", fmt.Errorf("%s: %w", name, err)
		}
		page, err := query(ctx, scope, brain.SearchRequest{
			Query:    args.Query,
			Filters:  filters,
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
	ResultSetID string `json:"result_set_id" desc:"Result set id from a prior search, find_exact, find_objects, or expand."`
	Limit       int    `json:"limit,omitempty" desc:"Max results for this page (default 10, max 50)."`
}

func (b brainTools) newContinueTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "continue",
		DisplayName: "Continue results",
		Description: `Return the next page of the active ranked result set. Call when a prior ranking result has_more.

Returns the next page of records, has_more, and the same result_set_id. Fails if result_set_id is missing, invalid, or no longer active because a newer ranking call replaced it.`,
		Category: ToolCategorySearch,
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
				if errors.Is(err, brain.ErrNotFound) {
					return "", fmt.Errorf("continue: result set not found; run search or find_exact again: %w", err)
				}
				return "", fmt.Errorf("continue: %w", err)
			}
			return formatBrainJSON(page)
		},
	})
}

type expandArgs struct {
	namespaceArg
	Path          string   `json:"path,omitempty" desc:"Absolute virtual path of an indexed file to expand. Prefer path when the node is a file."`
	ObjectID      string   `json:"object_id,omitempty" desc:"UUID of the object to expand when path is not used."`
	RelationTypes []string `json:"relation_types,omitempty" desc:"Optional relation types. Omit for parent/child containment; named types follow relationships (e.g. references)."`
	MaxHops       int      `json:"max_hops,omitempty" desc:"Graph hop depth (default 1, capped by host). Use 2+ to walk paths like entity→related→aggregate."`
	Direction     string   `json:"direction,omitempty" desc:"Graph edge direction: out, in, or both (default both)."`
	Limit         int      `json:"limit,omitempty" desc:"Page size when results are paginated (default 10, max 50)."`
}

func (b brainTools) newExpandTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "expand",
		DisplayName: "Expand {path}",
		Description: `List neighbors of a known path or object_id. Call when you already have a record and need its children, siblings, or related records.

Returns neighboring records (with vfs_path when set, and relation metadata on graph hops). Large results are paged with result_set_id and has_more, and replace the active result set. Fails if path/id is missing, not visible, or named relations are requested without a graph.`,
		Category: ToolCategoryFetch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args expandArgs, runtime HarnessRuntime) (string, error) {
			scope, err := b.scope(args.Namespace)
			if err != nil {
				return "", fmt.Errorf("expand: %w", err)
			}
			id, _, err := b.resolveFileRef(ctx, scope, args.Path, args.ObjectID, "expand", false)
			if err != nil {
				return "", err
			}
			runtime.EmitUpdate("Expanding knowledge object…")
			res, err := b.engine.Expand(ctx, scope, brain.ExpandRequest{
				ObjectID:      id,
				RelationTypes: args.RelationTypes,
				MaxHops:       args.MaxHops,
				Direction:     args.Direction,
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
	namespaceArg
	Title       string         `json:"title" desc:"Short title for the object."`
	Summary     string         `json:"summary,omitempty" desc:"Optional short abstract."`
	Content     string         `json:"content,omitempty" desc:"Optional full body text."`
	ContentType string         `json:"content_type,omitempty" desc:"Optional MIME type (e.g. text/plain, text/markdown)."`
	Properties  map[string]any `json:"properties,omitempty" desc:"Kind-defined fields. Discover names from schema."`
	ParentID    string         `json:"parent_id,omitempty" desc:"Optional parent UUID when this is a part object."`
	ObjectID    string         `json:"object_id,omitempty" desc:"Optional existing UUID to update; omit to create."`
}

func (b brainTools) newSaveTool(name, display, kind, roleDesc string) *Tool {
	desc := `Save a ` + roleDesc + ` as kind ` + kind + `.`
	if _, ok := b.brainMountForKind(""); ok {
		desc = `Save a ` + roleDesc + ` (kind ` + kind + `) as a Markdown file so it survives later steps as a durable record.

On success returns path, id, kind, and title. Fails if title is empty, the kind is invalid, or required properties are missing.`
	} else {
		desc += ` Call when a finding should survive later steps as a durable record.

On success returns the full record (id, kind, title, summary, content, properties). Fails if the kind is invalid or required properties are missing.`
	}
	return NewTool(ToolConfig{
		Name:        name,
		DisplayName: display,
		Description: desc,
		Category:    ToolCategoryEdit,
		Access:      ToolWriteAccess,
		Timeout:     30 * time.Second,
		Handler: func(ctx context.Context, args saveObjectArgs, runtime HarnessRuntime) (string, error) {
			runtime.EmitUpdate("Saving knowledge…")
			if _, ok := b.brainMountForKind(""); ok {
				out, err := b.saveAsFile(ctx, kind, args)
				if err != nil {
					return "", fmt.Errorf("%s: %w", name, err)
				}
				return formatBrainJSON(out)
			}
			obj, err := b.putFromArgs(ctx, kind, args)
			if err != nil {
				return "", fmt.Errorf("%s: %w", name, err)
			}
			return formatBrainJSON(brain.RichFromObject(obj, true))
		},
	})
}

type linkArgs struct {
	namespaceArg
	From         string  `json:"from,omitempty" desc:"Absolute virtual path of the source file (preferred when both ends are indexed files)."`
	To           string  `json:"to,omitempty" desc:"Absolute virtual path of the target file."`
	FromID       string  `json:"from_id,omitempty" desc:"UUID of the source object when path is not used."`
	ToID         string  `json:"to_id,omitempty" desc:"UUID of the target object when path is not used."`
	RelationType string  `json:"relation_type" desc:"Non-containment relation label (e.g. references, about)."`
	Note         string  `json:"note,omitempty" desc:"Short reason or rationale for the link (shown on expand)."`
	Status       string  `json:"status,omitempty" desc:"Optional link status (e.g. active, resolved)."`
	Role         string  `json:"role,omitempty" desc:"Optional role on the link (e.g. primary buyer)."`
	Confidence   float64 `json:"confidence,omitempty" desc:"Optional confidence in (0,1]; omit when unknown."`
	EvidenceID   string  `json:"evidence_id,omitempty" desc:"Optional UUID of a supporting object (email/chunk) for this link."`
}

type findObjectsArgs struct {
	namespaceArg
	Query   string         `json:"query" desc:"Semantic or keyword query for whole knowledge objects (entities)."`
	Kinds   []string       `json:"kinds,omitempty" desc:"Optional kind names to restrict results."`
	Filters map[string]any `json:"filters,omitempty" desc:"Optional field→value filters (same keys as search). Property filters require a kind when kinds are registered (or set kinds here)."`
	Limit   int            `json:"limit,omitempty" desc:"Max results for this page (default 10, max 50)."`
}

type findLinksArgs struct {
	namespaceArg
	RelationType string `json:"relation_type" desc:"Relation label to search (e.g. about, references)."`
	Query        string `json:"query" desc:"Text query matched against edge note metadata."`
	Limit        int    `json:"limit,omitempty" desc:"Max links for this page (default 10, max 50)."`
}

func (b brainTools) newFindLinksTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "find_links",
		DisplayName: "Find links: {query}",
		Description: `Find relationships by text on the relationship note. Call when you know the relation label and want pairs whose note matches the query.

Returns each link's endpoints (ids and paths when set), relation_type, and optional metadata. Fails if relation_type is empty, edge search is unavailable, or the namespace is invalid.`,
		Category: ToolCategorySearch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args findLinksArgs, runtime HarnessRuntime) (string, error) {
			runtime.EmitUpdate("Finding graph links…")
			scope, err := b.scope(args.Namespace)
			if err != nil {
				return "", fmt.Errorf("find_links: %w", err)
			}
			res, err := b.engine.FindLinks(ctx, scope, brain.FindLinksRequest{
				RelationType: args.RelationType,
				Query:        args.Query,
				Limit:        args.Limit,
			})
			if err != nil {
				return "", fmt.Errorf("find_links: %w", err)
			}
			return formatBrainJSON(annotateFindLinksPaths(res))
		},
	})
}

func (b brainTools) newFindObjectsTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "find_objects",
		DisplayName: "Find objects: {query}",
		Description: `Find a named record (a paper, person, experiment, fact, or earlier session note) rather than a passage. Call when starting a new plan to look up durable facts related to the task, and after a handoff to recover findings that may have been saved.

Returns a page of ranked records, a result_set_id, and has_more. Replaces the active result set. Fails if object search is unavailable, filters are invalid, or the namespace is invalid.`,
		Category: ToolCategorySearch,
		Access:   ToolReadAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args findObjectsArgs, runtime HarnessRuntime) (string, error) {
			runtime.EmitUpdate("Finding knowledge objects…")
			filters, err := brain.DecodeFilter(args.Filters)
			if err != nil {
				return "", fmt.Errorf("find_objects: %w", err)
			}
			scope, err := b.scope(args.Namespace)
			if err != nil {
				return "", fmt.Errorf("find_objects: %w", err)
			}
			page, err := b.engine.FindObjects(ctx, scope, brain.FindObjectsRequest{
				Query:   args.Query,
				Kinds:   args.Kinds,
				Filters: filters,
				Limit:   args.Limit,
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
		DisplayName: "Link {from} → {to}",
		Description: `Create or update a relationship between two existing records. Call to assert how they relate.

On success returns the endpoint ids/paths, relation_type, and metadata. Re-linking the same pair updates metadata. Fails if an endpoint is missing, is a part rather than a record, or a graph is not available.`,
		Category: ToolCategoryEdit,
		Access:   ToolWriteAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args linkArgs, runtime HarnessRuntime) (string, error) {
			scope, err := b.scope(args.Namespace)
			if err != nil {
				return "", fmt.Errorf("link: %w", err)
			}
			from, fromPath, err := b.resolveFileRef(ctx, scope, args.From, args.FromID, "from", true)
			if err != nil {
				return "", fmt.Errorf("link: %w", err)
			}
			to, toPath, err := b.resolveFileRef(ctx, scope, args.To, args.ToID, "to", true)
			if err != nil {
				return "", fmt.Errorf("link: %w", err)
			}
			meta, err := edgeMetaFromLinkArgs(args)
			if err != nil {
				return "", fmt.Errorf("link: %w", err)
			}
			runtime.EmitUpdate("Linking knowledge objects…")
			if err := b.engine.LinkWith(ctx, scope, from, to, args.RelationType, meta); err != nil {
				return "", fmt.Errorf("link: %w", err)
			}
			out := linkResult{
				FromID:       from.String(),
				ToID:         to.String(),
				FromPath:     fromPath,
				ToPath:       toPath,
				RelationType: strings.TrimSpace(args.RelationType),
				Linked:       true,
				Note:         meta.Note,
				LinkStatus:   meta.Status,
				Role:         meta.Role,
				Confidence:   meta.Confidence,
			}
			if meta.EvidenceID != nil {
				s := meta.EvidenceID.String()
				out.EvidenceID = s
			}
			return formatBrainJSON(out)
		},
	})
}

func (b brainTools) newUnlinkTool() *Tool {
	return NewTool(ToolConfig{
		Name:        "unlink",
		DisplayName: "Unlink {from} → {to}",
		Description: `Remove a relationship between two records. Call when that relationship should no longer hold.

On success the edge is gone. If the relationship was already absent, the call still succeeds. Fails if an endpoint is missing, is a part rather than a record, or a graph is not available.`,
		Category: ToolCategoryEdit,
		Access:   ToolWriteAccess,
		Timeout:  30 * time.Second,
		Handler: func(ctx context.Context, args linkArgs, runtime HarnessRuntime) (string, error) {
			scope, err := b.scope(args.Namespace)
			if err != nil {
				return "", fmt.Errorf("unlink: %w", err)
			}
			from, fromPath, err := b.resolveFileRef(ctx, scope, args.From, args.FromID, "from", true)
			if err != nil {
				return "", fmt.Errorf("unlink: %w", err)
			}
			to, toPath, err := b.resolveFileRef(ctx, scope, args.To, args.ToID, "to", true)
			if err != nil {
				return "", fmt.Errorf("unlink: %w", err)
			}
			runtime.EmitUpdate("Unlinking knowledge objects…")
			if err := b.engine.Unlink(ctx, scope, from, to, args.RelationType); err != nil {
				return "", fmt.Errorf("unlink: %w", err)
			}
			return formatBrainJSON(linkResult{
				FromID:       from.String(),
				ToID:         to.String(),
				FromPath:     fromPath,
				ToPath:       toPath,
				RelationType: strings.TrimSpace(args.RelationType),
				Linked:       false,
			})
		},
	})
}

// linkResult is the agent-facing payload for a successful link tool call.
type linkResult struct {
	FromID       string  `json:"from_id"`
	ToID         string  `json:"to_id"`
	FromPath     string  `json:"from_path,omitempty"`
	ToPath       string  `json:"to_path,omitempty"`
	RelationType string  `json:"relation_type"`
	Linked       bool    `json:"linked"`
	Note         string  `json:"note,omitempty"`
	LinkStatus   string  `json:"link_status,omitempty"` // edge status; not HTTP status
	Role         string  `json:"role,omitempty"`
	Confidence   float64 `json:"confidence,omitempty"`
	EvidenceID   string  `json:"evidence_id,omitempty"`
}

// saveFileResult is returned by file-backed save_* tools.
type saveFileResult struct {
	Path     string    `json:"path"`
	Rev      string    `json:"rev"`
	ObjectID uuid.UUID `json:"object_id"`
	Kind     string    `json:"kind"`
	Title    string    `json:"title"`
}

// findLinksResultView adds path fields for agent-facing find_links output.
type findLinksResultView struct {
	Links []findLinkHitView `json:"links"`
}

type findLinkHitView struct {
	From         brain.RichObject `json:"from"`
	To           brain.RichObject `json:"to"`
	FromPath     string           `json:"from_path,omitempty"`
	ToPath       string           `json:"to_path,omitempty"`
	RelationType string           `json:"relation_type"`
	Meta         brain.EdgeMeta   `json:"meta,omitempty"`
	Score        float64          `json:"score,omitempty"`
}

func edgeMetaFromLinkArgs(args linkArgs) (brain.EdgeMeta, error) {
	meta := brain.EdgeMeta{
		Note:       strings.TrimSpace(args.Note),
		Status:     strings.TrimSpace(args.Status),
		Role:       strings.TrimSpace(args.Role),
		Confidence: args.Confidence,
	}
	if strings.TrimSpace(args.EvidenceID) == "" {
		return meta, nil
	}
	id, err := parseUUID(args.EvidenceID, "evidence_id")
	if err != nil {
		return brain.EdgeMeta{}, err
	}
	meta.EvidenceID = &id
	return meta, nil
}

func (b brainTools) putFromArgs(ctx context.Context, kind string, args saveObjectArgs) (brain.Object, error) {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return brain.Object{}, fmt.Errorf("kind is not configured for this tool; use the matching save_* tool")
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
	scope, err := b.scope(args.Namespace)
	if err != nil {
		return brain.Object{}, err
	}
	return b.engine.Put(ctx, scope, obj)
}

// saveAsFile writes an Engram Markdown file on the brain Provider mount.
func (b brainTools) saveAsFile(ctx context.Context, kind string, args saveObjectArgs) (saveFileResult, error) {
	kind = strings.TrimSpace(kind)
	if kind == "" {
		return saveFileResult{}, fmt.Errorf("kind is not configured for this tool; use the matching save_* tool")
	}
	title := strings.TrimSpace(args.Title)
	if title == "" {
		return saveFileResult{}, fmt.Errorf("title is required; pass a non-empty title")
	}
	body := args.Content
	if body == "" {
		body = args.Summary
	}
	vpath, err := b.resolveEngramSavePath(ctx, kind, title, args.ObjectID, args.Namespace)
	if err != nil {
		return saveFileResult{}, err
	}
	if err := b.deps.VFS.MkdirAll(ctx, path.Dir(vpath)); err != nil {
		return saveFileResult{}, err
	}
	f := brain.EngramFile{
		Kind:       kind,
		Slug:       brain.Slugify(title),
		Title:      title,
		Properties: args.Properties,
		Body:       body,
	}
	if id, err := parseOptionalUUID(args.ObjectID, "object_id"); err != nil {
		return saveFileResult{}, err
	} else if id != nil {
		f.ID = *id
	} else {
		// Allocate before write so commit skips vfs_path lookup and we skip a post-write Get.
		f.ID = uuid.New()
	}
	raw, err := brain.FormatEngram(f)
	if err != nil {
		return saveFileResult{}, err
	}
	if err := b.deps.VFS.WriteFile(ctx, vpath, raw); err != nil {
		return saveFileResult{}, err
	}
	return saveFileResult{
		Path:     vpath,
		Rev:      vfs.ContentHash(string(raw)),
		ObjectID: f.ID,
		Kind:     kind,
		Title:    title,
	}, nil
}

func (b brainTools) resolveEngramSavePath(ctx context.Context, kind, title, objectID string, ns []brain.Attr) (string, error) {
	spec, ok := b.brainMountForKind(kind)
	if !ok {
		return "", fmt.Errorf("brain files are not mounted; save as a knowledge object instead of a file, or ask the host to mount the brain VFS")
	}
	if id, err := parseOptionalUUID(objectID, "object_id"); err != nil {
		return "", err
	} else if id != nil {
		scope, err := b.scope(ns)
		if err != nil {
			return "", err
		}
		obj, err := b.engine.Read(ctx, scope, *id)
		if err != nil {
			return "", err
		}
		if p := vfsPathFromProps(obj.Properties); p != "" {
			return vfs.CleanPath(p)
		}
		return "", fmt.Errorf("object_id %s has no vfs_path; cannot update as file", id)
	}
	slug := brain.Slugify(title)
	if slug == "" {
		slug = "note"
	}
	mode := brain.ModePrefix
	if spec.Params != nil && spec.Params["mode"] != "" {
		mode = spec.Params["mode"]
	}
	base := brain.EngramPath(spec.Point, mode, kind, slug)
	if _, err := b.deps.VFS.Stat(ctx, base); err == nil {
		base = strings.TrimSuffix(base, ".md") + "-" + uuid.New().String()[:8] + ".md"
	} else if !errors.Is(err, vfs.ErrNotExist) {
		return "", err
	}
	return base, nil
}

// resolveFileRef resolves path (preferred) or object_id to a first-class UUID.
// Engram paths resolve via object vfs_path; artifact paths need an indexed Document.
func (b brainTools) resolveFileRef(ctx context.Context, scope brain.Scope, pathStr, idStr, field string, requireFirstClass bool) (uuid.UUID, string, error) {
	p := strings.TrimSpace(pathStr)
	if p != "" {
		abs, err := vfs.CleanPath(p)
		if err != nil {
			return uuid.Nil, "", err
		}
		obj, err := b.engine.GetByProperty(ctx, scope, brain.PropVFSPath, abs)
		if err == nil {
			if requireFirstClass && obj.ParentID != nil {
				return uuid.Nil, "", fmt.Errorf("%s must be a first-class object (not a chunk)", field)
			}
			return obj.ID, abs, nil
		}
		if !errors.Is(err, brain.ErrNotFound) {
			return uuid.Nil, "", fmt.Errorf("brain: lookup %s: %w", abs, err)
		}
		if b.deps.Indexer != nil {
			id := b.deps.Indexer.DocumentID(abs)
			obj, err := b.engine.Read(ctx, scope, id)
			if err != nil {
				if errors.Is(err, brain.ErrNotFound) {
					return uuid.Nil, "", fmt.Errorf("%s path %s is not indexed (index_file first)", field, abs)
				}
				return uuid.Nil, "", err
			}
			if requireFirstClass && obj.ParentID != nil {
				return uuid.Nil, "", fmt.Errorf("%s must be a first-class object (not a chunk)", field)
			}
			return id, abs, nil
		}
		return uuid.Nil, "", fmt.Errorf("%s path %s is not an Engram and is not indexed (index_file first)", field, abs)
	}
	idLabel := "object_id"
	if field != "expand" {
		idLabel = field + "_id"
	}
	if strings.TrimSpace(idStr) == "" {
		if field == "expand" {
			return uuid.Nil, "", fmt.Errorf("expand: path or object_id is required")
		}
		return uuid.Nil, "", fmt.Errorf("%s or %s_id is required", field, field)
	}
	id, err := parseUUID(idStr, idLabel)
	if err != nil {
		return uuid.Nil, "", err
	}
	obj, err := b.engine.Read(ctx, scope, id)
	if err != nil && !errors.Is(err, brain.ErrNotFound) {
		return uuid.Nil, "", err
	}
	var vpath string
	if err == nil {
		vpath, _ = obj.Properties[brain.PropVFSPath].(string)
	}
	return id, vpath, nil
}

func vfsPathFromProps(props map[string]any) string {
	if props == nil {
		return ""
	}
	s, _ := props[brain.PropVFSPath].(string)
	return strings.TrimSpace(s)
}

func annotateFindLinksPaths(res brain.FindLinksResult) findLinksResultView {
	out := findLinksResultView{Links: make([]findLinkHitView, 0, len(res.Links))}
	for _, h := range res.Links {
		out.Links = append(out.Links, findLinkHitView{
			From:         h.From,
			To:           h.To,
			FromPath:     vfsPathFromProps(h.From.Properties),
			ToPath:       vfsPathFromProps(h.To.Properties),
			RelationType: h.RelationType,
			Meta:         h.Meta,
			Score:        h.Score,
		})
	}
	return out
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
// find_objects only when GraphObjectSearcher is available; find_links when GraphEdgeSearcher is available.
// When a brain Provider is mounted, save_* write Engram Markdown; link/expand accept paths.
func newBrainTools(engine *brain.Engine, sc *brain.SearchContext, kinds brain.WriteKinds, deps brainToolDeps) []*Tool {
	b := brainTools{engine: engine, sc: sc, deps: deps}
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
	if engine.HasEdgeSearch() {
		tools = append(tools, b.newFindLinksTool())
	}
	for _, s := range []struct {
		name, display, kind, role string
	}{
		{"save_discovery", "Save discovery: {title}", kinds.Discovery, "working discovery or finding"},
		{"save_fact", "Save fact: {title}", kinds.Fact, "durable fact"},
		{"save_memory", "Save memory: {title}", kinds.Memory, "preference or durable memory"},
	} {
		if kind := strings.TrimSpace(s.kind); kind != "" {
			tools = append(tools, b.newSaveTool(s.name, s.display, kind, s.role))
		}
	}
	if engine.HasGraphWriter() {
		tools = append(tools, b.newLinkTool(), b.newUnlinkTool())
	}
	return tools
}
