package brain

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// Search is corpus retrieval: the store's ranked parts (Store.Search, its
// own channels, fusion and decay), parent promotion, then a ResultSet page.
// Query text matches part title, summary, and content. Structured fields on
// the parent belong in FindObjects filters (or here only if the part row
// carries them). A parent with no parts does not appear. Entity search is
// FindObjects.
func (e *Engine) Search(ctx context.Context, scope Scope, req SearchRequest, results ResultSetStore) (SearchPage, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return SearchPage{}, err
	}
	ranked, err := e.hybridCandidates(ctx, scope, req)
	if err != nil {
		return SearchPage{}, err
	}
	ranked = filterScoredByScopeIDs(ranked, req.ScopeIDs)
	return e.materialize(ctx, scope, ranked, req.Query, req.Limit, results)
}

// FindExact is corpus retrieval by literal match: UUID Get, else the store's
// exact search (PartSearch.Exact), promotion, and ResultSet materialization.
// Same part-only candidate rule as Search. UUID lookup may return a parent.
func (e *Engine) FindExact(ctx context.Context, scope Scope, req SearchRequest, results ResultSetStore) (SearchPage, error) {
	if err := scope.Namespace.Validate(); err != nil {
		return SearchPage{}, err
	}
	ranked, err := e.exactCandidates(ctx, scope, req)
	if err != nil {
		return SearchPage{}, err
	}
	ranked = filterScoredByScopeIDs(ranked, req.ScopeIDs)
	return e.materialize(ctx, scope, ranked, req.Query, req.Limit, results)
}

// filterScoredByScopeIDs keeps hits whose id or parent_id is in allow (empty allow = no-op).
func filterScoredByScopeIDs(ranked []ScoredID, allow []uuid.UUID) []ScoredID {
	if len(allow) == 0 || len(ranked) == 0 {
		return ranked
	}
	set := make(map[uuid.UUID]struct{}, len(allow))
	for _, id := range allow {
		if id != uuid.Nil {
			set[id] = struct{}{}
		}
	}
	if len(set) == 0 {
		return ranked
	}
	out := make([]ScoredID, 0, len(ranked))
	for _, s := range ranked {
		if _, ok := set[s.ID]; ok {
			out = append(out, s)
			continue
		}
		if s.ParentID != nil {
			if _, ok := set[*s.ParentID]; ok {
				out = append(out, s)
			}
		}
	}
	return out
}

// Continue returns the next page of a prior ResultSet under scope.
func (e *Engine) Continue(ctx context.Context, scope Scope, resultSetID uuid.UUID, limit int, results ResultSetStore) (page SearchPage, err error) {
	if err := scope.Namespace.Validate(); err != nil {
		return page, err
	}
	if resultSetID == uuid.Nil {
		return page, fmt.Errorf("%w: result_set_id is required", ErrInvalid)
	}
	limit = e.normalizeLimit(limit)
	set, err := results.Get(ctx, resultSetID)
	if err != nil {
		return page, err
	}
	if !set.Namespace.Empty() {
		scope.Namespace = set.Namespace
	}
	start, end := sliceBounds(set.Offset, limit, len(set.ObjectIDs))
	objs, err := e.hydrateIDs(ctx, scope, set.ObjectIDs[start:end])
	if err != nil {
		return page, err
	}
	if err := e.recordRead(ctx, scope, richIDs(objs)); err != nil {
		return page, err
	}
	attachRelations(objs, set.Relations)
	set.Offset = end
	if e := results.Put(ctx, set); e != nil {
		err = e
		return page, err
	}
	return SearchPage{
		ResultSetID: resultSetID,
		HasMore:     end < len(set.ObjectIDs),
		Objects:     objs,
	}, nil
}

func (e *Engine) materialize(ctx context.Context, scope Scope, ranked []ScoredID, query string, limit int, results ResultSetStore) (SearchPage, error) {
	limit = e.normalizeLimit(limit)
	sortScored(ranked)
	promoted := promoteParents(ranked, e.cfg.EvidenceN, query, e.cfg.SnippetCap)

	ids := make([]uuid.UUID, len(promoted))
	byID := make(map[uuid.UUID]promotedParent, len(promoted))
	for i, p := range promoted {
		ids[i] = p.ParentID
		byID[p.ParentID] = p
	}
	if e.reranker != nil {
		var err error
		ids, err = e.rerankPromoted(ctx, scope, ids, byID)
		if err != nil {
			return SearchPage{}, err
		}
	}
	page, err := e.pageIDs(ctx, scope, ids, limit, results, nil)
	if err != nil {
		return SearchPage{}, err
	}
	for i := range page.Objects {
		if p, ok := byID[page.Objects[i].ID]; ok {
			sc := p.Score
			page.Objects[i].Score = &sc
			page.Objects[i].Evidence = p.Evidence
		}
	}
	return page, nil
}

// rerankPromoted asks the host reranker to order up to CandidateK parents, then
// keeps any parents beyond that cutoff in their original order.
func (e *Engine) rerankPromoted(ctx context.Context, scope Scope, ids []uuid.UUID, byID map[uuid.UUID]promotedParent) ([]uuid.UUID, error) {
	if len(ids) == 0 {
		return ids, nil
	}
	n := min(e.cfg.CandidateK, len(ids))
	head, err := e.hydrateIDs(ctx, scope, ids[:n])
	if err != nil {
		return nil, err
	}
	for i := range head {
		if p, ok := byID[head[i].ID]; ok {
			sc := p.Score
			head[i].Score = &sc
			head[i].Evidence = p.Evidence
		}
	}
	head, err = e.applyRerank(ctx, head)
	if err != nil {
		return nil, err
	}
	out := make([]uuid.UUID, 0, len(head)+len(ids)-n)
	seen := make(map[uuid.UUID]struct{}, len(head))
	for _, o := range head {
		if _, ok := seen[o.ID]; ok {
			continue
		}
		seen[o.ID] = struct{}{}
		out = append(out, o.ID)
	}
	for _, id := range ids[n:] {
		if _, ok := seen[id]; ok {
			continue
		}
		out = append(out, id)
	}
	return out, nil
}

// prepareSearch validates the query/filters and returns effective filters for the store.
// When the catalog is non-empty, property filters require kind, unregistered kinds are
// rejected, and missing kind filters are expanded to all registered kinds.
func (e *Engine) prepareSearch(req SearchRequest) (Filter, error) {
	if strings.TrimSpace(req.Query) == "" {
		return Filter{}, fmt.Errorf("%w: query is required", ErrInvalid)
	}
	return e.effectiveFilters(req.Filters)
}

func (e *Engine) effectiveFilters(f Filter) (Filter, error) {
	if err := ValidateFiltersAgainst(f, e.catalog); err != nil {
		return Filter{}, err
	}
	if e.catalog.Empty() {
		return f, nil
	}
	e.catalog.Freeze()
	return injectKindAllowList(f, e.catalog), nil
}

func (e *Engine) hybridCandidates(ctx context.Context, scope Scope, req SearchRequest) ([]ScoredID, error) {
	filters, err := e.prepareSearch(req)
	if err != nil {
		return nil, err
	}
	return e.searchParts(ctx, scope, req, filters, false)
}

func (e *Engine) exactCandidates(ctx context.Context, scope Scope, req SearchRequest) ([]ScoredID, error) {
	filters, err := e.prepareSearch(req)
	if err != nil {
		return nil, err
	}
	if id, err := uuid.Parse(strings.TrimSpace(req.Query)); err == nil && id != uuid.Nil {
		obj, err := e.store.Get(ctx, scope, id)
		if err != nil {
			return nil, err
		}
		return []ScoredID{{
			ID:         obj.ID,
			Score:      1,
			UpdatedAt:  obj.UpdatedAt,
			ParentID:   obj.ParentID,
			Title:      obj.Title,
			Content:    obj.Content,
			Position:   obj.Position,
			Properties: obj.Properties,
		}}, nil
	}
	return e.searchParts(ctx, scope, req, filters, true)
}

// searchParts asks the store for its ranked parts.
func (e *Engine) searchParts(ctx context.Context, scope Scope, req SearchRequest, filters Filter, exact bool) ([]ScoredID, error) {
	scope.SessionID = sessionSearchID(scope, req.Filters)
	return e.store.Search(ctx, scope, PartSearch{Query: strings.TrimSpace(req.Query), Filters: filters, Exact: exact, Limit: e.cfg.CandidateK})
}

// hydrateIDs loads objects for ids in request order, skipping missing/out-of-scope.
func (e *Engine) hydrateIDs(ctx context.Context, scope Scope, ids []uuid.UUID) ([]RichObject, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	objs, err := e.store.GetMany(ctx, scope, ids)
	if err != nil {
		return nil, fmt.Errorf("brain: hydrate: %w", err)
	}
	byID := make(map[uuid.UUID]Object, len(objs))
	for _, o := range objs {
		byID[o.ID] = o
	}
	out := make([]RichObject, 0, len(ids))
	for _, id := range ids {
		obj, ok := byID[id]
		if !ok {
			continue
		}
		out = append(out, RichFromObject(obj, false))
	}
	return out, nil
}

// pageIDs materializes a ResultSet page. relations (optional) is stored for continue.
func (e *Engine) pageIDs(ctx context.Context, scope Scope, ids []uuid.UUID, limit int, results ResultSetStore, relations map[uuid.UUID]Relation) (SearchPage, error) {
	if results == nil {
		return SearchPage{}, fmt.Errorf("%w: result set store is required", ErrUnsupported)
	}
	limit = e.normalizeLimit(limit)
	if maxN := e.cfg.MaxResultSetSize; maxN > 0 && len(ids) > maxN {
		ids = ids[:maxN]
	}
	_, end := sliceBounds(0, limit, len(ids))
	pageObjs, err := e.hydrateIDs(ctx, scope, ids[:end])
	if err != nil {
		return SearchPage{}, err
	}
	if err := e.recordRead(ctx, scope, richIDs(pageObjs)); err != nil {
		return SearchPage{}, err
	}
	attachRelations(pageObjs, relations)
	set := ResultSet{
		ID:        uuid.New(),
		ObjectIDs: slices.Clone(ids),
		Relations: relations,
		Namespace: scope.Namespace.Clone(),
		Offset:    end,
		CreatedAt: e.cfg.Now(),
	}
	if err := results.Put(ctx, set); err != nil {
		return SearchPage{}, err
	}
	return SearchPage{
		ResultSetID: set.ID,
		HasMore:     end < len(ids),
		Objects:     pageObjs,
	}, nil
}

func attachRelations(objs []RichObject, relations map[uuid.UUID]Relation) {
	if len(relations) == 0 {
		return
	}
	for i := range objs {
		if r, ok := relations[objs[i].ID]; ok {
			r := r
			objs[i].Relation = &r
		}
	}
}

func sliceBounds(offset, limit, n int) (start, end int) {
	start = min(max(offset, 0), n)
	end = min(start+limit, n)
	return start, end
}

// LandingIDs returns unique first-class object ids suitable for graph expand / link
// endpoints from rich hits (search, find_exact, find_objects).
// Parts use ParentID; parents use their own ID. Nil / empty parent pointers are skipped.
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
