package brain

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// Search runs hybrid retrieval (BM25 + optional vector), RRF, temporal decay,
// parent promotion, and materializes a ResultSet into results.
func (e *Engine) Search(ctx context.Context, scope Scope, req SearchRequest, results ResultSetStore) (SearchPage, error) {
	ctx, span := e.observer.StartOp(ctx, OpSearch)
	page, degrade, err := e.searchWithDegrade(ctx, scope, req, results, false)
	span.End(len(page.Objects), degrade, err)
	return page, err
}

// FindExact runs equality-first exact retrieval (no dense channel), then
// lexical + trigram fusion, promotion, and ResultSet materialization.
func (e *Engine) FindExact(ctx context.Context, scope Scope, req SearchRequest, results ResultSetStore) (SearchPage, error) {
	ctx, span := e.observer.StartOp(ctx, OpFindExact)
	page, degrade, err := e.searchWithDegrade(ctx, scope, req, results, true)
	span.End(len(page.Objects), degrade, err)
	return page, err
}

func (e *Engine) searchWithDegrade(ctx context.Context, scope Scope, req SearchRequest, results ResultSetStore, exact bool) (SearchPage, DegradeMode, error) {
	var (
		ranked  []ScoredID
		err     error
		degrade = DegradeNone
	)
	if exact {
		ranked, err = e.exactCandidates(ctx, scope, req)
	} else {
		ranked, degrade, err = e.hybridCandidates(ctx, scope, req)
	}
	if err != nil {
		return SearchPage{}, degrade, err
	}
	ranked = filterScoredByScopeIDs(ranked, req.ScopeIDs)
	page, err := e.materialize(ctx, scope, ranked, req.Limit, results)
	return page, degrade, err
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
func (e *Engine) Continue(ctx context.Context, scope Scope, resultSetID uuid.UUID, limit int, results ResultSetStore) (SearchPage, error) {
	ctx, span := e.observer.StartOp(ctx, OpContinue)
	page, err := e.continueInner(ctx, scope, resultSetID, limit, results)
	span.End(len(page.Objects), DegradeNone, err)
	return page, err
}

func (e *Engine) continueInner(ctx context.Context, scope Scope, resultSetID uuid.UUID, limit int, results ResultSetStore) (SearchPage, error) {
	if resultSetID == uuid.Nil {
		return SearchPage{}, fmt.Errorf("brain: result_set_id is required")
	}
	limit = e.normalizeLimit(limit)
	set, err := results.Get(ctx, resultSetID)
	if err != nil {
		return SearchPage{}, err
	}
	start, end := sliceBounds(set.Offset, limit, len(set.ObjectIDs))
	objs, err := e.hydrateParents(ctx, scope, set.ObjectIDs[start:end])
	if err != nil {
		return SearchPage{}, err
	}
	set.Offset = end
	if err := results.Put(ctx, set); err != nil {
		return SearchPage{}, err
	}
	return SearchPage{
		ResultSetID: resultSetID,
		HasMore:     end < len(set.ObjectIDs),
		Objects:     objs,
	}, nil
}

func (e *Engine) materialize(ctx context.Context, scope Scope, ranked []ScoredID, limit int, results ResultSetStore) (SearchPage, error) {
	limit = e.normalizeLimit(limit)
	applyTemporal(ranked, e.cfg.lambdaValue(), e.cfg.Now())
	sortScored(ranked)
	promoted := promoteParents(ranked, e.cfg.EvidenceN)

	ids := make([]uuid.UUID, len(promoted))
	byID := make(map[uuid.UUID]promotedParent, len(promoted))
	for i, p := range promoted {
		ids[i] = p.ParentID
		byID[p.ParentID] = p
	}
	page, err := e.pageIDs(ctx, scope, ids, limit, results)
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

// prepareSearch validates the query/filters and returns effective filters for the store.
// When the catalog is non-empty, property filters require kind, unregistered kinds are
// rejected, and missing kind filters are expanded to all registered kinds.
func (e *Engine) prepareSearch(req SearchRequest) (Filters, error) {
	if strings.TrimSpace(req.Query) == "" {
		return nil, fmt.Errorf("brain: query is required")
	}
	return e.effectiveFilters(req.Filters)
}

func (e *Engine) effectiveFilters(f Filters) (Filters, error) {
	if err := ValidateFiltersAgainst(f, e.catalog); err != nil {
		return nil, err
	}
	if e.catalog.Empty() {
		return f, nil
	}
	e.catalog.Freeze()
	// injectKindAllowList returns f unchanged when kind is already set (no clone).
	return injectKindAllowList(f, e.catalog), nil
}

func (e *Engine) hybridCandidates(ctx context.Context, scope Scope, req SearchRequest) ([]ScoredID, DegradeMode, error) {
	filters, err := e.prepareSearch(req)
	if err != nil {
		return nil, DegradeNone, err
	}
	query := strings.TrimSpace(req.Query)
	k := e.cfg.CandidateK
	lex, err := e.store.SearchLexical(ctx, scope, query, filters, k)
	if err != nil {
		return nil, DegradeNone, err
	}
	lists := [][]ScoredID{lex}
	degrade := DegradeNone
	if e.embedder != nil {
		emb, embErr := e.embedder.Embed(ctx, query)
		if embErr != nil {
			if e.cfg.allowEmbedderDegrade() {
				degrade = DegradeLexicalOnly
			} else {
				return nil, DegradeNone, fmt.Errorf("brain: embed query: %w", embErr)
			}
		} else if len(emb) > 0 {
			vec, err := e.store.SearchVector(ctx, scope, emb, filters, k)
			if err != nil {
				if e.cfg.allowEmbedderDegrade() {
					degrade = DegradeLexicalOnly
				} else {
					return nil, DegradeNone, err
				}
			} else if len(vec) > 0 {
				lists = append(lists, vec)
			}
		}
	}
	return rrfFuse(lists, e.cfg.RRFk), degrade, nil
}

func (e *Engine) exactCandidates(ctx context.Context, scope Scope, req SearchRequest) ([]ScoredID, error) {
	filters, err := e.prepareSearch(req)
	if err != nil {
		return nil, err
	}
	query := strings.TrimSpace(req.Query)

	if id, err := uuid.Parse(query); err == nil && id != uuid.Nil {
		obj, err := e.store.Get(ctx, scope, id)
		if err != nil {
			return nil, err
		}
		return []ScoredID{{
			ID:        obj.ID,
			Score:     1,
			UpdatedAt: obj.UpdatedAt,
			ParentID:  obj.ParentID,
			Title:     obj.Title,
			Content:   obj.Content,
			Position:  obj.Position,
		}}, nil
	}

	k := e.cfg.CandidateK
	lex, err := e.store.SearchLexical(ctx, scope, query, filters, k)
	if err != nil {
		return nil, err
	}
	tri, err := e.store.SearchTrigram(ctx, scope, query, filters, k)
	if err != nil {
		return nil, err
	}

	fused := rrfFuse([][]ScoredID{lex, tri}, e.cfg.RRFk)
	q := strings.ToLower(query)
	// Prefer exact title matches (boost), then remaining fused candidates.
	boosted := make([]ScoredID, 0)
	rest := make([]ScoredID, 0, len(fused))
	for _, item := range fused {
		if strings.ToLower(strings.TrimSpace(item.Title)) == q {
			item.Score += 10
			boosted = append(boosted, item)
			continue
		}
		rest = append(rest, item)
	}
	if len(boosted) == 0 {
		return fused, nil
	}
	return append(boosted, rest...), nil
}

func (e *Engine) hydrateParents(ctx context.Context, scope Scope, ids []uuid.UUID) ([]RichObject, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	objs, err := e.store.GetMany(ctx, scope, ids)
	if err != nil {
		return nil, fmt.Errorf("brain: hydrate parents: %w", err)
	}
	byID := make(map[uuid.UUID]Object, len(objs))
	for _, o := range objs {
		byID[o.ID] = o
	}
	out := make([]RichObject, 0, len(ids))
	for _, id := range ids {
		obj, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("brain: hydrate parent %s: %w", id, ErrNotFound)
		}
		out = append(out, RichFromObject(obj, false))
	}
	return out, nil
}

func (e *Engine) pageIDs(ctx context.Context, scope Scope, ids []uuid.UUID, limit int, results ResultSetStore) (SearchPage, error) {
	limit = e.normalizeLimit(limit)
	if maxN := e.cfg.MaxResultSetSize; maxN > 0 && len(ids) > maxN {
		ids = ids[:maxN]
	}
	_, end := sliceBounds(0, limit, len(ids))
	pageObjs, err := e.hydrateParents(ctx, scope, ids[:end])
	if err != nil {
		return SearchPage{}, err
	}
	set := ResultSet{
		ID:        uuid.New(),
		ObjectIDs: slices.Clone(ids),
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

func sliceBounds(offset, limit, n int) (start, end int) {
	start = min(max(offset, 0), n)
	end = min(start+limit, n)
	return start, end
}
