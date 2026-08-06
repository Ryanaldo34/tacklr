package brain

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"github.com/google/uuid"
)

// FindObjectsRequest is the engine input for entity/object find (graph node search).
type FindObjectsRequest struct {
	Query   string
	Kinds   []string // optional host kind names; empty = all kinds
	Filters Filters  // same property keys as search; see schema filterable_fields
	Limit   int
}

// FindObjects ranks knowledge objects as entities via GraphObjectSearcher
// (Helix text/vector or MemoryGraph), then hydrates under Scope from the store.
// Filters use the same catalog rules as search/find_exact (schema filterable_fields).
// Not a substitute for corpus Search: no part promotion evidence path.
func (e *Engine) FindObjects(ctx context.Context, scope Scope, req FindObjectsRequest, results ResultSetStore) (page SearchPage, err error) {
	ctx, span := e.observer.StartOp(ctx, OpFindObjects)
	degrade := DegradeNone
	defer func() { span.End(len(page.Objects), degrade, err) }()

	if e.graphS == nil {
		return page, ErrObjectSearchUnavailable
	}
	if results == nil {
		return page, ErrResultSetRequired
	}
	query := strings.TrimSpace(req.Query)
	if query == "" {
		return page, ErrQueryRequired
	}
	filters, err := e.prepareFindObjectFilters(req)
	if err != nil {
		return page, err
	}
	limit := e.normalizeLimit(req.Limit)
	k := max(limit*3, e.cfg.CandidateK)
	ns := scope.Namespace

	var lists [][]ScoredID
	textHits, err := e.graphS.SearchText(ctx, query, k, ns)
	if err != nil {
		return page, err
	}
	if len(textHits) > 0 {
		lists = append(lists, textHits)
	}
	if e.embedder != nil {
		emb, embErr := e.embedder.Embed(ctx, query)
		if embErr != nil {
			if !e.cfg.allowEmbedderDegrade() {
				return page, fmt.Errorf("brain: embed query: %w", embErr)
			}
			degrade = DegradeLexicalOnly
		} else if len(emb) > 0 {
			vecHits, vErr := e.graphS.SearchVector(ctx, emb, k, ns)
			if vErr != nil {
				if !e.cfg.allowEmbedderDegrade() {
					return page, fmt.Errorf("brain: vector search: %w", vErr)
				}
				degrade = DegradeLexicalOnly
			} else if len(vecHits) > 0 {
				lists = append(lists, vecHits)
			}
		}
	}
	if len(lists) == 0 {
		return page, nil
	}
	ranked := rrfFuse(lists, e.cfg.RRFk)
	applyTemporal(ranked, e.cfg.lambdaValue(), e.cfg.Now())
	sortScored(ranked)

	ids := make([]uuid.UUID, len(ranked))
	scoreByID := make(map[uuid.UUID]float64, len(ranked))
	for i, s := range ranked {
		ids[i] = s.ID
		scoreByID[s.ID] = s.Score
	}
	// Soft hydrate: load full objects, apply catalog filters on Postgres truth.
	objs, err := e.store.GetMany(ctx, scope, ids)
	if err != nil {
		return page, fmt.Errorf("brain: hydrate objects: %w", err)
	}
	byID := make(map[uuid.UUID]Object, len(objs))
	for _, o := range objs {
		byID[o.ID] = o
	}
	kinds := kindSet(req.Kinds)
	rich := make([]RichObject, 0, len(ids))
	for _, id := range ids {
		o, ok := byID[id]
		if !ok || o.ParentID != nil {
			continue
		}
		if kinds != nil {
			if _, want := kinds[o.Kind]; !want {
				continue
			}
		}
		if len(filters) > 0 && !objectMatchesFilters(o, filters) {
			continue
		}
		r := RichFromObject(o, false)
		if sc, ok := scoreByID[id]; ok {
			sc := sc
			r.Score = &sc
		}
		rich = append(rich, r)
	}
	if maxN := e.cfg.MaxResultSetSize; maxN > 0 && len(rich) > maxN {
		rich = rich[:maxN]
	}
	rich, err = e.applyRerank(ctx, rich)
	if err != nil {
		return page, err
	}
	keptIDs := make([]uuid.UUID, len(rich))
	for i := range rich {
		keptIDs[i] = rich[i].ID
	}
	end := min(limit, len(rich))
	set := ResultSet{
		ID:        uuid.New(),
		ObjectIDs: keptIDs,
		Offset:    end,
		CreatedAt: e.cfg.Now(),
	}
	if e := results.Put(ctx, set); e != nil {
		err = e
		return page, err
	}
	return SearchPage{
		ResultSetID: set.ID,
		HasMore:     end < len(keptIDs),
		Objects:     rich[:end],
	}, nil
}

func (e *Engine) applyRerank(ctx context.Context, objects []RichObject) ([]RichObject, error) {
	if e.reranker == nil || len(objects) == 0 {
		return objects, nil
	}
	out, err := e.reranker.Rerank(ctx, objects)
	if err != nil {
		return nil, fmt.Errorf("brain: rerank: %w", err)
	}
	if out == nil {
		return objects, nil
	}
	return out, nil
}

// prepareFindObjectFilters merges request kinds into filters and validates against catalog.
func (e *Engine) prepareFindObjectFilters(req FindObjectsRequest) (Filters, error) {
	f := Filters(nil)
	if len(req.Filters) > 0 {
		f = maps.Clone(req.Filters)
	}
	if len(req.Kinds) > 0 {
		if f == nil {
			f = Filters{}
		}
		// Explicit kinds win over a conflicting kind key in Filters.
		list := make([]any, 0, len(req.Kinds))
		for _, k := range req.Kinds {
			if k = strings.TrimSpace(k); k != "" {
				list = append(list, k)
			}
		}
		if len(list) == 1 {
			f[filterKind] = list[0]
		} else if len(list) > 1 {
			f[filterKind] = list
		}
	}
	if len(f) == 0 {
		return nil, nil
	}
	if err := ValidateFiltersAgainst(f, e.catalog); err != nil {
		return nil, err
	}
	if !e.catalog.Empty() {
		e.catalog.Freeze()
	}
	return f, nil
}

func kindSet(kinds []string) map[string]struct{} {
	if len(kinds) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(kinds))
	for _, k := range kinds {
		if k = strings.TrimSpace(k); k != "" {
			out[k] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// objectSearchReady is optional: Helix sets false until Bootstrap; MemoryGraph omits it (always ready).
type objectSearchReady interface {
	ObjectSearchReady() bool
}

// HasObjectSearch reports whether FindObjects / find_objects is available.
func (e *Engine) HasObjectSearch() bool {
	if e.graphS == nil {
		return false
	}
	if r, ok := e.graphS.(objectSearchReady); ok {
		return r.ObjectSearchReady()
	}
	return true
}
