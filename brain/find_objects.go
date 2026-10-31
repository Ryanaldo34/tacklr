package brain

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// FindObjectsRequest is entity retrieval (graph parent nodes).
// Query matches entity index text (title, summary, indexed properties, body).
// Filters (same keys as schema filterable_fields) apply in the graph before
// it ranks, and again to the hydrated parent row.
// Corpus retrieval is SearchRequest (store parts).
type FindObjectsRequest struct {
	Query   string
	Kinds   []string // optional host kind names; empty = all kinds
	Filters Filter
	Limit   int
}

// FindObjects ranks first-class objects in the graph (Graph.FindObjects),
// then hydrates them under Scope from the store and checks Filters on the
// stored rows. Requires a graph. Not corpus Search: no part evidence path.
// A parent with no parts is visible here and invisible to Search.
func (k *KnowledgeGraph) FindObjects(ctx context.Context, scope Scope, req FindObjectsRequest, results ResultSetStore) (page SearchPage, err error) {
	e := k.eng
	if err := scope.Namespace.Validate(); err != nil {
		return page, err
	}
	if results == nil {
		return page, fmt.Errorf("%w: result set store is required", ErrUnsupported)
	}
	query := strings.TrimSpace(req.Query)
	if query == "" {
		return page, fmt.Errorf("%w: query is required", ErrInvalid)
	}
	filters, err := e.prepareFindObjectFilters(req.Filters, trimmedKinds(req.Kinds))
	if err != nil {
		return page, err
	}
	plan, err := CompileFilter(filters)
	if err != nil {
		return page, err
	}
	limit := e.normalizeLimit(req.Limit)
	ranked, err := k.graph.FindObjects(ctx, ObjectSearch{
		Namespace:  scope.Namespace,
		Principals: scope.Principals,
		Query:      query,
		Filters:    filters,
		Limit:      max(limit*3, e.cfg.CandidateK),
	})
	if err != nil {
		return page, fmt.Errorf("brain: object search: %w", err)
	}
	if len(ranked) == 0 {
		return page, nil
	}
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
	rich := make([]RichObject, 0, len(ids))
	for _, id := range ids {
		o, ok := byID[id]
		if !ok || o.ParentID != nil {
			continue
		}
		if !plan.Match(o) {
			continue
		}
		r := RichFromObject(o, false)
		if sc, ok := scoreByID[id]; ok {
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
		Namespace: scope.Namespace.Clone(),
		Offset:    end,
		CreatedAt: e.cfg.Now(),
	}
	if e := results.Put(ctx, set); e != nil {
		err = e
		return page, err
	}
	if err := e.recordRead(ctx, scope, richIDs(rich[:end])); err != nil {
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

// prepareFindObjectFilters merges kinds into filters and validates against catalog.
func (e *Engine) prepareFindObjectFilters(filters Filter, kinds []string) (Filter, error) {
	f := cloneFilter(filters)
	switch len(kinds) {
	case 0:
	case 1:
		f.Kind = StringMatch{Eq: kinds[0]}
	default:
		f.Kind = StringMatch{In: kinds}
	}
	if f.empty() {
		return Filter{}, nil
	}
	if err := ValidateFiltersAgainst(f, e.catalog); err != nil {
		return Filter{}, err
	}
	if !e.catalog.Empty() {
		e.catalog.Freeze()
	}
	return f, nil
}

// trimmedKinds drops blank kind names.
func trimmedKinds(kinds []string) []string {
	out := make([]string, 0, len(kinds))
	for _, k := range kinds {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return out
}
