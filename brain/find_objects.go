package brain

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// FindObjectsRequest is the engine input for entity/object find (graph node search).
type FindObjectsRequest struct {
	Query string
	Kinds []string // optional host kind names; empty = all kinds
	Limit int
}

// FindObjects ranks knowledge objects as entities via GraphObjectSearcher
// (Helix text/vector or MemoryGraph), then hydrates under scope from the store.
// Not a substitute for corpus Search: no part promotion evidence path.
func (e *Engine) FindObjects(ctx context.Context, scope Scope, req FindObjectsRequest, results ResultSetStore) (SearchPage, error) {
	ctx, span := e.observer.StartOp(ctx, OpFindObjects)
	page, degrade, err := e.findObjectsInner(ctx, scope, req, results)
	span.End(len(page.Objects), degrade, err)
	return page, err
}

func (e *Engine) findObjectsInner(ctx context.Context, scope Scope, req FindObjectsRequest, results ResultSetStore) (SearchPage, DegradeMode, error) {
	gs, ok := e.graph.(GraphObjectSearcher)
	if !ok {
		return SearchPage{}, DegradeNone, fmt.Errorf("brain: graph object search is not available")
	}
	if results == nil {
		return SearchPage{}, DegradeNone, fmt.Errorf("brain: result set store is required")
	}
	query := strings.TrimSpace(req.Query)
	if query == "" {
		return SearchPage{}, DegradeNone, fmt.Errorf("brain: query is required")
	}
	limit := e.normalizeLimit(req.Limit)
	k := max(limit*3, e.cfg.CandidateK)
	ns := scope.Namespace
	degrade := DegradeNone

	var lists [][]ScoredID
	textHits, err := gs.SearchText(ctx, query, k, ns)
	if err != nil {
		return SearchPage{}, DegradeNone, err
	}
	if len(textHits) > 0 {
		lists = append(lists, textHits)
	}
	if e.embedder != nil {
		emb, embErr := e.embedder.Embed(ctx, query)
		if embErr != nil {
			if !e.cfg.allowEmbedderDegrade() {
				return SearchPage{}, DegradeNone, fmt.Errorf("brain: embed query: %w", embErr)
			}
			degrade = DegradeLexicalOnly
		} else if len(emb) > 0 {
			vecHits, vErr := gs.SearchVector(ctx, emb, k, ns)
			if vErr != nil {
				if !e.cfg.allowEmbedderDegrade() {
					return SearchPage{}, DegradeNone, vErr
				}
				degrade = DegradeLexicalOnly
			} else if len(vecHits) > 0 {
				lists = append(lists, vecHits)
			}
		}
	}
	if len(lists) == 0 {
		return SearchPage{}, degrade, nil
	}
	ranked := rrfFuse(lists, e.cfg.RRFk)
	applyTemporal(ranked, e.cfg.lambdaValue(), e.cfg.Now())
	sortScored(ranked)

	// Hydrate once: drop missing/soft-deleted/out-of-scope (same as expand graph path).
	ids := make([]uuid.UUID, len(ranked))
	scoreByID := make(map[uuid.UUID]float64, len(ranked))
	for i, s := range ranked {
		ids[i] = s.ID
		scoreByID[s.ID] = s.Score
	}
	if maxN := e.cfg.MaxResultSetSize; maxN > 0 && len(ids) > maxN {
		ids = ids[:maxN]
	}
	objs, err := e.store.GetMany(ctx, scope, ids)
	if err != nil {
		return SearchPage{}, degrade, fmt.Errorf("brain: hydrate objects: %w", err)
	}
	byID := make(map[uuid.UUID]Object, len(objs))
	for _, o := range objs {
		byID[o.ID] = o
	}
	kinds := kindSet(req.Kinds)
	rich := make([]RichObject, 0, len(ids))
	keptIDs := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		o, ok := byID[id]
		if !ok || o.ParentID != nil {
			// Skip invisible and part objects (entity find is parent-shaped).
			continue
		}
		if kinds != nil {
			if _, want := kinds[o.Kind]; !want {
				continue
			}
		}
		r := RichFromObject(o, false)
		if sc, ok := scoreByID[id]; ok {
			sc := sc
			r.Score = &sc
		}
		rich = append(rich, r)
		keptIDs = append(keptIDs, id)
	}
	end := min(limit, len(rich))
	set := ResultSet{
		ID:        uuid.New(),
		ObjectIDs: slices.Clone(keptIDs),
		Offset:    end,
		CreatedAt: e.cfg.Now(),
	}
	if err := results.Put(ctx, set); err != nil {
		return SearchPage{}, degrade, err
	}
	return SearchPage{
		ResultSetID: set.ID,
		HasMore:     end < len(keptIDs),
		Objects:     rich[:end],
	}, degrade, nil
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

// HasObjectSearch reports whether FindObjects / find_objects is available.
func (e *Engine) HasObjectSearch() bool {
	_, ok := e.graph.(GraphObjectSearcher)
	return ok
}
