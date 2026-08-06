package brain

import (
	"context"
	"fmt"
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
	if e.graphS == nil {
		return SearchPage{}, DegradeNone, ErrObjectSearchUnavailable
	}
	if results == nil {
		return SearchPage{}, DegradeNone, ErrResultSetRequired
	}
	query := strings.TrimSpace(req.Query)
	if query == "" {
		return SearchPage{}, DegradeNone, ErrQueryRequired
	}
	limit := e.normalizeLimit(req.Limit)
	k := max(limit*3, e.cfg.CandidateK)
	ns := scope.Namespace
	degrade := DegradeNone

	var lists [][]ScoredID
	textHits, err := e.graphS.SearchText(ctx, query, k, ns)
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
			vecHits, vErr := e.graphS.SearchVector(ctx, emb, k, ns)
			if vErr != nil {
				if !e.cfg.allowEmbedderDegrade() {
					return SearchPage{}, DegradeNone, fmt.Errorf("brain: vector search: %w", vErr)
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

	ids := make([]uuid.UUID, len(ranked))
	scoreByID := make(map[uuid.UUID]float64, len(ranked))
	for i, s := range ranked {
		ids[i] = s.ID
		scoreByID[s.ID] = s.Score
	}
	objs, err := e.hydrateIDs(ctx, scope, ids)
	if err != nil {
		return SearchPage{}, degrade, err
	}
	kinds := kindSet(req.Kinds)
	rich := make([]RichObject, 0, len(objs))
	for _, r := range objs {
		if r.ParentID != nil {
			continue
		}
		if kinds != nil {
			if _, want := kinds[r.Kind]; !want {
				continue
			}
		}
		if sc, ok := scoreByID[r.ID]; ok {
			sc := sc
			r.Score = &sc
		}
		rich = append(rich, r)
	}
	if maxN := e.cfg.MaxResultSetSize; maxN > 0 && len(rich) > maxN {
		rich = rich[:maxN]
	}
	// Single ID list derived from filtered rich (no parallel keptIDs slice during filter).
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
