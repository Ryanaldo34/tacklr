package brain

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Search runs hybrid retrieval (BM25 + optional vector), RRF, temporal decay,
// parent promotion, and materializes a ResultSet into results.
func (e *Engine) Search(ctx context.Context, scope Scope, req SearchRequest, results ResultSetStore) (SearchPage, error) {
	ranked, err := e.hybridCandidates(ctx, scope, req)
	if err != nil {
		return SearchPage{}, err
	}
	return e.materialize(ctx, scope, ranked, req.Limit, results)
}

// FindExact runs equality-first exact retrieval (no dense channel), then
// lexical + trigram fusion, promotion, and ResultSet materialization.
func (e *Engine) FindExact(ctx context.Context, scope Scope, req SearchRequest, results ResultSetStore) (SearchPage, error) {
	ranked, err := e.exactCandidates(ctx, scope, req)
	if err != nil {
		return SearchPage{}, err
	}
	return e.materialize(ctx, scope, ranked, req.Limit, results)
}

// Continue returns the next page of a prior ResultSet under scope.
func (e *Engine) Continue(ctx context.Context, scope Scope, resultSetID uuid.UUID, limit int, results ResultSetStore) (SearchPage, error) {
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

func (e *Engine) prepareSearch(req SearchRequest) (PartSearcher, error) {
	if strings.TrimSpace(req.Query) == "" {
		return nil, fmt.Errorf("brain: query is required")
	}
	if err := ValidateFilters(req.Filters); err != nil {
		return nil, err
	}
	ps, ok := e.store.(PartSearcher)
	if !ok {
		return nil, fmt.Errorf("brain: store does not support search")
	}
	return ps, nil
}

func (e *Engine) hybridCandidates(ctx context.Context, scope Scope, req SearchRequest) ([]ScoredID, error) {
	ps, err := e.prepareSearch(req)
	if err != nil {
		return nil, err
	}
	query := strings.TrimSpace(req.Query)
	k := e.cfg.CandidateK
	lex, err := ps.SearchLexical(ctx, scope, query, req.Filters, k)
	if err != nil {
		return nil, err
	}
	lists := [][]ScoredID{lex}
	if e.embedder != nil {
		emb, err := e.embedder.Embed(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("brain: embed query: %w", err)
		}
		if len(emb) > 0 {
			vec, err := ps.SearchVector(ctx, scope, emb, req.Filters, k)
			if err != nil {
				return nil, err
			}
			if len(vec) > 0 {
				lists = append(lists, vec)
			}
		}
	}
	return rrfFuse(lists, e.cfg.RRFk), nil
}

func (e *Engine) exactCandidates(ctx context.Context, scope Scope, req SearchRequest) ([]ScoredID, error) {
	ps, err := e.prepareSearch(req)
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
	lex, err := ps.SearchLexical(ctx, scope, query, req.Filters, k)
	if err != nil {
		return nil, err
	}
	tri, err := ps.SearchTrigram(ctx, scope, query, req.Filters, k)
	if err != nil {
		return nil, err
	}

	fused := rrfFuse([][]ScoredID{lex, tri}, e.cfg.RRFk)
	q := strings.ToLower(query)
	seen := map[uuid.UUID]struct{}{}
	var out []ScoredID
	for _, list := range [][]ScoredID{lex, tri} {
		for _, item := range list {
			if strings.ToLower(strings.TrimSpace(item.Title)) != q {
				continue
			}
			if _, ok := seen[item.ID]; ok {
				continue
			}
			seen[item.ID] = struct{}{}
			item.Score += 10
			out = append(out, item)
		}
	}
	if len(out) == 0 {
		return fused, nil
	}
	for _, p := range fused {
		if _, ok := seen[p.ID]; ok {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

func (e *Engine) hydrateParents(ctx context.Context, scope Scope, ids []uuid.UUID) ([]RichObject, error) {
	out := make([]RichObject, 0, len(ids))
	for _, id := range ids {
		obj, err := e.store.Get(ctx, scope, id)
		if err != nil {
			return nil, fmt.Errorf("brain: hydrate parent %s: %w", id, err)
		}
		out = append(out, RichFromObject(obj, false))
	}
	return out, nil
}

// pageIDs materializes ordered ids into a ResultSet and returns the first page.
func (e *Engine) pageIDs(ctx context.Context, scope Scope, ids []uuid.UUID, limit int, results ResultSetStore) (SearchPage, error) {
	limit = e.normalizeLimit(limit)
	_, end := sliceBounds(0, limit, len(ids))
	pageObjs, err := e.hydrateParents(ctx, scope, ids[:end])
	if err != nil {
		return SearchPage{}, err
	}
	set := ResultSet{
		ID:        uuid.New(),
		ObjectIDs: append([]uuid.UUID(nil), ids...),
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

// sliceBounds returns [start,end) for a page into a slice of length n.
func sliceBounds(offset, limit, n int) (start, end int) {
	start = min(max(offset, 0), n)
	end = min(start+limit, n)
	return start, end
}
