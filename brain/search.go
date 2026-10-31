package brain

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// SearchRequest is one search: free text, an optional kind, and filters on
// the records' fields. Without a query, a kind or filters list the matching
// records. Within limits the search to the parts of one record.
type SearchRequest struct {
	Query   string
	Kind    string // optional; with registered kinds, one of them
	Filters Filter
	// Within is a record, or a part (meaning its record); the results are
	// that record's parts that match the query.
	Within uuid.UUID
	Limit  int
}

// Search ranks records for a query in one list: the store's records and
// parts (with the session's messages when Scope.SessionID is set) and, with
// a graph, its objects and the links whose notes match. Each backend ranks
// its own data; Search fuses the lists by reciprocal rank, so a record found
// by several sources ranks higher, and reads every result from the store.
//
// A query that is a UUID returns that object. A query in double quotes is
// matched literally: only the store's exact search runs. Links rank only in
// a search without a kind or filters, since filters describe records. When
// the graph fails, Search returns the store's results unless
// FailOnGraphError is set.
//
// With no query, a kind or filters list the matching records newest first,
// from the store. Within searches only the store, for that record's parts:
// the graph's objects and links are other records, so they do not apply.
func (e *Engine) Search(ctx context.Context, scope Scope, req SearchRequest) (Page, error) {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return Page{}, err
	}
	query := strings.TrimSpace(req.Query)
	narrowed := strings.TrimSpace(req.Kind) != "" || !req.Filters.empty()
	switch {
	case query == "" && req.Within != uuid.Nil:
		return Page{}, fmt.Errorf("%w: within needs a query; related lists a record's parts", ErrInvalid)
	case query == "" && !narrowed:
		return Page{}, fmt.Errorf("%w: give a query, or a kind or filters to list records", ErrInvalid)
	}
	filters, err := e.searchFilters(req.Kind, req.Filters)
	if err != nil {
		return Page{}, err
	}
	limit := e.normalizeLimit(req.Limit)
	if query == "" {
		return e.list(ctx, scope, filters, limit)
	}
	if id, err := uuid.Parse(query); err == nil && id != uuid.Nil {
		return e.page(ctx, scope, Rest{refs: []ref{{ID: id}}}, limit)
	}
	text, exact := strings.CutPrefix(query, `"`)
	if exact {
		text = strings.TrimSuffix(text, `"`)
	}
	text = strings.TrimSpace(text)
	if req.Within != uuid.Nil {
		return e.searchWithin(ctx, scope, req.Within, text, exact, filters, limit)
	}
	// Links rank only when the caller narrowed nothing: filters describe records.
	withLinks := strings.TrimSpace(req.Kind) == "" && req.Filters.empty()
	sources, err := e.searchSources(ctx, scope, text, exact, withLinks, filters)
	if err != nil {
		return Page{}, err
	}
	refs, err := e.rerank(ctx, scope, text, fuse(defaultRRFk, sources...))
	if err != nil {
		return Page{}, err
	}
	return e.page(ctx, scope, Rest{refs: refs[:min(len(refs), e.cfg.MaxResultSetSize)], filter: filters}, limit)
}

// list pages the records filters match, newest first.
func (e *Engine) list(ctx context.Context, scope Scope, filters Filter, limit int) (Page, error) {
	hits, err := e.store.Search(ctx, scope, RecordSearch{Filters: filters, Limit: e.cfg.MaxResultSetSize})
	if err != nil {
		return Page{}, fmt.Errorf("brain: list records: %w", err)
	}
	refs := make([]ref, len(hits))
	for i, h := range hits {
		refs[i] = ref{ID: h.ID}
	}
	return e.page(ctx, scope, Rest{refs: refs}, limit)
}

// searchWithin ranks the parts of within's record for text; each result is
// a part with the snippet that matched.
func (e *Engine) searchWithin(ctx context.Context, scope Scope, within uuid.UUID, text string, exact bool, filters Filter, limit int) (Page, error) {
	hits, err := e.store.Search(ctx, scope, RecordSearch{Query: text, Filters: filters, Exact: exact, Within: within, Limit: e.cfg.MaxResultSetSize})
	if err != nil {
		return Page{}, fmt.Errorf("brain: store search: %w", err)
	}
	refs := make([]ref, len(hits))
	for i, h := range hits {
		refs[i] = ref{ID: h.ID, Score: h.Score, Evidence: []Evidence{{
			PartID: h.ID, Title: h.Title, Snippet: snippetAround(h.Content, text, e.cfg.SnippetCap), Score: h.Score, Position: h.Position,
		}}}
	}
	return e.page(ctx, scope, Rest{refs: refs}, limit)
}

// searchSources runs the store and, with a graph, its object and link
// searches at once, and returns their ranked lists. A graph error is
// logged and its list left empty, unless FailOnGraphError is set.
func (e *Engine) searchSources(ctx context.Context, scope Scope, text string, exact, withLinks bool, filters Filter) ([][]ref, error) {
	sources := make([][]ref, 3)
	errs := make([]error, 3)
	var wg sync.WaitGroup
	wg.Go(func() {
		storeScope := scope
		storeScope.SessionID = sessionSearchID(scope, filters)
		hits, err := e.store.Search(ctx, storeScope, RecordSearch{Query: text, Filters: filters, Exact: exact, Limit: e.cfg.CandidateK})
		if err != nil {
			errs[0] = fmt.Errorf("brain: store search: %w", err)
			return
		}
		promoted := promoteParents(hits, e.cfg.EvidenceN, text, e.cfg.SnippetCap)
		refs := make([]ref, len(promoted))
		for i, p := range promoted {
			refs[i] = ref{ID: p.ParentID, Evidence: p.Evidence}
		}
		sources[0] = refs
	})
	if e.graph != nil && !exact {
		wg.Go(func() {
			hits, err := e.graph.FindObjects(ctx, ObjectSearch{Namespace: scope.Namespace, Principals: scope.Principals, Query: text, Filters: filters, Limit: e.cfg.CandidateK})
			if err != nil {
				errs[1] = fmt.Errorf("brain: graph object search: %w", err)
				return
			}
			refs := make([]ref, len(hits))
			for i, h := range hits {
				refs[i] = ref{ID: h.ID, Check: true}
			}
			sources[1] = refs
		})
		if withLinks {
			wg.Go(func() {
				hits, err := e.graph.FindLinks(ctx, LinkSearch{Namespace: scope.Namespace, Principals: scope.Principals, Query: text, Limit: e.cfg.CandidateK})
				if err != nil {
					errs[2] = fmt.Errorf("brain: graph link search: %w", err)
					return
				}
				refs := make([]ref, len(hits))
				for i, h := range hits {
					refs[i] = ref{Link: &linkRef{From: h.FromID, To: h.ToID, Relation: h.RelationType, Meta: h.Meta}}
				}
				sources[2] = refs
			})
		}
	}
	wg.Wait()
	if graphErr := errors.Join(errs[1:]...); graphErr != nil && errs[0] == nil && !e.cfg.FailOnGraphError {
		slog.WarnContext(ctx, "graph search failed; returning the store's results", "error", graphErr)
		return sources, nil
	}
	return sources, errors.Join(errs...)
}

// searchFilters checks kind and filters against the catalog and returns the
// filters the backends apply: kind joins them, and with registered kinds an
// empty kind filter becomes every registered kind.
func (e *Engine) searchFilters(kind string, f Filter) (Filter, error) {
	f = f.clone()
	if kind = strings.TrimSpace(kind); kind != "" {
		f.Kind = StringMatch{Eq: kind}
	}
	if err := f.ValidateAgainst(e.catalog); err != nil {
		return Filter{}, err
	}
	if e.catalog.Empty() {
		return f, nil
	}
	e.catalog.Freeze()
	return f.withKindAllowList(e.catalog), nil
}

// keepOrder is the rerank step without a reranker: the fused order.
func keepOrder(_ context.Context, _ Scope, _ string, refs []ref) ([]ref, error) {
	return refs, nil
}

// rerankHead asks fn to reorder the objects among the first CandidateK
// refs for query; links and refs past CandidateK keep their places, and
// objects fn leaves out are dropped.
func (e *Engine) rerankHead(ctx context.Context, scope Scope, query string, refs []ref, fn RerankFunc) ([]ref, error) {
	head := refs[:min(e.cfg.CandidateK, len(refs))]
	page, err := e.read(ctx, scope, head, FilterPlan{})
	if err != nil {
		return nil, err
	}
	objs := slices.Collect(page.Objects())
	if len(objs) == 0 {
		return refs, nil
	}
	ordered, err := fn(ctx, query, objs)
	if err != nil {
		return nil, fmt.Errorf("brain: rerank: %w", err)
	}
	byID := make(map[uuid.UUID]ref, len(head))
	var slots []int
	for i, r := range head {
		if r.Link == nil {
			byID[r.ID] = r
			slots = append(slots, i)
		}
	}
	out := append([]ref(nil), refs...)
	n := 0
	for _, o := range ordered {
		if r, ok := byID[o.ID]; ok && n < len(slots) {
			delete(byID, o.ID)
			out[slots[n]] = r
			n++
		}
	}
	// Objects the reranker dropped are dropped from the head.
	for _, i := range slots[n:] {
		out[i] = ref{}
	}
	return deleteZeroRefs(out), nil
}

func deleteZeroRefs(refs []ref) []ref {
	out := refs[:0]
	for _, r := range refs {
		if r.ID != uuid.Nil || r.Link != nil {
			out = append(out, r)
		}
	}
	return out
}

// uniqueUUIDs drops nil and repeated ids, keeping the first order.
func uniqueUUIDs(ids []uuid.UUID) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(ids))
	out := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
