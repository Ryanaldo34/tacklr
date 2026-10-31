package brain

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// Result is one ranked result of Search or Related: an object, or a link
// whose note matched a search.
type Result struct {
	Object *RichObject `json:"object,omitempty"`
	Link   *LinkResult `json:"link,omitempty"`
}

// Results are a page's results, best first.
type Results []Result

// Objects yields the objects among rs, in order.
func (rs Results) Objects() iter.Seq[RichObject] {
	return func(yield func(RichObject) bool) {
		for _, r := range rs {
			if r.Object != nil && !yield(*r.Object) {
				return
			}
		}
	}
}

// Links yields the links among rs, in order.
func (rs Results) Links() iter.Seq[LinkResult] {
	return func(yield func(LinkResult) bool) {
		for _, r := range rs {
			if r.Link != nil && !yield(*r.Link) {
				return
			}
		}
	}
}

// LinkResult is a relationship whose note matched, with both of its ends.
type LinkResult struct {
	From     RichObject `json:"from"`
	To       RichObject `json:"to"`
	Relation string     `json:"relation"`
	Meta     EdgeMeta   `json:"meta"`
	Score    float64    `json:"score"`
}

// Page is one page of Search or Related results, best first. More reports
// that Rest holds further results; Next returns the next page of Rest.
type Page struct {
	Results Results `json:"results"`
	Facets  Facets  `json:"facets"`
	More    bool    `json:"more"`
	Rest    Rest    `json:"-"`
}

// Facets count the kinds and the common property values of a page's
// objects, so a caller can narrow a search with values that exist. With
// registered kinds they cover each kind's fields; in open mode, the
// properties with at most maxFacetValues distinct values on the page.
type Facets struct {
	Kinds      map[string]int            `json:"kinds,omitempty"`
	Properties map[string]map[string]int `json:"properties,omitempty"`
}

// maxFacetValues is the most distinct values an open-mode property may have
// on one page and still be a facet: more is an identifier, not a facet.
const maxFacetValues = 5

// Rest is the ranked remainder of a Search or Related call. It holds ids,
// scores and evidence, not rows, so it stays small; Next reads its next
// page from the store. It marshals to JSON, so a caller can keep it across
// processes (a SearchContext does, for the agent tools).
type Rest struct {
	refs   []ref
	filter Filter
}

// ref is one ranked, not yet read result.
type ref struct {
	ID       uuid.UUID  `json:"id,omitempty"`
	Score    float64    `json:"score,omitempty"`
	Evidence []Evidence `json:"evidence,omitempty"`
	Relation *Relation  `json:"relation,omitempty"`
	// Check marks a graph hit, whose stored row must pass Rest.filter.
	Check bool     `json:"check,omitempty"`
	Link  *linkRef `json:"link,omitempty"`
}

// linkRef is a link result before its ends are read.
type linkRef struct {
	From     uuid.UUID `json:"from"`
	To       uuid.UUID `json:"to"`
	Relation string    `json:"relation"`
	Meta     EdgeMeta  `json:"meta"`
}

// key identifies a result across sources: its object, or its edge.
func (r ref) key() string {
	if r.Link != nil {
		return r.Link.From.String() + ">" + r.Link.Relation + ">" + r.Link.To.String()
	}
	return r.ID.String()
}

type restJSON struct {
	Refs   []ref  `json:"refs,omitempty"`
	Filter Filter `json:"filter,omitzero"`
}

// Empty reports whether r holds no results.
func (r Rest) Empty() bool { return len(r.refs) == 0 }

// MarshalJSON implements json.Marshaler.
func (r Rest) MarshalJSON() ([]byte, error) {
	return json.Marshal(restJSON{Refs: r.refs, Filter: r.filter})
}

// UnmarshalJSON implements json.Unmarshaler.
func (r *Rest) UnmarshalJSON(b []byte) error {
	var v restJSON
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	r.refs, r.filter = v.Refs, v.Filter
	return nil
}

// Next returns the next page of rest, at most limit results.
func (e *Engine) Next(ctx context.Context, scope Scope, rest Rest, limit int) (Page, error) {
	if err := checkScope(ctx, scope.Namespace); err != nil {
		return Page{}, err
	}
	return e.page(ctx, scope, rest, e.normalizeLimit(limit))
}

// page reads results from the front of rest until it has limit of them or
// rest runs out. A result whose row the scope cannot read, or a graph hit
// whose stored row fails the filter, is skipped.
func (e *Engine) page(ctx context.Context, scope Scope, rest Rest, limit int) (Page, error) {
	plan, err := rest.filter.Compile()
	if err != nil {
		return Page{}, err
	}
	var results Results
	refs := rest.refs
	for len(results) < limit && len(refs) > 0 {
		batch := refs[:min(limit-len(results), len(refs))]
		refs = refs[len(batch):]
		got, err := e.read(ctx, scope, batch, plan)
		if err != nil {
			return Page{}, err
		}
		results = append(results, got...)
	}
	var ids []uuid.UUID
	for o := range results.Objects() {
		ids = append(ids, o.ID)
	}
	for l := range results.Links() {
		ids = append(ids, l.From.ID, l.To.ID)
	}
	if err := e.recordRead(ctx, scope, uniqueUUIDs(ids)); err != nil {
		return Page{}, err
	}
	if results == nil {
		results = Results{}
	}
	return Page{
		Results: results,
		Facets:  e.facets(results),
		More:    len(refs) > 0,
		Rest:    Rest{refs: refs, filter: rest.filter},
	}, nil
}

// read reads the rows of refs in one GetMany and builds their results in
// order.
func (e *Engine) read(ctx context.Context, scope Scope, refs []ref, plan FilterPlan) (Results, error) {
	ids := make([]uuid.UUID, 0, len(refs)*2)
	for _, r := range refs {
		if r.Link != nil {
			ids = append(ids, r.Link.From, r.Link.To)
		} else {
			ids = append(ids, r.ID)
		}
	}
	objs, err := e.store.GetMany(ctx, scope, uniqueUUIDs(ids))
	if err != nil {
		return nil, fmt.Errorf("brain: read results: %w", err)
	}
	byID := make(map[uuid.UUID]Object, len(objs))
	for _, o := range objs {
		byID[o.ID] = o
	}
	out := make(Results, 0, len(refs))
	for _, r := range refs {
		if r.Link != nil {
			from, okF := byID[r.Link.From]
			to, okT := byID[r.Link.To]
			if okF && okT {
				out = append(out, Result{Link: &LinkResult{
					From: from.Rich(false), To: to.Rich(false),
					Relation: r.Link.Relation, Meta: r.Link.Meta, Score: r.Score,
				}})
			}
			continue
		}
		obj, ok := byID[r.ID]
		if !ok || (r.Check && !plan.Match(obj)) {
			continue
		}
		rich := obj.Rich(false)
		if r.Score != 0 {
			rich.Score = new(r.Score)
		}
		rich.Evidence, rich.Relation = r.Evidence, r.Relation
		out = append(out, Result{Object: &rich})
	}
	return out, nil
}

// facets counts the kinds and the common property values of the objects
// among results.
func (e *Engine) facets(results Results) Facets {
	f := Facets{Kinds: map[string]int{}, Properties: map[string]map[string]int{}}
	for o := range results.Objects() {
		f.Kinds[o.Kind]++
		spec, registered := e.catalog.Get(o.Kind)
		for k, v := range o.Properties {
			if registered {
				if _, ok := spec.Field(k); !ok {
					continue
				}
			} else if !isFilterScalar(v) {
				continue
			}
			val := fmt.Sprint(v)
			if f.Properties[k] == nil {
				f.Properties[k] = map[string]int{}
			}
			f.Properties[k][val]++
		}
	}
	if e.catalog.Empty() {
		maps.DeleteFunc(f.Properties, func(_ string, vals map[string]int) bool { return len(vals) > maxFacetValues })
	}
	if len(f.Kinds) == 0 {
		f.Kinds = nil
	}
	if len(f.Properties) == 0 {
		f.Properties = nil
	}
	return f
}

// source is one ranked list a search or related call fuses, with its weight.
// fuse merges ranked lists by reciprocal rank fusion: a result scores the
// sum of 1/(k+rank) over the lists that rank it, so the backends' scores,
// which are not comparable, play no part. The first list that ranks a
// result supplies its evidence and relation. Ties sort by key, so the order
// is deterministic.
func fuse(k int, lists ...[]ref) []ref {
	type acc struct {
		ref   ref
		score float64
	}
	byKey := map[string]*acc{}
	var order []string
	for _, list := range lists {
		for rank, r := range list {
			key := r.key()
			a, ok := byKey[key]
			if !ok {
				a = &acc{ref: r}
				byKey[key] = a
				order = append(order, key)
			} else if a.ref.Evidence == nil {
				a.ref.Evidence = r.Evidence
			}
			a.ref.Check = a.ref.Check && r.Check
			a.score += 1 / float64(k+rank+1)
		}
	}
	out := make([]ref, len(order))
	for i, key := range order {
		a := byKey[key]
		a.ref.Score = a.score
		out[i] = a.ref
	}
	slices.SortStableFunc(out, func(a, b ref) int {
		return cmp.Or(cmp.Compare(b.Score, a.Score), strings.Compare(a.key(), b.key()))
	})
	return out
}
