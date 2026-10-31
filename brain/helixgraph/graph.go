// Package helixgraph is a brain.Graph on HelixDB.
//
// Helix does the work natively: filters apply before ranking (*Within
// searches over the readable candidates that pass them), a neighborhood walk
// of any depth is one request, and endpoints come back as object ids, not
// internal node ids. The graph embeds node text, edge notes and queries with
// its own Embedding and fuses its text and vector lists with its own
// Ranking. Open creates the indexes and waits until they are active.
//
// Each node and edge stores its tenant key, which partitions the text and
// vector indexes, and its namespace attrs as one token set. A node also
// stores its Readers and its scalar properties as token sets. A token is a hash of an attr or a
// principal, so the graph holds no principal ids, and one property write
// replaces a whole set. A query reads a node when it covers the node's
// namespace and the node has no readers or shares a principal with it, as
// brain.Scope.CanRead decides.
package helixgraph

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	helix "github.com/helixdb/helix-db/sdks/go"

	"github.com/ryanaldo34/tacklr/brain"
)

// nodeLabel is the Helix node label for knowledge objects.
const nodeLabel = "Object"

// Node property keys.
const (
	propObjectID   = "object_id"
	propKind       = "kind"
	propSearchText = "search_text"
	propEmbedding  = "embedding"
	// propTenant is the namespace's tenant key on nodes and edges. Text and
	// vector indexes are partitioned on it, so Helix ranks one tenant only.
	propTenant = "tenant"
	// propNamespace is the token set of the namespace attrs, on nodes and edges.
	propNamespace = "namespace"
	// propReaders is the token set of a node's Readers; empty when it has none.
	propReaders = "readers"
	// propRestrictedTenant is the tenant key of a node with Readers, and empty
	// otherwise. Its index finds a tenant's restricted nodes without a scan.
	propRestrictedTenant = "restricted_tenant"
	// propTitle, propCreatedAt and propUpdatedAt are what filters and recency
	// decay read; the times are Unix milliseconds.
	propTitle     = "title"
	propCreatedAt = "created_at"
	propUpdatedAt = "updated_at"
	// propProperties is the token set of the scalar properties, one token
	// per key and typed value, so filters match them before ranking.
	propProperties = "properties"
)

// Edge property keys.
const (
	propEdgeNote       = "note"
	propEdgeStatus     = "status"
	propEdgeRole       = "role"
	propEdgeConfidence = "confidence"
	propEdgeEvidenceID = "evidence_id"
	propEdgeCreatedAt  = "created_at"
	propEdgeUpdatedAt  = "updated_at"
	propEdgeEmbedding  = "embedding"
)

var edgeMetaProps = []string{propEdgeNote, propEdgeStatus, propEdgeRole, propEdgeConfidence, propEdgeEvidenceID, propEdgeCreatedAt, propEdgeUpdatedAt}

// indexPollInterval is how often Open checks index builds.
const indexPollInterval = 50 * time.Millisecond

// conflictBackoff and maxConflictBackoff bound the wait before exec replays a
// conflicted request.
const (
	conflictBackoff    = 5 * time.Millisecond
	maxConflictBackoff = 200 * time.Millisecond
)

// Options configures Open.
type Options struct {
	// Dimension is the embedding length of nodes and edges. Zero means no
	// vector indexes: searches rank text only.
	Dimension uint
	// Metric is the vector distance (default cosine).
	Metric helix.VectorDistanceMetric
	// EdgeRelations are the relations whose edges FindLinks can rank; a
	// FindLinks without a relation ranks them all.
	EdgeRelations []string
	// MaxOverfetch is how many elements a tenant-wide search may skip because
	// the caller cannot read them, and still rank from the index (default
	// 256). Past it, Helix ranks within the readable candidates.
	MaxOverfetch uint
	// Embedding embeds node text, edge notes and queries. Its vectors must
	// have Dimension values. Without an Embedder, searches rank text only.
	Embedding brain.Embedding
	// Ranking fuses the text and vector lists and ages them by updated_at.
	Ranking brain.Ranking
}

// Graph is a HelixDB knowledge graph.
type Graph struct {
	client       *helix.Client
	relations    []string // the indexed edge relations FindLinks ranks
	vectors      bool
	maxOverfetch int
	embedding    brain.Embedding
	ranking      brain.Ranking
}

// Open returns a graph over client after it creates every index opts needs
// and they are active.
func Open(ctx context.Context, client *helix.Client, opts Options) (*Graph, error) {
	g := &Graph{
		client:       client,
		vectors:      opts.Dimension > 0 && opts.Embedding.Enabled(),
		maxOverfetch: int(cmp.Or(opts.MaxOverfetch, 256)),
		embedding:    opts.Embedding,
		ranking:      opts.Ranking,
		relations:    slices.Clone(opts.EdgeRelations),
	}
	metric := cmp.Or(opts.Metric, helix.VectorDistanceCosine)
	indexes := map[string]helix.IndexSpec{
		"object_id":  helix.NodeUniqueEqualityIndex(nodeLabel, propObjectID),
		"tenant":     helix.NodeEqualityIndex(nodeLabel, propTenant),
		"restricted": helix.NodeEqualityIndex(nodeLabel, propRestrictedTenant),
		"text":       helix.NodeTextIndex(nodeLabel, propSearchText, propTenant),
	}
	if opts.Dimension > 0 {
		indexes["vector"] = helix.NodeVectorIndex(nodeLabel, propEmbedding, opts.Dimension, metric, propTenant)
	}
	for i, rel := range opts.EdgeRelations {
		indexes[fmt.Sprintf("edge_text_%d", i)] = helix.EdgeTextIndex(rel, propEdgeNote, propTenant)
		if opts.Dimension > 0 {
			indexes[fmt.Sprintf("edge_vector_%d", i)] = helix.EdgeVectorIndex(rel, propEdgeEmbedding, opts.Dimension, metric, propTenant)
		}
	}
	if err := g.createIndexes(ctx, indexes); err != nil {
		return nil, fmt.Errorf("helixgraph: open: %w", err)
	}
	return g, nil
}

// createIndexes creates indexes in one request, then waits for each build.
func (g *Graph) createIndexes(ctx context.Context, indexes map[string]helix.IndexSpec) error {
	q := helix.WriteQuery("brain_create_indexes")
	names := make([]string, 0, len(indexes))
	for name, spec := range indexes {
		q = q.VarAs(name, helix.G().CreateIndexIfNotExists(spec))
		names = append(names, name)
	}
	var receipts map[string]json.RawMessage
	if err := g.exec(ctx, q.Returning(names...), &receipts, helix.WriterOnly()); err != nil {
		return err
	}
	for name, raw := range receipts {
		receipt, err := helix.UnmarshalIndexDdlReceipt(raw)
		if err != nil {
			return fmt.Errorf("index %s: %w", name, err)
		}
		var op helix.IndexOperationID
		switch r := receipt.(type) {
		case helix.IndexDdlAccepted:
			op = r.OperationID
		case helix.IndexDdlExistingOperation:
			op = r.OperationID
		default: // already active
			continue
		}
		if err := g.awaitIndex(ctx, name, op); err != nil {
			return err
		}
	}
	return nil
}

// awaitIndex polls an index operation until it succeeds.
func (g *Graph) awaitIndex(ctx context.Context, name string, op helix.IndexOperationID) error {
	tick := time.NewTicker(indexPollInterval)
	defer tick.Stop()
	for {
		req := helix.ReadQuery("brain_index_operation").VarAs("op", helix.G().GetIndexOperation(string(op))).Returning("op")
		var out struct {
			Op json.RawMessage `json:"op"`
		}
		if err := g.exec(ctx, req, &out, helix.WriterOnly()); err != nil {
			return fmt.Errorf("index %s: %w", name, err)
		}
		status, err := helix.UnmarshalIndexOperationStatus(out.Op)
		if err != nil {
			return fmt.Errorf("index %s: %w", name, err)
		}
		switch status.(type) {
		case *helix.IndexOperationSucceeded:
			return nil
		case *helix.IndexOperationQueued, *helix.IndexOperationRunning:
		default: // blocked or aborted: it needs an operator
			return fmt.Errorf("index %s build stopped: %+v", name, status)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// exec runs req. Helix runs each request as one optimistic transaction, so
// concurrent writers conflict; a conflicted request committed nothing, so
// exec replays it after a jittered, doubling wait until it lands or ctx
// ends.
func (g *Graph) exec(ctx context.Context, req helix.Request, out any, opts ...helix.ExecOption) error {
	wait := conflictBackoff
	for {
		err := g.client.Exec(ctx, req, out, opts...)
		if !helix.IsConflict(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		case <-time.After(wait/2 + rand.N(wait)):
		}
		wait = min(2*wait, maxConflictBackoff)
	}
}

// token is a fixed-length hex hash of an attr or principal, so no token
// holds the set delimiter or contains another token.
func token(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:16])
}

// tokenSet is "|t1|t2|…|", or empty for no tokens.
func tokenSet(tokens []string) string {
	if len(tokens) == 0 {
		return ""
	}
	return "|" + strings.Join(tokens, "|") + "|"
}

// member is the substring a token set holds exactly when tok is in it.
func member(tok string) string { return "|" + tok + "|" }

func attrToken(a brain.Attr) string { return token("attr", a.Name, a.Value) }

func principalToken(p string) string { return token("principal", p) }

// nsProps are the namespace properties of a node or edge in ns.
func nsProps(ns brain.Namespace) helix.Props {
	tokens := make([]string, len(ns))
	for i, a := range ns {
		tokens[i] = attrToken(a)
	}
	return helix.Props{helix.Prop(propTenant, ns.Tenant().Key()), helix.Prop(propNamespace, tokenSet(tokens))}
}

// readerProps are the visibility properties of a node with readers.
func readerProps(ns brain.Namespace, readers []string) helix.Props {
	tokens := make([]string, len(readers))
	for i, r := range readers {
		tokens[i] = principalToken(r)
	}
	restricted := ""
	if len(readers) > 0 {
		restricted = ns.Tenant().Key()
	}
	return helix.Props{helix.Prop(propReaders, tokenSet(tokens)), helix.Prop(propRestrictedTenant, restricted)}
}

// scopeFilter matches what a scope reaches: elements of its tenant that
// carry every other scope attr and, for nodes, have no readers or share one
// of its principals.
type scopeFilter struct {
	tenant   helix.ParamRef
	attrs    []helix.Predicate
	readable helix.Predicate
}

func newScopeFilter(param func(name, value string) helix.ParamRef, scope brain.Namespace, principals []string) scopeFilter {
	f := scopeFilter{tenant: param("tenant", scope.Tenant().Key())}
	for i, a := range scope[1:] {
		f.attrs = append(f.attrs, helix.PredContainsExpr(propNamespace, param(fmt.Sprintf("ns%d", i), member(attrToken(a))).Expr()))
	}
	readable := []helix.Predicate{helix.PredEq(propReaders, "")}
	for i, p := range principals {
		readable = append(readable, helix.PredContainsExpr(propReaders, param(fmt.Sprintf("principal%d", i), member(principalToken(p))).Expr()))
	}
	f.readable = helix.PredOr(readable...)
	return f
}

// covered matches the nodes and edges the scope's namespace covers,
// whatever their readers.
func (f scopeFilter) covered(extra ...helix.Predicate) helix.Predicate {
	return helix.PredAnd(append(append([]helix.Predicate{helix.PredEq(propTenant, f.tenant)}, f.attrs...), extra...)...)
}

// nodes matches the nodes the scope can read.
func (f scopeFilter) nodes(extra ...helix.Predicate) helix.Predicate {
	return f.covered(append([]helix.Predicate{f.readable}, extra...)...)
}

// hidden finds up to limit nodes of the scope's tenant it cannot read,
// through the restricted index; it is empty in a tenant without restricted
// nodes.
func (f scopeFilter) hidden(limit int) *helix.Traversal {
	return helix.G().NWhere(helix.SourceEq(propRestrictedTenant, f.tenant)).Where(helix.PredNot(f.readable)).Limit(limit)
}

// visiblePlan builds the rankings of one search so that only elements the
// scope can read take a slot. A narrowed scope ranks within the readable
// candidates. A tenant-wide one runs probes first, and each ranking then
// runs as the one of three entries the probes pick:
//
//   - name: nothing in the tenant is hidden, so the index ranks it alone.
//   - name_over: at most MaxOverfetch elements are excluded, so the index
//     ranks k+MaxOverfetch, the excluded ones are removed, and k remain.
//   - name_within: more are excluded, so Helix ranks within the candidates.
//
// A skipped entry returns [], so the three merge into one list.
type visiblePlan struct {
	q        *helix.ReadQueryBuilder
	narrowed bool
	k, kOver helix.ParamRef
	// The plan's variable names, each set once with the plan's prefix, so
	// several plans share one request: the probes, the rankings, and the
	// candidate sets a ranking within the candidates may use.
	hidden, hiddenOver, bounded, excluded, excludedOver, few, many string
	text, vector, readable, intoReadable                           string
	names                                                          []string
}

// planVisible adds the probes to q, naming each variable and parameter with
// prefix so several plans share one request. excluded maps the bounded
// hidden nodes to the elements their ranking must skip; excludedVar reads
// such a set back.
func (g *Graph) planVisible(q *helix.ReadQueryBuilder, prefix string, scope scopeFilter, limit int, narrowed bool,
	excluded func(bounded *helix.Traversal) *helix.Traversal, excludedVar func(name string) *helix.Traversal,
) *visiblePlan {
	p := &visiblePlan{
		q: q, narrowed: narrowed, k: q.ParamI64(prefix+"limit", int64(limit)),
		hidden: prefix + "hidden", hiddenOver: prefix + "hidden_over", bounded: prefix + "bounded",
		excluded: prefix + "excluded", excludedOver: prefix + "excluded_over", few: prefix + "few", many: prefix + "many",
		text: prefix + "text", vector: prefix + "vector", readable: prefix + "readable", intoReadable: prefix + "into_readable",
	}
	if narrowed {
		return p
	}
	over := g.maxOverfetch
	p.kOver = q.ParamI64(prefix+"limit_over", int64(limit+over))
	p.q = q.
		VarAs(p.hidden, scope.hidden(over+1)).
		VarAs(p.hiddenOver, helix.G().N(helix.NodeVar(p.hidden)).Skip(over)).
		VarAsIf(p.bounded, helix.VarEmpty(p.hiddenOver), helix.G().N(helix.NodeVar(p.hidden))).
		VarAs(p.excluded, excluded(helix.G().N(helix.NodeVar(p.bounded)))).
		VarAs(p.excludedOver, excludedVar(p.excluded).Skip(over)).
		VarAsIf(p.few, helix.VarEmpty(p.excludedOver), helix.G().N(helix.NodeVar(p.bounded))).
		VarAsIf(p.many, helix.VarEmpty(p.few), helix.G().N(helix.NodeVar(p.hidden)).Limit(1))
	return p
}

// prepare adds the variable name (one of the plan's) that only rankings
// within the candidates use.
func (p *visiblePlan) prepare(name string, trav *helix.Traversal) {
	if p.narrowed {
		p.q = p.q.VarAs(name, trav)
		return
	}
	p.q = p.q.VarAsIf(name, helix.VarNotEmpty(p.many), trav)
}

// rank adds the ranking name (p.text or p.vector): index ranks the tenant
// partition to a depth, and within ranks the readable candidates.
func (p *visiblePlan) rank(name string, index func(k helix.ParamRef) *helix.Traversal, within func() *helix.Traversal, project ...helix.Projection) {
	if p.narrowed {
		p.q = p.q.VarAs(name, within().Project(project...))
		p.names = append(p.names, name)
		return
	}
	p.q = p.q.
		VarAsIf(name, helix.VarEmpty(p.hidden), index(p.k).Project(project...)).
		VarAsIf(name+"_over", helix.VarNotEmpty(p.few), index(p.kOver).Without(p.excluded).Limit(p.k).Project(project...)).
		VarAsIf(name+"_within", helix.VarNotEmpty(p.many), within().Project(project...))
	p.names = append(p.names, name, name+"_over", name+"_within")
}

// execPlan runs the plans, which share one request, and returns each
// ranking's rows by its prefixed name.
func execPlan[T any](ctx context.Context, g *Graph, plans ...*visiblePlan) (map[string][]T, error) {
	var names []string
	for _, p := range plans {
		names = append(names, p.names...)
	}
	var out map[string][]T
	if err := g.exec(ctx, plans[len(plans)-1].q.Returning(names...), &out); err != nil {
		return nil, err
	}
	merged := make(map[string][]T, len(out))
	for name, rows := range out {
		base := strings.TrimSuffix(strings.TrimSuffix(name, "_over"), "_within")
		merged[base] = append(merged[base], rows...)
	}
	return merged, nil
}

// Put implements brain.Graph: one write that updates the node in place
// when it exists (keeping its edges) and adds it otherwise. It embeds the
// text first when the graph has an Embedder.
func (g *Graph) Put(ctx context.Context, in brain.Indexed) error {
	obj := in.Object
	props := append(append(helix.Props{
		helix.Prop(propKind, obj.Kind),
		helix.Prop(propTitle, obj.Title),
		helix.Prop(propSearchText, in.Text),
		helix.Prop(propCreatedAt, obj.CreatedAt.UnixMilli()),
		helix.Prop(propUpdatedAt, obj.UpdatedAt.UnixMilli()),
		helix.Prop(propProperties, propertyTokens(obj.Properties)),
	}, nsProps(obj.Namespace)...), readerProps(obj.Namespace, obj.Readers)...)
	if g.vectors {
		vec, err := g.embedding.EmbedOne(ctx, in.Text)
		if err != nil {
			return fmt.Errorf("embed object: %w", err)
		}
		if vec != nil {
			props = append(props, helix.Prop(propEmbedding, vec))
		}
	}
	q := helix.WriteQuery("brain_put_object")
	oid := q.ParamString("object_id", obj.ID.String())
	update := helix.G().N(helix.NodeVar("existing"))
	for _, p := range props {
		update = update.SetProperty(p.Name, p.Value)
	}
	req := q.
		VarAs("existing", helix.G().NWhere(helix.SourceEq(propObjectID, oid))).
		VarAsIf("updated", helix.VarNotEmpty("existing"), update.Count()).
		VarAsIf("added", helix.VarEmpty("existing"), helix.G().AddN(nodeLabel, append(helix.Props{helix.Prop(propObjectID, oid)}, props...)).Count()).
		Returning()
	return g.exec(ctx, req, nil, helix.WriterOnly())
}

// propertyTokens is the token set of props' scalar values. A token names the
// key and the typed value, so a filter matches only a value of the same type
// (string, bool, or a number compared as float64), as brain.FilterPlan does.
func propertyTokens(props map[string]any) string {
	var tokens []string
	for key, v := range props {
		if t, ok := valueToken(key, v); ok {
			tokens = append(tokens, t)
		}
	}
	slices.Sort(tokens)
	return tokenSet(tokens)
}

// valueToken is the token of key=v, or false when v is not a scalar.
func valueToken(key string, v any) (string, bool) {
	k, ok := brain.FilterValueKey(v)
	return token("prop", key, k), ok
}

// filterPredicates translates filters into node predicates, applied before
// ranking.
func filterPredicates(filters brain.Filter) ([]helix.Predicate, error) {
	plan, err := filters.Compile()
	if err != nil {
		return nil, err
	}
	var preds []helix.Predicate
	for _, c := range plan.Conditions() {
		var alts []helix.Predicate
		switch {
		case c.Property:
			// A plan's values are scalars, so each has a token.
			for _, v := range c.Values {
				t, _ := valueToken(c.Field, v)
				alts = append(alts, helix.PredContains(propProperties, member(t)))
			}
		case c.Field == brain.FieldKind:
			for _, v := range c.Values {
				alts = append(alts, helix.PredEq(propKind, v))
			}
		case c.Field == brain.FieldTitle:
			for _, v := range c.Values {
				alts = append(alts, helix.PredEq(propTitle, v))
			}
		default:
			prop := propCreatedAt
			if c.Field == brain.FieldUpdatedAt {
				prop = propUpdatedAt
			}
			at := c.Time.UnixMilli()
			if c.Op == brain.OpGte {
				alts = append(alts, helix.PredGte(prop, at))
			} else {
				alts = append(alts, helix.PredLt(prop, at))
			}
		}
		preds = append(preds, helix.PredOr(alts...))
	}
	return preds, nil
}

// Remove implements brain.Graph. Helix drops incident edges with the node.
func (g *Graph) Remove(ctx context.Context, id uuid.UUID) error {
	q := helix.WriteQuery("brain_remove_object")
	oid := q.ParamString("object_id", id.String())
	req := q.VarAs("dropped", helix.G().NWhere(helix.SourceEq(propObjectID, oid)).Drop().Count()).Returning()
	return g.exec(ctx, req, nil, helix.WriterOnly())
}

// Erase implements brain.Index in one write: it drops every node the scope's
// namespace covers, whatever its readers, and Helix drops their edges.
func (g *Graph) Erase(ctx context.Context, scope brain.Scope) error {
	q := helix.WriteQuery("brain_erase")
	covered := newScopeFilter(q.ParamString, scope.Namespace, nil).covered()
	req := q.VarAs("dropped", helix.G().NWithLabelWhere(nodeLabel, covered).Drop().Count()).Returning()
	return g.exec(ctx, req, nil, helix.WriterOnly())
}

// Link implements brain.Graph: one write that drops any edge from→to with
// the relation and adds the new one, with the note's embedding when the
// graph has an Embedder.
func (g *Graph) Link(ctx context.Context, e brain.GraphEdge) error {
	rel := e.RelationType
	props := append(edgeProps(e.Meta), nsProps(e.Namespace)...)
	if g.vectors {
		vec, err := g.embedding.EmbedOne(ctx, e.Meta.Note)
		if err != nil {
			return fmt.Errorf("embed note: %w", err)
		}
		if vec != nil {
			props = append(props, helix.Prop(propEdgeEmbedding, vec))
		}
	}
	q := helix.WriteQuery("brain_add_edge")
	req := q.
		VarAs("from", helix.G().NWhere(helix.SourceEq(propObjectID, q.ParamString("from", e.From.String())))).
		VarAs("to", helix.G().NWhere(helix.SourceEq(propObjectID, q.ParamString("to", e.To.String())))).
		VarAs("dropped", helix.G().N(helix.NodeVar("from")).DropEdgeLabeled(helix.NodeVar("to"), rel).Count()).
		VarAsIf("added", helix.VarNotEmpty("to"), helix.G().N(helix.NodeVar("from")).AddE(rel, helix.NodeVar("to"), props).Count()).
		Returning("added")
	// A skipped conditional entry returns [], not a count.
	var out struct {
		Added json.RawMessage `json:"added"`
	}
	if err := g.exec(ctx, req, &out, helix.WriterOnly()); err != nil {
		return err
	}
	var added int
	if json.Unmarshal(out.Added, &added) != nil || added == 0 {
		return fmt.Errorf("%w: graph node %s or %s", brain.ErrNotFound, e.From, e.To)
	}
	return nil
}

// Unlink implements brain.Graph via Helix DropEdgeLabeled.
func (g *Graph) Unlink(ctx context.Context, from, to uuid.UUID, rel string) error {
	q := helix.WriteQuery("brain_remove_edge")
	req := q.
		VarAs("from", helix.G().NWhere(helix.SourceEq(propObjectID, q.ParamString("from", from.String())))).
		VarAs("to", helix.G().NWhere(helix.SourceEq(propObjectID, q.ParamString("to", to.String())))).
		VarAs("dropped", helix.G().N(helix.NodeVar("from")).DropEdgeLabeled(helix.NodeVar("to"), rel).Count()).
		Returning()
	return g.exec(ctx, req, nil, helix.WriterOnly())
}

func edgeProps(meta brain.EdgeMeta) helix.Props {
	props := helix.Props{
		helix.Prop(propEdgeCreatedAt, meta.CreatedAt.UTC().Format(time.RFC3339Nano)),
		helix.Prop(propEdgeUpdatedAt, meta.UpdatedAt.UTC().Format(time.RFC3339Nano)),
	}
	for name, value := range map[string]string{propEdgeNote: meta.Note, propEdgeStatus: meta.Status, propEdgeRole: meta.Role} {
		if value != "" {
			props = append(props, helix.Prop(name, value))
		}
	}
	if meta.Confidence != 0 {
		props = append(props, helix.Prop(propEdgeConfidence, meta.Confidence))
	}
	if meta.EvidenceID != nil {
		props = append(props, helix.Prop(propEdgeEvidenceID, meta.EvidenceID.String()))
	}
	return props
}

// edgeRow is an edge's metadata as projected by Helix.
type edgeRow struct {
	Note       string   `json:"note"`
	Status     string   `json:"status"`
	Role       string   `json:"role"`
	Confidence *float64 `json:"confidence"`
	EvidenceID string   `json:"evidence_id"`
	CreatedAt  string   `json:"created_at"`
	UpdatedAt  string   `json:"updated_at"`
}

func (r edgeRow) meta() brain.EdgeMeta {
	meta := brain.EdgeMeta{Note: r.Note, Status: r.Status, Role: r.Role}
	if r.Confidence != nil {
		meta.Confidence = *r.Confidence
	}
	if id, err := uuid.Parse(r.EvidenceID); err == nil {
		meta.EvidenceID = &id
	}
	meta.CreatedAt, _ = time.Parse(time.RFC3339Nano, r.CreatedAt)
	meta.UpdatedAt, _ = time.Parse(time.RFC3339Nano, r.UpdatedAt)
	return meta
}

// FindObjects implements brain.Graph in one request: a text and, with an
// Embedder, a vector ranking over the readable nodes that pass q.Filters,
// fused with the graph's Ranking. A tenant-wide scope with no filters ranks
// from the index (see visiblePlan); otherwise Helix ranks within the
// readable candidates that pass the filters.
func (g *Graph) FindObjects(ctx context.Context, sq brain.ObjectSearch) ([]brain.ScoredID, error) {
	extra, err := filterPredicates(sq.Filters)
	if err != nil {
		return nil, err
	}
	vec := g.queryVector(ctx, sq.Query)
	q := helix.ReadQuery("brain_find_objects")
	scope := newScopeFilter(q.ParamString, sq.Namespace, sq.Principals)
	candidates := func() *helix.Traversal { return helix.G().NWithLabelWhere(nodeLabel, scope.nodes(extra...)) }
	p := g.planVisible(q, "", scope, sq.Limit, len(sq.Namespace) > 1 || len(extra) > 0,
		func(bounded *helix.Traversal) *helix.Traversal { return bounded },
		func(name string) *helix.Traversal { return helix.G().N(helix.NodeVar(name)) })
	project := func(rank string) []helix.Projection {
		return []helix.Projection{helix.ProjectPropAs(propObjectID, "id"), helix.ProjectPropAs(propUpdatedAt, "updated_at"), helix.ProjectPropAs(rank, strings.TrimPrefix(rank, "$"))}
	}
	query := q.ParamString("query", sq.Query)
	p.rank(p.text,
		func(k helix.ParamRef) *helix.Traversal {
			return helix.G().TextSearchNodes(nodeLabel, propSearchText, query, k, scope.tenant)
		},
		func() *helix.Traversal {
			return candidates().TextSearchNodesWithin(nodeLabel, propSearchText, query, p.k, scope.tenant)
		},
		project("$score")...)
	if vec != nil {
		p.rank(p.vector,
			func(k helix.ParamRef) *helix.Traversal {
				return helix.G().VectorSearchNodes(nodeLabel, propEmbedding, vec, k, scope.tenant)
			},
			func() *helix.Traversal {
				return candidates().VectorSearchNodesWithin(nodeLabel, propEmbedding, vec, p.k, scope.tenant)
			},
			project("$distance")...)
	}
	out, err := execPlan[objectRow](ctx, g, p)
	if err != nil {
		return nil, err
	}
	fused := g.ranking.Fuse(scoredIDs(out[p.text]), scoredIDs(out[p.vector]))
	return fused[:min(len(fused), sq.Limit)], nil
}

// rank is a hit's text score or vector distance; Score maps a distance to
// a similarity in (0, 1].
type rank struct {
	Score    *float64 `json:"score"`
	Distance float64  `json:"distance"`
}

func (r rank) value() float64 {
	if r.Score != nil {
		return *r.Score
	}
	return 1 / (1 + r.Distance)
}

type objectRow struct {
	rank
	ID        uuid.UUID `json:"id"`
	UpdatedAt int64     `json:"updated_at"`
}

func scoredIDs(rows []objectRow) []brain.ScoredID {
	var out []brain.ScoredID
	for _, row := range rows {
		out = append(out, brain.ScoredID{ID: row.ID, Score: row.value(), UpdatedAt: time.UnixMilli(row.UpdatedAt).UTC()})
	}
	return out
}

// queryVector embeds a search query. A query that does not embed is
// searched by its words alone.
func (g *Graph) queryVector(ctx context.Context, query string) []float32 {
	if !g.vectors {
		return nil
	}
	vec, err := g.embedding.EmbedOne(ctx, query)
	if err != nil {
		slog.WarnContext(ctx, "query embed failed; searching text only", "error", err)
	}
	return vec
}

// FindLinks implements brain.Graph in one request, like FindObjects, over
// the edges of one relation the namespace covers whose two ends the query
// can read. A tenant-wide scope excludes the edges of the tenant's hidden
// nodes. Helix rejects a relation not in Options.EdgeRelations.
func (g *Graph) FindLinks(ctx context.Context, lq brain.LinkSearch) ([]brain.EdgeSearchHit, error) {
	rels := g.relations
	if lq.RelationType != "" {
		rels = []string{lq.RelationType}
	}
	if len(rels) == 0 {
		return nil, nil
	}
	vec := g.queryVector(ctx, lq.Query)
	q := helix.ReadQuery("brain_find_links")
	scope := newScopeFilter(q.ParamString, lq.Namespace, lq.Principals)
	query := q.ParamString("query", lq.Query)
	plans := make([]*visiblePlan, len(rels))
	for i, rel := range rels {
		// Each relation's plan shares the request; the later plans build on
		// the earlier ones' query.
		p := g.planVisible(q, fmt.Sprintf("r%d_", i), scope, lq.Limit, len(lq.Namespace) > 1,
			func(bounded *helix.Traversal) *helix.Traversal {
				return bounded.BothE(rel).Dedup().Limit(g.maxOverfetch + 1)
			},
			func(name string) *helix.Traversal { return helix.G().E(helix.EdgeVar(name)) })
		// The candidates are the covered edges leaving a readable node that
		// are also among the edges entering one.
		p.prepare(p.readable, helix.G().NWithLabelWhere(nodeLabel, scope.nodes()))
		p.prepare(p.intoReadable, helix.G().N(helix.NodeVar(p.readable)).InE(rel))
		candidates := func() *helix.Traversal {
			return helix.G().N(helix.NodeVar(p.readable)).OutE(rel).Where(scope.covered()).Within(p.intoReadable)
		}
		p.rank(p.text,
			func(k helix.ParamRef) *helix.Traversal {
				return helix.G().TextSearchEdges(rel, propEdgeNote, query, k, scope.tenant)
			},
			func() *helix.Traversal {
				return candidates().TextSearchEdgesWithin(rel, propEdgeNote, query, p.k, scope.tenant)
			},
			edgeProjections("$score")...)
		if vec != nil {
			p.rank(p.vector,
				func(k helix.ParamRef) *helix.Traversal {
					return helix.G().VectorSearchEdges(rel, propEdgeEmbedding, vec, k, scope.tenant)
				},
				func() *helix.Traversal {
					return candidates().VectorSearchEdgesWithin(rel, propEdgeEmbedding, vec, p.k, scope.tenant)
				},
				edgeProjections("$distance")...)
		}
		plans[i], q = p, p.q
	}
	out, err := execPlan[edgeHitRow](ctx, g, plans...)
	if err != nil {
		return nil, err
	}
	var lists [][]brain.EdgeSearchHit
	for i, rel := range rels {
		lists = append(lists, edgeHits(rel, out[plans[i].text]), edgeHits(rel, out[plans[i].vector]))
	}
	fused := g.ranking.FuseEdges(lists...)
	return fused[:min(len(fused), lq.Limit)], nil
}

// edgeProjections project an edge hit: its ends' object ids, its rank and its
// metadata.
func edgeProjections(rank string) []helix.Projection {
	projections := []helix.Projection{
		helix.ProjectFromEndpoint(propObjectID, "from"),
		helix.ProjectToEndpoint(propObjectID, "to"),
		helix.ProjectPropAs(rank, strings.TrimPrefix(rank, "$")),
	}
	for _, prop := range edgeMetaProps {
		projections = append(projections, helix.ProjectProp(prop))
	}
	return projections
}

type edgeHitRow struct {
	edgeRow
	rank
	From uuid.UUID `json:"from"`
	To   uuid.UUID `json:"to"`
}

func edgeHits(rel string, rows []edgeHitRow) []brain.EdgeSearchHit {
	var out []brain.EdgeSearchHit
	for _, row := range rows {
		out = append(out, brain.EdgeSearchHit{FromID: row.From, ToID: row.To, RelationType: rel, Meta: row.meta(), Score: row.value()})
	}
	return out
}

// Neighborhood implements brain.Graph in one request. Hop d starts
// from the frontier node set of hop d-1; each (relation, direction) of a hop
// is one projection of source, edge and peer. Only nodes the namespace
// covers are returned or join a frontier. Each projection and frontier holds
// at most q.Limit rows. No relations follows every edge label, and each hit
// carries its edge's label.
func (g *Graph) Neighborhood(ctx context.Context, nq brain.NeighborhoodQuery) ([]brain.GraphNeighbor, error) {
	// Each step follows one label set: no labels follows every relation.
	labels := [][]string{nil}
	if len(nq.Relations) > 0 {
		labels = make([][]string, len(nq.Relations))
		for i, rel := range nq.Relations {
			labels[i] = []string{rel}
		}
	}
	dirs := []string{brain.DirectionOut, brain.DirectionIn}
	if nq.Direction != brain.DirectionBoth {
		dirs = []string{nq.Direction}
	}
	q := helix.ReadQuery("brain_neighborhood")
	scope := newScopeFilter(q.ParamString, nq.Namespace, nq.Principals)
	k := q.ParamI64("limit", int64(nq.Limit))
	q = q.VarAs("f0", helix.G().NWhere(helix.SourceEq(propObjectID, q.ParamString("object_id", nq.ObjectID.String()))))
	type entry struct {
		name, dir string
		depth     int
	}
	var entries []entry
	for depth := 1; depth <= nq.MaxHops; depth++ {
		prev := helix.NodeVar(fmt.Sprintf("f%d", depth-1))
		var steps []helix.SubTraversal
		for i, label := range labels {
			for _, dir := range dirs {
				name := fmt.Sprintf("h%d_%d_%s", depth, i, dir)
				edges := helix.G().N(prev).Bind("src").OutE(label...)
				step := helix.Sub().Out(label...)
				if dir == brain.DirectionIn {
					edges = helix.G().N(prev).Bind("src").InE(label...)
					step = helix.Sub().In(label...)
				}
				steps = append(steps, step)
				q = q.VarAs(name, edges.Bind("e").OtherN().Where(scope.nodes()).Limit(k).ProjectBindings(neighborProjections()...))
				entries = append(entries, entry{name: name, dir: dir, depth: depth})
			}
		}
		if depth < nq.MaxHops {
			// Union needs two branches; one relation in one direction steps directly.
			next := helix.G().N(prev)
			switch {
			case len(steps) > 1:
				next = next.Union(steps...)
			case dirs[0] == brain.DirectionOut:
				next = next.Out(labels[0]...)
			default:
				next = next.In(labels[0]...)
			}
			q = q.VarAs(fmt.Sprintf("f%d", depth), next.Where(scope.nodes()).Dedup().Limit(k))
		}
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.name
	}
	var out map[string][]neighborRow
	if err := g.exec(ctx, q.Returning(names...), &out); err != nil {
		return nil, err
	}
	seen := map[uuid.UUID]struct{}{nq.ObjectID: {}}
	var hits []brain.GraphNeighbor
	for _, e := range entries {
		for _, row := range out[e.name] {
			if _, ok := seen[row.ID]; ok {
				continue
			}
			seen[row.ID] = struct{}{}
			hits = append(hits, brain.GraphNeighbor{ObjectID: row.ID, SourceID: row.Source, RelationType: row.Label, Direction: e.dir, Depth: e.depth, Meta: row.meta()})
			if len(hits) == nq.Limit {
				return hits, nil
			}
		}
	}
	return hits, nil
}

type neighborRow struct {
	edgeRow
	Source uuid.UUID `json:"source"`
	ID     uuid.UUID `json:"id"`
	Label  string    `json:"label"`
}

func neighborProjections() []helix.BindingProjection {
	projections := []helix.BindingProjection{
		helix.ProjectNamedBinding("src", propObjectID, "source"),
		helix.ProjectCurrentBinding(propObjectID, "id"),
		helix.ProjectNamedBinding("e", "$label", "label"),
	}
	for _, p := range edgeMetaProps {
		projections = append(projections, helix.ProjectNamedBinding("e", p, p))
	}
	return projections
}
