package helixgraph_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	helix "github.com/helixdb/helix-db/sdks/go"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/graphtest"
	"github.com/ryanaldo34/tacklr/brain/helixgraph"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
)

func TestHelixGraph_contract(t *testing.T) {
	g := helixtest.Graph(t)
	graphtest.Run(t, func(t *testing.T, emb brain.Embedder) brain.Graph {
		if emb == nil {
			return g
		}
		return helixtest.Open(t, func(o *helixgraph.Options) { o.Embedding = brain.Embedding{Embedder: emb} })
	})
}

func TestOpen_failsUntilHelixHasBuiltEveryIndex(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	// Nothing listens on port 9 (discard), so every request fails to connect.
	unreachable, err := helix.NewClient("http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	helixProxy := newFaultProxy(t)
	// newIndex makes Open build an index no other test has, so it waits on it.
	newIndex := func() helixgraph.Options {
		return helixgraph.Options{Dimension: graphtest.Dimension, EdgeRelations: []string{"r" + strings.ReplaceAll(uuid.NewString(), "-", "")}}
	}
	running := `{"status":"running","operation_id":"%s","index_id":"1","generation":"1","operation_kind":"build","family":"text","stage":"scan","attempt":1,"progress":{"entities":"0","input_bytes":"0","output_operations":"0","output_bytes":"0"}}`
	blocked := `{"op":{"status":"blocked","blocker_code":"invalid_source_data","operation_id":"%s","index_id":"1","generation":"1","operation_kind":"build","family":"text","stage":"scan","attempt":1,"progress":{"entities":"0","input_bytes":"0","output_operations":"0","output_bytes":"0"}}}`

	errs := map[string]error{}
	_, errs["unreachable"] = helixgraph.Open(ctx, unreachable, helixtest.Options)
	helixProxy.failWith("brain_create_indexes", `{"text":{"kind":"unknown"}}`)
	_, errs["index receipt"] = helixgraph.Open(ctx, helixProxy.client, helixtest.Options)
	// Another Open's build of the index, which Helix does not know.
	helixProxy.failWith("brain_create_indexes", `{"text":{"kind":"existing_operation","operation_id":"%s"}}`)
	_, errs["another open's build"] = helixgraph.Open(ctx, helixProxy.client, helixtest.Options)
	helixProxy.fail("brain_index_operation", http.StatusServiceUnavailable, "")
	_, errs["index status request"] = helixgraph.Open(ctx, helixProxy.client, newIndex())
	helixProxy.fail("brain_index_operation", http.StatusOK, `{"op":{}}`)
	_, errs["index status"] = helixgraph.Open(ctx, helixProxy.client, newIndex())
	helixProxy.failWith("brain_index_operation", blocked)
	_, errs["blocked index"] = helixgraph.Open(ctx, helixProxy.client, newIndex())
	helixProxy.failWith("brain_index_operation", `{"op":`+running+`}`)
	short, cancel := context.WithTimeout(ctx, 3*indexPoll)
	_, waited := helixgraph.Open(short, helixProxy.client, newIndex())
	cancel()
	// Another Open is creating the same indexes.
	helixProxy.rule.Store(&faultRule{query: "brain_create_indexes", status: http.StatusConflict, once: true})
	_, afterConflict := helixgraph.Open(ctx, helixProxy.client, helixtest.Options)
	helixProxy.fail("brain_create_indexes", http.StatusConflict, "")
	conflicted, cancel := context.WithTimeout(ctx, 3*indexPoll)
	_, keptConflicting := helixgraph.Open(conflicted, helixProxy.client, helixtest.Options)
	cancel()

	for name, err := range errs {
		if err == nil {
			t.Errorf("open with %s succeeded, want an error", name)
		}
	}
	if !errors.Is(waited, context.DeadlineExceeded) {
		t.Errorf("open while an index keeps running = %v, want the deadline", waited)
	}
	if afterConflict != nil {
		t.Errorf("open after a conflict with another open = %v, want it to retry and succeed", afterConflict)
	}
	if !helix.IsConflict(keptConflicting) || !errors.Is(keptConflicting, context.DeadlineExceeded) {
		t.Errorf("open while every create conflicts = %v, want the conflict and the deadline", keptConflicting)
	}
}

// TestEngine_aFailingHelixLeavesTheStoreTheSourceOfTruth: a graph write that
// fails after the store commit is still audited and leaves the row; expand
// falls back to containment; graph reads report the failure.
func TestEngine_aFailingHelixLeavesTheStoreTheSourceOfTruth(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	helixProxy := newFaultProxy(t)
	g, err := helixgraph.Open(ctx, helixProxy.client, helixtest.Options)
	if err != nil {
		t.Fatal(err)
	}
	store := brain.NewMemoryStore()
	var audited []string
	eng, err := brain.NewEngine(store,
		brain.WithAudit(func(ctx context.Context, ev brain.AuditEvent) error {
			audited = append(audited, string(ev.Op))
			return ctx.Err()
		}))
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, g)
	scope := brain.Scope{Namespace: brain.Namespace{{Name: "org", Value: uuid.NewString()}}}
	doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "doc"}, brain.Parts(brain.Object{Kind: "Chunk", Content: "part"}))
	if err != nil {
		t.Fatal(err)
	}
	other, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "other"})
	if err != nil {
		t.Fatal(err)
	}
	if err := kg.Link(ctx, scope, doc.ID, other.ID, "references"); err != nil {
		t.Fatal(err)
	}
	audited = nil

	helixProxy.fail("*", http.StatusBadRequest, "")
	_, putErr := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: "written"})
	_, treeErr := eng.PutTree(ctx, scope, doc, nil)
	removeErr := eng.SoftDelete(ctx, scope, other.ID)
	degraded, degradedErr := kg.Expand(ctx, scope, brain.ExpandRequest{ObjectID: doc.ID, RelationTypes: []string{"contains", "references"}}, brain.NewSearchContext())
	reads := map[string]error{
		"link":   kg.Link(ctx, scope, other.ID, doc.ID, "references"),
		"unlink": kg.Unlink(ctx, scope, doc.ID, other.ID, "references"),
	}
	_, reads["expand"] = kg.Expand(ctx, scope, brain.ExpandRequest{ObjectID: doc.ID, RelationTypes: []string{"references"}}, brain.NewSearchContext())
	_, reads["find objects"] = kg.FindObjects(ctx, scope, brain.FindObjectsRequest{Query: "doc"}, brain.NewSearchContext())
	_, reads["find links"] = kg.FindLinks(ctx, scope, brain.FindLinksRequest{RelationType: graphtest.EdgeSearchLabel, Query: "doc"})
	_, reads["shortest path"] = kg.ShortestPath(ctx, scope, brain.PathRequest{From: doc.ID, To: other.ID})
	helixProxy.hang("brain_put_object")
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, hungErr := eng.Put(short, scope, brain.Object{Kind: "Document", Title: "hung"})
	cancel()
	writes := slices.Clone(audited)
	docs, _ := eng.ListByKind(ctx, scope, "Document", 10)
	_, otherErr := eng.Read(ctx, scope, other.ID)
	kids, _ := store.ListChildren(ctx, scope, doc.ID)
	helixProxy.fail("*", http.StatusBadRequest, "")
	eraseErr := eng.Erase(ctx, scope)
	_, keptErr := eng.Read(ctx, scope, doc.ID)
	helixProxy.rule.Store(nil)
	retryErr := eng.Erase(ctx, scope)
	_, erasedErr := eng.Read(ctx, scope, doc.ID)
	left, leftErr := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{Query: "doc"}, brain.NewSearchContext())

	for name, err := range map[string]error{"put": putErr, "put tree": treeErr, "hung put": hungErr} {
		if !errors.Is(err, brain.ErrIndexPut) {
			t.Errorf("%s on a failing graph = %v, want ErrIndexPut", name, err)
		}
	}
	if !errors.Is(hungErr, context.DeadlineExceeded) {
		t.Errorf("put whose graph write outlives the caller = %v, want the deadline", hungErr)
	}
	if !slices.Equal(writes, []string{"put", "put", "put"}) {
		t.Errorf("audited %v, want the three committed puts", writes)
	}
	if got := titlesOf(docs); !slices.Contains(got, "written") || !slices.Contains(got, "hung") || len(kids) != 0 {
		t.Errorf("store after the graph failures: documents %v, parts %v, want written and hung, and no parts", got, kids)
	}
	if !errors.Is(removeErr, brain.ErrIndexRemove) || otherErr != nil {
		t.Errorf("soft delete on a failing graph = %v, then read = %v, want ErrIndexRemove and the row kept", removeErr, otherErr)
	}
	if !errors.Is(eraseErr, brain.ErrIndexRemove) || keptErr != nil {
		t.Errorf("erase on a failing graph = %v, then read = %v, want ErrIndexRemove and the rows kept", eraseErr, keptErr)
	}
	if retryErr != nil || !errors.Is(erasedErr, brain.ErrNotFound) || leftErr != nil || len(left.Objects) != 0 {
		t.Errorf("erase again once the graph is back = %v, read %v, find objects %+v %v, want everything gone from store and graph", retryErr, erasedErr, left.Objects, leftErr)
	}
	if degradedErr != nil || degraded.Mode != "children" {
		t.Errorf("expand with containment on a failing graph = %+v %v, want the children only", degraded, degradedErr)
	}
	for name, err := range reads {
		if err == nil {
			t.Errorf("%s on a failing graph succeeded, want an error", name)
		}
	}
}

// indexPoll is longer than helixgraph's index poll interval.
const indexPoll = 200 * time.Millisecond

// faultProxy forwards to the shared Helix and answers chosen queries itself.
type faultProxy struct {
	client *helix.Client
	rule   atomic.Pointer[faultRule]
}

type faultRule struct {
	query  string // query_name to answer, or "*" for every request
	status int
	body   string // %s is the operation id of a fresh index build, if any
	hang   bool   // answer only once the caller gives up
	once   bool   // answer one request, then forward again
}

func newFaultProxy(t *testing.T) *faultProxy {
	t.Helper()
	target, err := url.Parse(helixtest.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	p := &faultProxy{}
	forward := httputil.NewSingleHostReverseProxy(target)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		var req struct {
			QueryName string `json:"query_name"`
		}
		_ = json.Unmarshal(body, &req)
		if rule := p.rule.Load(); rule != nil && (rule.query == "*" || rule.query == req.QueryName) {
			if rule.once {
				p.rule.CompareAndSwap(rule, nil)
			}
			if rule.hang {
				<-r.Context().Done()
				return
			}
			w.WriteHeader(rule.status)
			_, _ = io.WriteString(w, strings.ReplaceAll(rule.body, "%s", uuid.NewString()))
			return
		}
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	if p.client, err = helix.NewClient(srv.URL); err != nil {
		t.Fatal(err)
	}
	return p
}

func (p *faultProxy) fail(query string, status int, body string) {
	p.rule.Store(&faultRule{query: query, status: status, body: body})
}

func (p *faultProxy) failWith(query, body string) { p.fail(query, http.StatusOK, body) }

func (p *faultProxy) hang(query string) { p.rule.Store(&faultRule{query: query, hang: true}) }

func TestGraph_searchPastMaxOverfetchRanksOnlyReadableElements(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	client, err := helix.NewClient(helixtest.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	opts := helixtest.Options
	opts.MaxOverfetch = 1
	opts.Embedding = brain.Embedding{Embedder: graphtest.Embedder{}}
	g, err := helixgraph.Open(ctx, client, opts)
	if err != nil {
		t.Fatal(err)
	}
	word := "w" + strings.ReplaceAll(uuid.NewString(), "-", "")
	put := func(ns brain.Namespace, marker string, readers ...string) uuid.UUID {
		id := uuid.New()
		if err := g.Put(ctx, brain.Indexed{Object: brain.Object{ID: id, Kind: "Document", Namespace: ns, Readers: readers}, Text: word + " " + marker}); err != nil {
			t.Fatal(err)
		}
		return id
	}
	link := func(ns brain.Namespace, from, to uuid.UUID, marker string) {
		if err := g.Link(ctx, brain.GraphEdge{Namespace: ns, From: from, To: to, RelationType: graphtest.EdgeSearchLabel, Meta: brain.EdgeMeta{Note: word + " " + marker}}); err != nil {
			t.Fatal(err)
		}
	}
	// Two hidden nodes are more than MaxOverfetch, and match the query best.
	nodes := brain.Namespace{{Name: "org", Value: uuid.NewString()}}
	open := put(nodes, "vecb")
	put(nodes, "veca", "legal")
	put(nodes, "veca", "legal")
	// One hidden node, but two of its edges are more than MaxOverfetch.
	edges := brain.Namespace{{Name: "org", Value: uuid.NewString()}}
	a, b, secret := put(edges, ""), put(edges, ""), put(edges, "", "legal")
	link(edges, a, secret, "veca")
	link(edges, b, secret, "veca")
	link(edges, a, b, "vecb")

	objects, objErr := g.FindObjects(ctx, brain.ObjectSearch{Namespace: nodes, Query: word + " veca", Limit: 1})
	found, edgeErr := g.FindLinks(ctx, brain.LinkSearch{Namespace: edges, RelationType: graphtest.EdgeSearchLabel, Query: word + " veca", Limit: 1})

	if objErr != nil || len(objects) != 1 || objects[0].ID != open {
		t.Errorf("object search past the overfetch = %+v %v, want the readable node", objects, objErr)
	}
	if edgeErr != nil || len(found) != 1 || found[0].FromID != a || found[0].ToID != b {
		t.Errorf("edge search past the overfetch = %+v %v, want a→b", found, edgeErr)
	}
}

// TestEngine_liveKnowledgeGraph is the Engine outcome on real Helix: Put
// dual-writes nodes, LinkWith stores edges with note vectors, and Expand,
// FindObjects, FindLinks and ShortestPath return hydrated objects; a
// soft-deleted target stops hydrating.
func TestEngine_liveKnowledgeGraph(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	g := helixtest.Graph(t)
	eng, err := brain.NewEngine(brain.NewMemoryStore(),
		brain.WithKinds(brain.KindSpec{Kind: "Document", IsParent: true}, brain.KindSpec{Kind: "Fact", IsParent: true}),
	)
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, g)
	scope := brain.Scope{Namespace: brain.Namespace{{Name: "org", Value: uuid.NewString()}}}
	put := func(kind, title, content string) brain.Object {
		obj, err := eng.Put(ctx, scope, brain.Object{Kind: kind, Title: title, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		return obj
	}
	doc := put("Document", "Contract", "MSA body")
	fact := put("Fact", "Risk", "indemnity exclusivity penalty on late delivery")
	summary := put("Document", "Summary", "short summary")
	if err := kg.LinkWith(ctx, scope, doc.ID, fact.ID, "about", brain.EdgeMeta{Note: "late delivery penalty exclusivity"}); err != nil {
		t.Fatal(err)
	}
	if err := kg.Link(ctx, scope, fact.ID, summary.ID, "about"); err != nil {
		t.Fatal(err)
	}
	doc.Title = "Contract v2"
	if _, err := eng.Put(ctx, scope, doc); err != nil {
		t.Fatal(err)
	}

	expand, expandErr := kg.Expand(ctx, scope, brain.ExpandRequest{ObjectID: doc.ID, RelationTypes: []string{"about"}, MaxHops: 2}, brain.NewSearchContext())
	objects, objectsErr := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{Query: "indemnity exclusivity penalty", Kinds: []string{"Fact"}, Limit: 10}, brain.NewSearchContext())
	links, linksErr := kg.FindLinks(ctx, scope, brain.FindLinksRequest{Query: "late delivery penalty", RelationType: "about", Limit: 10})
	path, pathErr := kg.ShortestPath(ctx, scope, brain.PathRequest{From: doc.ID, To: summary.ID, RelationType: "about", Direction: brain.DirectionOut})
	if err := eng.SoftDelete(ctx, scope, fact.ID); err != nil {
		t.Fatal(err)
	}
	afterDelete, afterErr := kg.Expand(ctx, scope, brain.ExpandRequest{ObjectID: doc.ID, RelationTypes: []string{"about"}}, brain.NewSearchContext())

	if expandErr != nil || len(expand.Objects) != 2 || expand.Objects[0].ID != fact.ID || expand.Objects[1].ID != summary.ID || expand.Objects[1].Relation.Depth != 2 {
		t.Errorf("two-hop expand = %+v %v, want fact then summary at depth 2", expand.Objects, expandErr)
	}
	if objectsErr != nil || len(objects.Objects) != 1 || objects.Objects[0].ID != fact.ID {
		t.Errorf("find objects = %+v %v, want the fact", objects.Objects, objectsErr)
	}
	if linksErr != nil || len(links.Links) == 0 || links.Links[0].From.ID != doc.ID || links.Links[0].To.ID != fact.ID {
		t.Errorf("find links = %+v %v, want doc→fact first", links.Links, linksErr)
	}
	if pathErr != nil || len(path.Objects) != 3 || path.Objects[0].Title != "Contract v2" || path.Objects[2].ID != summary.ID {
		t.Errorf("shortest path = %+v %v, want doc, fact, summary", path.Objects, pathErr)
	}
	if afterErr != nil || len(afterDelete.Objects) != 0 {
		t.Errorf("expand after soft delete = %+v %v, want none", afterDelete.Objects, afterErr)
	}
}

func titlesOf(objs []brain.Object) []string {
	out := make([]string, len(objs))
	for i, o := range objs {
		out[i] = o.Title
	}
	return out
}
