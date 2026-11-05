package helixgraph_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	helix "github.com/helixdb/helix-db/sdks/go"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/graphtest"
	"github.com/ryanaldo34/tacklr/brain/helixgraph"
	"github.com/ryanaldo34/tacklr/internal/faultproxy"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
)

// TestHelixGraph_contract: Helix meets the graph contract every brain graph
// must meet.
func TestHelixGraph_contract(t *testing.T) {
	t.Parallel()
	g := helixtest.Graph(t)
	graphtest.Run(t, func(t *testing.T, emb brain.Embedder) brain.Graph {
		if emb == nil {
			return g
		}
		return helixtest.Open(t, func(o *helixgraph.Options) { o.Embedding = brain.Embedding{Embedder: emb} })
	})
}

// TestOpen_failsUntilHelixHasBuiltEveryIndex: Open fails on a bad index
// receipt or status, waits while Helix is not listening or an index builds,
// and retries past one gateway error or create conflict.
func TestOpen_failsUntilHelixHasBuiltEveryIndex(t *testing.T) {
	t.Parallel()
	// Opens that should succeed get a deadline too: a Helix that is gone
	// fails the test instead of leaving it waiting.
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	// Nothing listens on port 9 (discard), so every request fails to connect.
	unreachable, err := helix.NewClient("http://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	helixProxy := newHelixProxy(t)
	// newIndex makes Open build an index no other test has, so it waits on it.
	newIndex := func() helixgraph.Options {
		return helixgraph.Options{Dimension: graphtest.Dimension, EdgeRelations: []string{"r" + strings.ReplaceAll(uuid.NewString(), "-", "")}}
	}
	running := `{"status":"running","operation_id":"%s","index_id":"1","generation":"1","operation_kind":"build","family":"text","stage":"scan","attempt":1,"progress":{"entities":"0","input_bytes":"0","output_operations":"0","output_bytes":"0"}}`
	blocked := `{"op":{"status":"blocked","blocker_code":"invalid_source_data","operation_id":"%s","index_id":"1","generation":"1","operation_kind":"build","family":"text","stage":"scan","attempt":1,"progress":{"entities":"0","input_bytes":"0","output_operations":"0","output_bytes":"0"}}}`

	errs := map[string]error{}
	helixProxy.failWith("brain_create_indexes", `{"text":{"kind":"unknown"}}`)
	_, errs["index receipt"] = helixgraph.Open(ctx, helixProxy.client, helixtest.Options)
	// Another Open's build of the index, which Helix does not know.
	helixProxy.failWith("brain_create_indexes", `{"text":{"kind":"existing_operation","operation_id":"%s"}}`)
	_, errs["another open's build"] = helixgraph.Open(ctx, helixProxy.client, helixtest.Options)
	helixProxy.fail("brain_index_operation", http.StatusInternalServerError, "")
	_, errs["index status request"] = helixgraph.Open(ctx, helixProxy.client, newIndex())
	helixProxy.fail("brain_index_operation", http.StatusOK, `{"op":{}}`)
	_, errs["index status"] = helixgraph.Open(ctx, helixProxy.client, newIndex())
	helixProxy.failWith("brain_index_operation", blocked)
	_, errs["blocked index"] = helixgraph.Open(ctx, helixProxy.client, newIndex())
	helixProxy.failWith("brain_index_operation", `{"op":`+running+`}`)
	short, cancel := context.WithTimeout(ctx, 3*indexPoll)
	_, waited := helixgraph.Open(short, helixProxy.client, newIndex())
	cancel()
	// Helix is not listening yet: Open waits for it until the deadline.
	starting, cancel := context.WithTimeout(ctx, 3*indexPoll)
	_, notListening := helixgraph.Open(starting, unreachable, helixtest.Options)
	cancel()
	// A gateway in front of Helix failed one request.
	helixProxy.failOnce("brain_index_operation", http.StatusBadGateway)
	_, afterGateway := helixgraph.Open(ctx, helixProxy.client, newIndex())
	// Another Open is creating the same indexes.
	helixProxy.failOnce("brain_create_indexes", http.StatusConflict)
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
	if !errors.Is(notListening, context.DeadlineExceeded) {
		t.Errorf("open while Helix is not listening = %v, want it to wait until the deadline", notListening)
	}
	if afterGateway != nil {
		t.Errorf("open after one gateway error = %v, want it to retry and succeed", afterGateway)
	}
	if afterConflict != nil {
		t.Errorf("open after a conflict with another open = %v, want it to retry and succeed", afterConflict)
	}
	if !errors.Is(keptConflicting, context.DeadlineExceeded) {
		t.Errorf("open while every create conflicts = %v, want it to give up at the deadline", keptConflicting)
	}
}

// TestEngine_aFailingHelixLeavesTheStoreTheSourceOfTruth: a graph write that
// fails after the store commit is still audited and leaves the row; expand
// falls back to containment; graph reads report the failure.
func TestEngine_aFailingHelixLeavesTheStoreTheSourceOfTruth(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	helixProxy := newHelixProxy(t)
	g, err := helixgraph.Open(ctx, helixProxy.client, helixtest.Options)
	if err != nil {
		t.Fatal(err)
	}
	store := brain.NewMemoryStore()
	var audited []string
	eng, err := brain.NewEngine(store, brain.WithGraph(g),
		brain.WithAudit(func(ctx context.Context, ev brain.AuditEvent) error {
			audited = append(audited, string(ev.Op))
			return ctx.Err()
		}))
	if err != nil {
		t.Fatal(err)
	}
	scope := brain.Scope{Namespace: brain.Namespace{{Name: "org", Value: uuid.NewString()}}}
	doc, err := eng.PutTree(ctx, scope, brain.Object{Kind: "Document", Title: "doc"}, brain.Parts(brain.Object{Kind: "Chunk", Content: "part"}))
	if err != nil {
		t.Fatal(err)
	}
	other, err := eng.Save(ctx, scope, brain.Object{Kind: "Document", Title: "other"})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.Link(ctx, scope, brain.LinkRequest{From: doc.ID, To: other.ID, Relation: "references"}); err != nil {
		t.Fatal(err)
	}
	audited = nil

	helixProxy.fail("*", http.StatusBadRequest, "")
	_, saveErr := eng.Save(ctx, scope, brain.Object{Kind: "Document", Title: "written"})
	_, treeErr := eng.PutTree(ctx, scope, doc, nil)
	removeErr := eng.SoftDelete(ctx, scope, other.ID)
	children, childrenErr := eng.Related(ctx, scope, brain.RelatedRequest{ID: doc.ID})
	found, foundErr := eng.Search(ctx, scope, brain.SearchRequest{Query: "written"})
	failed := map[string]error{
		"link":   eng.Link(ctx, scope, brain.LinkRequest{From: other.ID, To: doc.ID, Relation: "references"}),
		"unlink": eng.Link(ctx, scope, brain.LinkRequest{From: doc.ID, To: other.ID, Relation: "references", Remove: true}),
	}
	_, failed["related by a relation"] = eng.Related(ctx, scope, brain.RelatedRequest{ID: doc.ID, Relation: "references"})
	helixProxy.hang("brain_put_object")
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, hungErr := eng.Save(short, scope, brain.Object{Kind: "Document", Title: "hung"})
	cancel()
	writes := slices.DeleteFunc(slices.Clone(audited), func(op string) bool { return op == string(brain.AuditRead) })
	helixProxy.fail("*", http.StatusBadRequest, "")
	hung, _ := eng.Search(ctx, scope, brain.SearchRequest{Query: "hung"})
	_, otherErr := eng.Open(ctx, scope, other.ID)
	kidsTree, _ := store.GetTree(ctx, scope, doc.ID, 0)
	kids := kidsTree.Parts
	eraseErr := eng.Erase(ctx, scope)
	_, keptErr := eng.Open(ctx, scope, doc.ID)
	helixProxy.Clear()
	retryErr := eng.Erase(ctx, scope)
	_, erasedErr := eng.Open(ctx, scope, doc.ID)
	left, leftErr := eng.Search(ctx, scope, brain.SearchRequest{Query: "doc"})

	for name, err := range map[string]error{"save": saveErr, "put tree": treeErr, "hung save": hungErr} {
		if !errors.Is(err, brain.ErrIndexPut) {
			t.Errorf("%s on a failing graph = %v, want ErrIndexPut", name, err)
		}
	}
	if !errors.Is(hungErr, context.DeadlineExceeded) {
		t.Errorf("save whose graph write outlives the caller = %v, want the deadline", hungErr)
	}
	if !slices.Equal(writes, []string{"put", "put", "put"}) {
		t.Errorf("audited %v, want the three committed saves", writes)
	}
	if foundErr != nil || titles(found) != "written" || titles(hung) != "hung" || len(kids) != 0 {
		t.Errorf("search on a failing graph = %q %v and %q, parts %v, want the store's written and hung records, and no parts", titles(found), foundErr, titles(hung), kids)
	}
	if !errors.Is(removeErr, brain.ErrIndexRemove) || otherErr != nil {
		t.Errorf("soft delete on a failing graph = %v, then open = %v, want ErrIndexRemove and the row kept", removeErr, otherErr)
	}
	if !errors.Is(eraseErr, brain.ErrIndexRemove) || keptErr != nil {
		t.Errorf("erase on a failing graph = %v, then open = %v, want ErrIndexRemove and the rows kept", eraseErr, keptErr)
	}
	if retryErr != nil || !errors.Is(erasedErr, brain.ErrNotFound) || leftErr != nil || len(left.Results) != 0 {
		t.Errorf("erase again once the graph is back = %v, open %v, search %+v %v, want everything gone from store and graph", retryErr, erasedErr, left.Results, leftErr)
	}
	if childrenErr != nil || len(children.Results) != 0 {
		t.Errorf("related on a failing graph = %+v %v, want the children only (none left)", children.Results, childrenErr)
	}
	for name, err := range failed {
		if err == nil {
			t.Errorf("%s on a failing graph succeeded, want an error", name)
		}
	}
}

// titles joins the titles of a page's objects.
func titles(p brain.Page) string {
	var out []string
	for _, r := range p.Results {
		if r.Object != nil {
			out = append(out, r.Object.Title)
		}
	}
	return strings.Join(out, ",")
}

// indexPoll is longer than helixgraph's index poll interval.
const indexPoll = 200 * time.Millisecond

// helixProxy is a fault proxy in front of the shared Helix, and a client
// that goes through it.
type helixProxy struct {
	*faultproxy.Proxy
	client *helix.Client
}

func newHelixProxy(t *testing.T) helixProxy {
	t.Helper()
	target, err := url.Parse(helixtest.URL(t))
	if err != nil {
		t.Fatal(err)
	}
	p := faultproxy.Start(t, target)
	client, err := helix.NewClient(p.URL())
	if err != nil {
		t.Fatal(err)
	}
	return helixProxy{Proxy: p, client: client}
}

// query matches the requests for the Helix query name, or every request for
// "*".
func query(name string) func(*http.Request) bool {
	return func(r *http.Request) bool {
		var req struct {
			QueryName string `json:"query_name"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		return name == "*" || req.QueryName == name
	}
}

// fail answers the query with status and body. A %s in body is the
// operation id of an index build no other test has.
func (p helixProxy) fail(name string, status int, body string) {
	p.Set(faultproxy.Rule{Match: query(name), Fault: faultproxy.Answer(status, strings.ReplaceAll(body, "%s", uuid.NewString()))})
}

func (p helixProxy) failWith(name, body string) { p.fail(name, http.StatusOK, body) }

// failOnce answers one request for the query with status.
func (p helixProxy) failOnce(name string, status int) {
	p.Set(faultproxy.Rule{Match: query(name), Fault: faultproxy.Answer(status, ""), Once: true})
}

func (p helixProxy) hang(name string) {
	p.Set(faultproxy.Rule{Match: query(name), Fault: faultproxy.Hang()})
}

// TestGraph_searchPastMaxOverfetchRanksOnlyReadableElements: when more hidden
// nodes or edges than MaxOverfetch rank first, a search still returns the
// readable node or edge.
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
