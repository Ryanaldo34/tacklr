package brain_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/helixgraph"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
)

func TestEngine_graphReadsGoRoundObjectsTheScopeCannotRead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, g := brain.NewMemoryStore(), helixtest.Graph(t)
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, g)
	tenant := mustNS(t, "org", uuid.NewString())
	team := append(tenant.Clone(), brain.Attr{Name: "team", Value: "x"})
	other := append(tenant.Clone(), brain.Attr{Name: "team", Value: "y"})
	// admin reads every object, so it writes and links them all.
	admin := brain.Scope{Namespace: tenant, Principals: []string{"legal"}}
	ids := map[string]uuid.UUID{}
	for _, o := range []brain.Object{
		{Title: "a", Namespace: team}, {Title: "b", Namespace: team}, {Title: "c", Namespace: team},
		{Title: "far", Namespace: team}, {Title: "behind", Namespace: team},
		{Title: "hidden", Namespace: other}, {Title: "secret", Namespace: team, Readers: []string{"legal"}},
	} {
		o.Kind = "Document"
		put, err := eng.Put(ctx, admin, o)
		if err != nil {
			t.Fatal(err)
		}
		ids[o.Title] = put.ID
	}
	// a→hidden→far is shorter than a→b→c→far, and a→secret→behind than
	// a→b→c→far→behind, but the team reads neither hidden nor secret.
	for _, l := range [][2]string{{"a", "b"}, {"b", "c"}, {"c", "far"}, {"a", "hidden"}, {"hidden", "far"}, {"a", "secret"}, {"secret", "behind"}, {"far", "behind"}} {
		if err := kg.Link(ctx, admin, ids[l[0]], ids[l[1]], "references"); err != nil {
			t.Fatal(err)
		}
	}
	auditDown, err := brain.NewEngine(store,
		brain.WithAudit(func(context.Context, brain.AuditEvent) error { return errString("audit down") }))
	if err != nil {
		t.Fatal(err)
	}
	auditDownKG := brain.NewKnowledgeGraph(auditDown, g)
	teamScope, legal := brain.Scope{Namespace: team}, brain.Scope{Namespace: team, Principals: []string{"legal"}}
	path := func(kg *brain.KnowledgeGraph, ctx context.Context, scope brain.Scope, from, to string, depth int) ([]string, error) {
		res, err := kg.ShortestPath(ctx, scope, brain.PathRequest{From: ids[from], To: ids[to], Direction: "out", MaxDepth: depth})
		return titlesOf(res.Objects), err
	}
	expand := func(scope brain.Scope) []string {
		res, err := kg.Expand(ctx, scope, brain.ExpandRequest{ObjectID: ids["a"], RelationTypes: []string{"references"}, Direction: "out", MaxHops: 2}, brain.NewSearchContext())
		if err != nil {
			t.Fatal(err)
		}
		got := titlesOf(res.Objects)
		slices.Sort(got)
		return got
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	roundHidden, roundHiddenErr := path(kg, ctx, teamScope, "a", "far", 0)
	roundSecret, roundSecretErr := path(kg, ctx, teamScope, "a", "behind", 0)
	viaSecret, viaSecretErr := path(kg, ctx, legal, "a", "behind", 0)
	_, tooDeep := path(kg, ctx, teamScope, "a", "c", 1)
	_, auditFailed := path(auditDownKG, ctx, teamScope, "a", "c", 0)
	_, stopped := path(kg, cancelled, teamScope, "a", "c", 0)
	_, noTo := kg.ShortestPath(ctx, teamScope, brain.PathRequest{From: ids["a"]})

	for name, tc := range map[string]struct {
		got  []string
		err  error
		want []string
	}{
		"path round an object out of scope":               {roundHidden, roundHiddenErr, []string{"a", "b", "c", "far"}},
		"path round an object the principals cannot read": {roundSecret, roundSecretErr, []string{"a", "b", "c", "far", "behind"}},
		"path through it for a reader":                    {viaSecret, viaSecretErr, []string{"a", "secret", "behind"}},
		"two-hop expand":                                  {expand(teamScope), nil, []string{"b", "c"}},
		"two-hop expand for a reader":                     {expand(legal), nil, []string{"b", "behind", "c", "secret"}},
	} {
		if tc.err != nil || !slices.Equal(tc.got, tc.want) {
			t.Errorf("%s = %v %v, want %v", name, tc.got, tc.err, tc.want)
		}
	}
	for name, tc := range map[string]struct{ err, want error }{
		"one hop too few": {tooDeep, brain.ErrNotFound},
		"no to":           {noTo, brain.ErrInvalid},
		"cancelled":       {stopped, context.Canceled},
		"a failing audit": {auditFailed, errString("audit down")},
	} {
		if !errors.Is(tc.err, tc.want) {
			t.Errorf("%s = %v, want %v", name, tc.err, tc.want)
		}
	}
}

// vectorEmbedder embeds each text as its fixed vector, [0, 1] by default,
// and fails on poison.
type vectorEmbedder struct {
	vectors map[string][]float32
	poison  string
}

func (e vectorEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	out := make([][]float32, len(texts))
	for i, text := range texts {
		if text == e.poison {
			return nil, errString("embed down")
		}
		out[i] = []float32{0, 1}
		if v, ok := e.vectors[text]; ok {
			out[i] = v
		}
	}
	return out, nil
}

func (vectorEmbedder) Model() string { return "test-model" }

func TestEngine_findLinksRanksNoteVectorsWithNoteText(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	emb := vectorEmbedder{vectors: map[string][]float32{"customer is unhappy": {1, 0}, "angry buyer": {1, 0}}, poison: "poison"}
	g := helixtest.Open(t, func(o *helixgraph.Options) { o.Embedding = brain.Embedding{Embedder: emb} })
	eng, err := brain.NewEngine(brain.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, g)
	scope := brain.Scope{Namespace: mustNS(t, "org", uuid.NewString())}
	put := func(title string) brain.Object {
		obj, err := eng.Put(ctx, scope, brain.Object{Kind: "Document", Title: title})
		if err != nil {
			t.Fatal(err)
		}
		return obj
	}
	deal, unhappy, renewal := put("deal"), put("unhappy"), put("renewal")
	for to, note := range map[uuid.UUID]string{unhappy.ID: "customer is unhappy", renewal.ID: "renewal next month"} {
		if err := kg.LinkWith(ctx, scope, deal.ID, to, "about", brain.EdgeMeta{Note: note}); err != nil {
			t.Fatal(err)
		}
	}

	byVector, vectorErr := kg.FindLinks(ctx, scope, brain.FindLinksRequest{RelationType: "about", Query: "angry buyer", Limit: 1})
	byText, textErr := kg.FindLinks(ctx, scope, brain.FindLinksRequest{RelationType: "about", Query: "renewal", Limit: 5})
	poisonLink := kg.LinkWith(ctx, scope, deal.ID, renewal.ID, "about", brain.EdgeMeta{Note: "poison"})
	_, poisonLinks := kg.FindLinks(ctx, scope, brain.FindLinksRequest{RelationType: "about", Query: "poison"})
	_, poisonObjects := kg.FindObjects(ctx, scope, brain.FindObjectsRequest{Query: "poison"}, brain.NewSearchContext())

	if vectorErr != nil || len(byVector.Links) != 1 || byVector.Links[0].To.ID != unhappy.ID {
		t.Errorf("vector-only match = %+v %v, want deal→unhappy", byVector.Links, vectorErr)
	}
	if textErr != nil || len(byText.Links) == 0 || byText.Links[0].To.ID != renewal.ID {
		t.Errorf("text and vector match = %+v %v, want deal→renewal first", byText.Links, textErr)
	}
	if !errors.Is(poisonLink, errString("embed down")) {
		t.Errorf("link whose note does not embed = %v, want the embedder's error", poisonLink)
	}
	if poisonLinks != nil || poisonObjects != nil {
		t.Errorf("searches whose query does not embed = %v, %v, want them answered by their words", poisonLinks, poisonObjects)
	}
}

func TestEngine_linkNeedsBothGraphNodes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := brain.NewMemoryStore()
	scope := brain.Scope{Namespace: mustNS(t, "org", uuid.NewString())}
	// Objects written before the graph was attached have no graph nodes.
	from, to := brain.Object{ID: uuid.New(), Kind: "Document", Namespace: scope.Namespace}, brain.Object{ID: uuid.New(), Kind: "Document", Namespace: scope.Namespace}
	for _, obj := range []brain.Object{from, to} {
		if err := putFixture(store, obj); err != nil {
			t.Fatal(err)
		}
	}
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}
	kg := brain.NewKnowledgeGraph(eng, helixtest.Graph(t))

	err = kg.Link(ctx, scope, from.ID, to.ID, "references")

	if !errors.Is(err, brain.ErrNotFound) {
		t.Fatalf("link without graph nodes = %v, want ErrNotFound", err)
	}
}
