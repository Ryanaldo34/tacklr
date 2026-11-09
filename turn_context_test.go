package tacklr_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/brain/storetest"
	"github.com/ryanaldo34/tacklr/internal/helixtest"
	"github.com/ryanaldo34/tacklr/vfs/mount"
)

// turnView is what the probe tool saw of its call.
type turnView struct {
	Session  string          `json:"session"`
	Bindings []mount.Binding `json:"bindings"`
	Plan     []tacklr.Todo   `json:"plan"`
}

// probe is a read tool that reports its call's session, credentials and
// plan, then changes its copy of the credentials.
func probe() *tacklr.Tool {
	return tacklr.NewTool(tacklr.ToolConfig{
		Name: "probe", Description: "Report the turn.", Access: tacklr.ToolReadAccess,
		Handler: func(_ context.Context, _ struct{}, cc *tacklr.CallContext) (string, error) {
			v := turnView{Session: cc.SessionID(), Bindings: cc.Bindings(), Plan: cc.Plan()}
			raw, _ := json.Marshal(v)
			for _, b := range v.Bindings {
				b.Params[mount.ParamName] = "changed"
			}
			return string(raw), nil
		},
	})
}

// TestCallContext_givesAToolItsSessionCredentialsAndPlan: a tool reads the
// session's id, the turn's credentialed mounts and the committed plan. Its
// copy of the mounts is its own: changing it changes no later call's.
func TestCallContext_givesAToolItsSessionCredentialsAndPlan(t *testing.T) {
	t.Parallel()
	model, _ := scripted(t)
	bindings := []mount.Binding{{Provider: "google", Auth: mount.Credential{Token: "at-1"}, Params: map[string]string{mount.ParamName: "mail"}}}
	h, err := tacklr.NewHarness(t.Context(), tacklr.BindTurn(tacklr.AgentOptions{Model: model, MaxWindowSize: 8192, Tools: []*tacklr.Tool{probe()}}, "sess-7", nil, bindings))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	prompt(t, h, "goal")
	if _, err := runRound(t, h, call("plan", "create_plan", `{"plan":"ship","todos":[{"title":"read mail","status":"in_progress","description":"find the deal"}]}`)); err != nil {
		t.Fatal(err)
	}

	var first, second turnView
	for _, v := range []*turnView{&first, &second} {
		if err := json.Unmarshal([]byte(result(t, h, call("p", "probe", `{}`))), v); err != nil {
			t.Fatal(err)
		}
	}

	if first.Session != "sess-7" {
		t.Errorf("session = %q, want sess-7", first.Session)
	}
	if len(first.Bindings) != 1 || first.Bindings[0].Auth.Token != "at-1" || first.Bindings[0].Alias() != "mail" {
		t.Errorf("bindings = %+v, want the turn's mail credential", first.Bindings)
	}
	if len(second.Bindings) != 1 || second.Bindings[0].Alias() != "mail" || bindings[0].Alias() != "mail" {
		t.Errorf("second call's bindings = %+v, turn's = %+v; want neither changed by the first call", second.Bindings, bindings)
	}
	if len(first.Plan) != 1 || first.Plan[0].Title != "read mail" {
		t.Errorf("plan = %+v, want the committed todo", first.Plan)
	}
}

// TestBrainTools_saveAndLinkFollowTheHostSchema: save describes each kind
// it writes with its fields; link offers only the host's relations, says
// which kinds each joins, keeps a link's role, and refuses another
// relation or a link between other kinds.
func TestBrainTools_saveAndLinkFollowTheHostSchema(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	eng, err := brain.NewEngine(brain.NewMemoryStore(), brain.WithGraph(helixtest.Graph(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err := eng.ApplyKinds(ctx,
		brain.KindSpec{Kind: "Deal", Description: "An open opportunity.", IsParent: true, Fields: []brain.FieldSpec{
			{Name: "status", Type: brain.FieldTypeString, Required: true, Enum: []string{"open", "closed"}, Description: "Where it stands."},
		}},
		brain.KindSpec{Kind: "Person", IsParent: true},
	); err != nil {
		t.Fatal(err)
	}
	ns := storetest.Namespace(t, "org", uuid.NewString())
	inner, _ := scripted(t)
	model := &toolsSeen{Model: inner}
	h := planned(t, "schema", tacklr.AgentOptions{
		Model: model, MaxWindowSize: 100000, Brain: eng, SearchNamespace: ns,
		BrainWriteKinds: []string{"Deal", "Person", "Memory"},
		BrainRelations: []brain.RelationSpec{
			{Relation: "stakeholder", Description: "a person on the deal", From: []string{"Deal"}, To: []string{"Person"}},
			{Relation: "mentions"},
		},
	}, tacklr.Todo{Title: "record"})
	if _, err := h.Infer(ctx, newStream(t).out); err != nil {
		t.Fatal(err)
	}
	tools := map[string]*tacklr.Tool{}
	for _, tool := range model.tools {
		tools[tool.QualifiedName()] = tool
	}
	run := func(name, args string) string { return result(t, h, call("s", name, args)) }
	id := func(out string) string {
		var obj struct{ ID string }
		if err := json.Unmarshal([]byte(out), &obj); err != nil {
			t.Fatalf("saved %q: %v", out, err)
		}
		return obj.ID
	}
	deal := id(committedResult(t, h, "save", `{"kind":"Deal","title":"Acme","properties":{"status":"open"}}`))
	person := id(committedResult(t, h, "save", `{"kind":"Person","title":"Sam"}`))
	link := func(from, to, relation, extra string) string {
		return run("link", `{"from":"`+from+`","to":"`+to+`","relation":"`+relation+`"`+extra+`}`)
	}

	linked := link(deal, person, "stakeholder", `,"role":"champion","note":"pushes it internally"`)
	related := run("related", `{"ref":"`+deal+`","relations":["stakeholder"]}`)
	backwards := link(person, deal, "stakeholder", "")
	unknown := link(deal, person, "about", "")
	anyEnds := link(person, deal, "mentions", "")
	removeBackwards := link(person, deal, "stakeholder", `,"remove":true`)
	missingTo := link(deal, uuid.NewString(), "stakeholder", "")
	missingFrom := link(uuid.NewString(), person, "stakeholder", "")

	save, linkTool := tools["save"].Description(), tools["link"].Description()
	if !containsAll(save, "Kinds you may save:", "- Deal: An open opportunity.", "status (string, required, one of open|closed) Where it stands.", "- Person", "- Memory") {
		t.Errorf("save description = %q, want each kind with its description and fields", save)
	}
	if !containsAll(linkTool, "Relations (from → to):", "- stakeholder (Deal → Person): a person on the deal", "- mentions (any → any)") {
		t.Errorf("link description = %q, want each relation with its ends", linkTool)
	}
	raw, _ := json.Marshal(tools["link"].Parameters())
	if !strings.Contains(string(raw), `"enum":["stakeholder","mentions"]`) {
		t.Errorf("link parameters = %s, want relation to be one of the host's relations", raw)
	}
	if !containsAll(linked, `"linked": true`, `"role": "champion"`) || !containsAll(related, `"role": "champion"`, "pushes it internally") {
		t.Errorf("link = %s, related = %s; want the link kept with its role and note", linked, related)
	}
	if !strings.Contains(backwards, "stakeholder links from Deal, not Person") {
		t.Errorf("a stakeholder link from a person = %q, want it refused for its ends", backwards)
	}
	if !strings.Contains(unknown, `relation "about"; relations: stakeholder, mentions`) {
		t.Errorf("an unknown relation = %q, want it refused with the relations", unknown)
	}
	if !strings.Contains(anyEnds, `"linked": true`) || !strings.Contains(removeBackwards, `"linked": false`) {
		t.Errorf("a relation with any ends = %q, a remove = %q; want both to go", anyEnds, removeBackwards)
	}
	if !strings.Contains(missingTo, "link: to ") || !strings.Contains(missingFrom, "link: from ") {
		t.Errorf("a link to a missing record = %q, from one = %q; want both refused", missingTo, missingFrom)
	}
}

// toolsSeen is a model that keeps the tools of the last request it got.
type toolsSeen struct {
	tacklr.Model
	tools []*tacklr.Tool
}

func (m *toolsSeen) Invoke(ctx context.Context, req tacklr.ModelRequest) (chan tacklr.LLMResponseChunk, error) {
	m.tools = req.Tools
	return m.Model.Invoke(ctx, req)
}

// TestRelationSpec_endsNamesTheKindsEachSideTakes covers a spec with only
// a target kind.
func TestRelationSpec_endsNamesTheKindsEachSideTakes(t *testing.T) {
	t.Parallel()
	spec := brain.RelationSpec{Relation: "about", To: []string{"Deal", "Account"}}
	if err := spec.Ends("Email", "Deal"); err != nil {
		t.Errorf("about from an Email to a Deal = %v, want it allowed", err)
	}
	if err := spec.Ends("Email", "Person"); err == nil || !strings.Contains(err.Error(), "about links to Deal or Account, not Person") {
		t.Errorf("about to a Person = %v, want it refused", err)
	}
	if !slices.Equal(spec.To, []string{"Deal", "Account"}) {
		t.Errorf("Ends changed the spec: %v", spec.To)
	}
}
