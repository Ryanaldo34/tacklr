package tacklr

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/ryanaldo34/tacklr/brain"
)

func TestBrainTools_hostNamespaceScopedRead(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	other := uuid.New()
	docID := uuid.New()

	if err := store.Put(brain.Object{
		ID: docID, Kind: "Document", Title: "Deal memo",
		Summary: "Q3", Content: "full body", ContentType: "text/plain",
		NamespaceID: ns, Properties: map[string]any{"stage": "negotiation"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.PutKind(brain.ObjectKind{
		Kind: "Document", Description: "docs", IsParent: true,
		FilterableFields: json.RawMessage(`[]`),
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}

	h := NewAgent(ctx, AgentOptions{
		Config:          Config{MaxWindowSize: 1024},
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: &ns,
	})
	gotNS, ok := h.SearchNamespace()
	if !ok || gotNS != ns {
		t.Fatalf("SearchNamespace from options: %v %v", gotNS, ok)
	}

	readTool := h.findTool("read", "")
	schemaTool := h.findTool("schema", "")
	if readTool == nil || schemaTool == nil {
		t.Fatal("brain tools must be injected when Brain is configured")
	}

	out, err := readTool.invoke(ctx, `{"object_id":"`+docID.String()+`"}`, h.runtime)
	if err != nil {
		t.Fatal(err)
	}
	var rich brain.RichObject
	if err := json.Unmarshal([]byte(out.output), &rich); err != nil {
		t.Fatalf("read JSON: %v\n%s", err, out.output)
	}
	if rich.Content != "full body" || rich.Title != "Deal memo" || rich.ID != docID {
		t.Fatalf("rich object: %+v", rich)
	}

	h.SetSearchNamespace(other)
	_, err = readTool.invoke(ctx, `{"object_id":"`+docID.String()+`"}`, h.runtime)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("want not found under other namespace, got %v", err)
	}

	h.ClearSearchNamespace()
	out, err = readTool.invoke(ctx, `{"object_id":"`+docID.String()+`"}`, h.runtime)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.output, "full body") {
		t.Fatalf("cleared namespace read: %s", out.output)
	}

	sout, err := schemaTool.invoke(ctx, `{"kind":"Document"}`, h.runtime)
	if err != nil {
		t.Fatal(err)
	}
	var schema brain.SchemaResult
	if err := json.Unmarshal([]byte(sout.output), &schema); err != nil {
		t.Fatal(err)
	}
	if len(schema.Kinds) != 1 || schema.Kinds[0].Kind != "Document" {
		t.Fatalf("schema: %+v", schema)
	}

	_, err = readTool.invoke(ctx, `{"object_id":"not-a-uuid"}`, h.runtime)
	if err == nil {
		t.Fatal("invalid object_id must fail")
	}
}

func TestWorkerInheritsBrainAndNamespace(t *testing.T) {
	ctx := context.Background()
	store := brain.NewMemoryStore()
	ns := uuid.New()
	docID := uuid.New()
	if err := store.Put(brain.Object{
		ID: docID, Kind: "Document", Title: "Shared", Content: "worker-visible",
		NamespaceID: ns,
	}); err != nil {
		t.Fatal(err)
	}
	eng, err := brain.NewEngine(store)
	if err != nil {
		t.Fatal(err)
	}

	workerModel := &mockStrategy{
		invokeFn: func(ctx context.Context, msgs []*Message, tools []*Tool, ch chan<- LLMResponseChunk) {
			ch <- LLMResponseChunk{Type: StreamEventMessage, Content: "ok", IsComplete: true}
		},
	}
	parent := NewAgent(ctx, AgentOptions{
		Config:          Config{MaxWindowSize: 1024},
		Model:           &mockStrategy{},
		Brain:           eng,
		SearchNamespace: &ns,
		SubAgents: []*SubAgent{
			{WorkerName: "researcher", Model: workerModel},
		},
	})

	worker := parent.newWorkerHarness(ctx, "researcher", "spawn_tc1", parent.subagents["researcher"])

	gotNS, ok := worker.SearchNamespace()
	if !ok || gotNS != ns {
		t.Fatalf("worker namespace %v %v, want %v", gotNS, ok, ns)
	}

	readTool := worker.findTool("read", "")
	if readTool == nil {
		t.Fatal("worker must inherit brain read tool")
	}
	out, err := readTool.invoke(ctx, `{"object_id":"`+docID.String()+`"}`, worker.runtime)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.output, "worker-visible") {
		t.Fatalf("worker read: %s", out.output)
	}

	parent.ClearSearchNamespace()
	gotNS, ok = worker.SearchNamespace()
	if !ok || gotNS != ns {
		t.Fatalf("worker namespace after parent clear: %v %v", gotNS, ok)
	}
}
