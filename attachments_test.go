package tacklr

import (
	"context"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/stores"
)

func TestAttachDocumentMakesBytesAvailableThroughVFSAndContext(t *testing.T) {
	h, err := NewAgent(context.Background(), AgentOptions{SessionID: "attach", Store: stores.NewInMemoryStore(), Model: &mockStrategy{}})
	if err != nil {
		t.Fatal(err)
	}
	path, err := h.AttachDocument(context.Background(), "report.txt", []byte("important text\n"))
	if err != nil {
		t.Fatal(err)
	}
	if path != "/context/report.txt" {
		t.Fatalf("path=%q", path)
	}
	got, err := h.session.VFS.ReadFile(context.Background(), path)
	if err != nil || string(got) != "important text\n" {
		t.Fatalf("got=%q err=%v", got, err)
	}
	msgs := h.Messages()
	if len(msgs) == 0 || !strings.Contains(msgs[len(msgs)-1].Content, path) {
		t.Fatalf("messages=%#v", msgs)
	}
	// Second attach reuses the /context mount.
	if _, err := h.AttachDocument(context.Background(), "other.txt", []byte("x")); err != nil {
		t.Fatal(err)
	}
}

func TestAttachDocument_rejectsBadNameAndNilSession(t *testing.T) {
	var none *AgentHarness
	if _, err := none.AttachDocument(context.Background(), "a.txt", []byte("x")); err == nil {
		t.Fatal("nil harness")
	}
	h, err := NewAgent(context.Background(), AgentOptions{SessionID: "attach-bad", Store: stores.NewInMemoryStore(), Model: &mockStrategy{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", ".", "..", "/"} {
		if _, err := h.AttachDocument(context.Background(), name, []byte("x")); err == nil {
			t.Fatalf("name %q should fail", name)
		}
	}
}
