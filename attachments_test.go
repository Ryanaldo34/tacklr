package tacklr

import (
	"context"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr/stores"
)

func TestAttachDocumentMakesBytesAvailableThroughVFSAndContext(t *testing.T) {
	h := NewAgent(context.Background(), AgentOptions{SessionID: "attach", Store: stores.NewInMemoryStore(), Model: &mockStrategy{}})
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
}
