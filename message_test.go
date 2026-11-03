package tacklr

import (
	"testing"

	"github.com/ryanaldo34/tacklr/message"
)

func TestToolCall_KeyAndWireID(t *testing.T) {
	if (message.ToolCall{ID: "i", CallID: "c"}).Key() != "i" {
		t.Fatal("Key prefers ID")
	}
	if (message.ToolCall{CallID: "c"}).Key() != "c" {
		t.Fatal("Key falls back to CallID")
	}
	if (message.ToolCall{ID: "i", CallID: "c"}).WireID() != "c" {
		t.Fatal("WireID prefers CallID")
	}
	if (message.ToolCall{ID: "i"}).WireID() != "i" {
		t.Fatal("WireID falls back to ID")
	}
}

// TestMIMEHelpers_andMessageMIMETypes covers NormalizeMIME/DataURL/MIMEFromDataURL
// and Message.MIMETypes first-seen binary collection.
func TestMIMEHelpers_andMessageMIMETypes(t *testing.T) {
	if message.NormalizeMIME(" Image/PNG; charset=binary ") != "image/png" {
		t.Fatal("NormalizeMIME")
	}
	if !message.IsTextMIME("") || !message.IsTextMIME("text/plain") || message.IsTextMIME("image/png") {
		t.Fatal("IsTextMIME")
	}
	if message.MIMEFromDataURL("https://x") != "" {
		t.Fatal("non-data URL")
	}
	if message.MIMEFromDataURL("data:image/jpeg;base64,AAAA") != "image/jpeg" {
		t.Fatal("MIMEFromDataURL")
	}
	if message.DataURL("image/png", "data:image/png;base64,XX") != "data:image/png;base64,XX" {
		t.Fatal("DataURL passthrough")
	}
	if message.DataURL("", "abc") != "data:application/octet-stream;base64,abc" {
		t.Fatal("DataURL default mime")
	}
	if message.DataURL("image/png", "abc") != "data:image/png;base64,abc" {
		t.Fatal("DataURL build")
	}

	var nilMsg *message.Message
	if nilMsg.MIMETypes() != nil {
		t.Fatal("nil message")
	}
	if (&message.Message{}).MIMETypes() != nil {
		t.Fatal("empty parts")
	}
	m := &message.Message{ContentParts: []message.ContentPart{
		{Type: message.ContentTypeInputText, Text: "hi"},
		{Type: message.ContentTypeInputImage, FileData: &message.FileData{MIMEType: "image/png"}},
		{Type: message.ContentTypeInputImage, FileData: &message.FileData{MIMEType: "image/png"}}, // dedupe
		{Type: message.ContentTypeInputImage, ImageURL: &message.ImageURL{URL: "data:image/webp;base64,x"}},
		{Type: message.ContentTypeInputFile, FileData: &message.FileData{MIMEType: "application/pdf"}},
		{Type: message.ContentTypeInputFile, FileData: &message.FileData{MIMEType: "text/plain"}}, // ignored
	}}
	got := m.MIMETypes()
	if len(got) != 3 || got[0] != "image/png" || got[1] != "image/webp" || got[2] != "application/pdf" {
		t.Fatalf("MIMETypes = %v", got)
	}
	// Image without FileData MIME uses data URL parse.
	m2 := &message.Message{ContentParts: []message.ContentPart{
		{Type: message.ContentTypeInputImage, ImageURL: &message.ImageURL{URL: "data:image/gif;base64,y"}},
	}}
	if g := m2.MIMETypes(); len(g) != 1 || g[0] != "image/gif" {
		t.Fatalf("data-url only = %v", g)
	}
}
