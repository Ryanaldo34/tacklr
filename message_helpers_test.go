package tacklr

import "testing"

func TestToolCall_KeyAndWireID(t *testing.T) {
	if (ToolCall{ID: "i", CallID: "c"}).Key() != "i" {
		t.Fatal("Key prefers ID")
	}
	if (ToolCall{CallID: "c"}).Key() != "c" {
		t.Fatal("Key falls back to CallID")
	}
	if (ToolCall{ID: "i", CallID: "c"}).WireID() != "c" {
		t.Fatal("WireID prefers CallID")
	}
	if (ToolCall{ID: "i"}).WireID() != "i" {
		t.Fatal("WireID falls back to ID")
	}
}

// TestMIMEHelpers_andMessageMIMETypes covers NormalizeMIME/DataURL/MIMEFromDataURL
// and Message.MIMETypes first-seen binary collection.
func TestMIMEHelpers_andMessageMIMETypes(t *testing.T) {
	if NormalizeMIME(" Image/PNG; charset=binary ") != "image/png" {
		t.Fatal("NormalizeMIME")
	}
	if !IsTextMIME("") || !IsTextMIME("text/plain") || IsTextMIME("image/png") {
		t.Fatal("IsTextMIME")
	}
	if MIMEFromDataURL("https://x") != "" {
		t.Fatal("non-data URL")
	}
	if MIMEFromDataURL("data:image/jpeg;base64,AAAA") != "image/jpeg" {
		t.Fatal("MIMEFromDataURL")
	}
	if DataURL("image/png", "data:image/png;base64,XX") != "data:image/png;base64,XX" {
		t.Fatal("DataURL passthrough")
	}
	if DataURL("", "abc") != "data:application/octet-stream;base64,abc" {
		t.Fatal("DataURL default mime")
	}
	if DataURL("image/png", "abc") != "data:image/png;base64,abc" {
		t.Fatal("DataURL build")
	}

	var nilMsg *Message
	if nilMsg.MIMETypes() != nil {
		t.Fatal("nil message")
	}
	if (&Message{}).MIMETypes() != nil {
		t.Fatal("empty parts")
	}
	m := &Message{ContentParts: []ContentPart{
		{Type: ContentTypeInputText, Text: "hi"},
		{Type: ContentTypeInputImage, FileData: &FileData{MIMEType: "image/png"}},
		{Type: ContentTypeInputImage, FileData: &FileData{MIMEType: "image/png"}}, // dedupe
		{Type: ContentTypeInputImage, ImageURL: &ImageURL{URL: "data:image/webp;base64,x"}},
		{Type: ContentTypeInputFile, FileData: &FileData{MIMEType: "application/pdf"}},
		{Type: ContentTypeInputFile, FileData: &FileData{MIMEType: "text/plain"}}, // ignored
	}}
	got := m.MIMETypes()
	if len(got) != 3 || got[0] != "image/png" || got[1] != "image/webp" || got[2] != "application/pdf" {
		t.Fatalf("MIMETypes = %v", got)
	}
	// Image without FileData MIME uses data URL parse.
	m2 := &Message{ContentParts: []ContentPart{
		{Type: ContentTypeInputImage, ImageURL: &ImageURL{URL: "data:image/gif;base64,y"}},
	}}
	if g := m2.MIMETypes(); len(g) != 1 || g[0] != "image/gif" {
		t.Fatalf("data-url only = %v", g)
	}
}
