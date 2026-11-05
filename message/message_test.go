package message_test

import (
	"slices"
	"testing"

	"github.com/ryanaldo34/tacklr/message"
)

// TestToolCall_KeyAndWireID: Key prefers the provider item id and WireID
// prefers the call id; each falls back to the other id.
func TestToolCall_KeyAndWireID(t *testing.T) {
	both := message.ToolCall{ID: "i", CallID: "c"}
	idOnly := message.ToolCall{ID: "i"}
	callOnly := message.ToolCall{CallID: "c"}

	got := []string{both.Key(), callOnly.Key(), both.WireID(), idOnly.WireID()}

	if want := []string{"i", "c", "c", "i"}; !slices.Equal(got, want) {
		t.Fatalf("Key, Key, WireID, WireID = %q, want %q", got, want)
	}
}

// TestMessage_MIMETypesListsEachBinaryTypeOnce: a message lists the
// normalized MIME type of each image and file part once, in first-seen
// order, from the file data or else the image's data URL. Text parts, text
// files and an image URL that is not a data URL add nothing. DataURL builds
// the URL that MIMETypes reads back.
func TestMessage_MIMETypesListsEachBinaryTypeOnce(t *testing.T) {
	m := &message.Message{ContentParts: []message.ContentPart{
		{Type: message.ContentTypeInputText, Text: "hi"},
		{Type: message.ContentTypeInputImage, FileData: &message.FileData{MIMEType: " Image/PNG; charset=binary "}},
		{Type: message.ContentTypeInputImage, FileData: &message.FileData{MIMEType: "image/png"}},
		{Type: message.ContentTypeInputImage, ImageURL: &message.ImageURL{URL: message.DataURL(" Image/WEBP ", "x")}},
		{Type: message.ContentTypeInputImage, ImageURL: &message.ImageURL{URL: "https://example.test/a.gif"}},
		{Type: message.ContentTypeInputImage, ImageURL: &message.ImageURL{URL: message.DataURL("", message.DataURL("image/gif", "y"))}},
		{Type: message.ContentTypeInputFile, FileData: &message.FileData{MIMEType: "application/pdf"}},
		{Type: message.ContentTypeInputFile, FileData: &message.FileData{MIMEType: "text/plain"}},
		{Type: message.ContentTypeInputImage, ImageURL: &message.ImageURL{URL: message.DataURL("", "z")}},
	}}

	got := m.MIMETypes()
	var none *message.Message

	want := []string{"image/png", "image/webp", "image/gif", "application/pdf", "application/octet-stream"}
	if !slices.Equal(got, want) {
		t.Fatalf("MIMETypes = %q, want %q", got, want)
	}
	if none.MIMETypes() != nil || (&message.Message{Content: "text"}).MIMETypes() != nil {
		t.Fatal("a nil message or one without parts listed MIME types")
	}
}
