package inference

import (
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
)

func TestSupportsMIME_visionAndPDF(t *testing.T) {
	s := NewOpenAIInferenceStrategy(nil).WithModel("gpt-4o")
	if !s.SupportsMIME("text/plain") || !s.SupportsMIME("") {
		t.Fatal("text always supported")
	}
	if !s.SupportsMIME("image/png") || !s.SupportsMIME("image/jpeg") {
		t.Fatal("gpt-4o should support images")
	}
	if !s.SupportsMIME("application/pdf") {
		t.Fatal("gpt-4o should support PDF")
	}
	if s.SupportsMIME("application/zip") {
		t.Fatal("unknown binary rejected")
	}
	s2 := NewOpenAIInferenceStrategy(nil).WithModel("unknown-local-llm")
	if s2.SupportsMIME("image/png") || s2.SupportsMIME("application/pdf") {
		t.Fatal("unknown model rejects binary")
	}
	if !s2.SupportsMIME("text/markdown") {
		t.Fatal("text/* always true")
	}
	// Empty model id: no binary.
	s3 := NewOpenAIInferenceStrategy(nil)
	if s3.SupportsMIME("image/png") {
		t.Fatal("empty model rejects image")
	}
	// Nil receiver path via interface still text-safe.
	var nilS *OpenAIInferenceStrategy
	if !nilS.SupportsMIME("text/plain") || nilS.SupportsMIME("image/png") {
		t.Fatal("nil strategy text only")
	}
}

func TestMarshalMessagesToInput_multimodal(t *testing.T) {
	msgs := []*tacklr.Message{
		{
			Role:    tacklr.RoleUser,
			Content: "describe",
			ContentParts: []tacklr.ContentPart{
				{Type: tacklr.ContentTypeInputText, Text: "describe"},
				{Type: tacklr.ContentTypeInputImage, ImageURL: &tacklr.ImageURL{URL: "data:image/png;base64,AAAA"}},
				{Type: tacklr.ContentTypeInputFile, FileData: &tacklr.FileData{
					Data: "data:application/pdf;base64,JVBERg==", MIMEType: "application/pdf", Filename: "a.pdf",
				}},
			},
		},
	}
	items := marshalMessagesToInput(msgs)
	if len(items) != 1 {
		t.Fatalf("items=%d", len(items))
	}
	body := string(items[0])
	for _, want := range []string{`"input_text"`, `"input_image"`, `"input_file"`, `"data:image/png;base64,AAAA"`, `"a.pdf"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %s in %s", want, body)
		}
	}
	// Text-only unchanged shape
	plain := marshalMessagesToInput([]*tacklr.Message{{Role: tacklr.RoleUser, Content: "hi"}})
	if !strings.Contains(string(plain[0]), `"content":"hi"`) {
		t.Fatalf("plain = %s", plain[0])
	}
}

func TestUnsupportedMIMEs(t *testing.T) {
	s := NewOpenAIInferenceStrategy(nil).WithModel("unknown")
	bad := tacklr.UnsupportedMIMEs(s, []string{"text/plain", "image/png", "image/png", "application/pdf"})
	if len(bad) != 2 {
		t.Fatalf("bad=%v", bad)
	}
}
