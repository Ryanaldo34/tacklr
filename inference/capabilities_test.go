package inference

import (
	"errors"
	"strings"
	"testing"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/telemetry"
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
	s3 := NewOpenAIInferenceStrategy(nil)
	if s3.SupportsMIME("image/png") {
		t.Fatal("empty model rejects image")
	}
}

func TestMaxContextWindow_knownPrefixAndUnknown(t *testing.T) {
	s := NewOpenAIInferenceStrategy(nil).WithModel("gpt-5.4")
	n, err := s.MaxContextWindow()
	if err != nil || n != 1000000 {
		t.Fatalf("gpt-5.4: n=%d err=%v", n, err)
	}
	n, err = NewOpenAIInferenceStrategy(nil).WithModel("o3-custom").MaxContextWindow()
	if err != nil || n != 200000 {
		t.Fatalf("o3 prefix: n=%d err=%v", n, err)
	}
	n, err = NewOpenAIInferenceStrategy(nil).WithModel("gpt-5-preview").MaxContextWindow()
	if err != nil || n != 1000000 {
		t.Fatalf("gpt-5 prefix: n=%d err=%v", n, err)
	}
	_, err = NewOpenAIInferenceStrategy(nil).WithModel("mystery-model").MaxContextWindow()
	if err == nil || !errors.Is(err, tacklr.ErrUnknownModel) {
		t.Fatalf("unknown: err=%v", err)
	}
}

func TestModelTelemetryIdentity_openaiURL(t *testing.T) {
	id := NewOpenAIInferenceStrategy(nil).WithModel("gpt-test").WithURL("https://api.openai.com/v1").ModelTelemetryIdentity()
	if id.Model != "gpt-test" || id.Provider != telemetry.GenAIProviderOpenAI {
		t.Fatalf("%+v", id)
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
