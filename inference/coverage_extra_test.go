package inference

import (
	"testing"

	"github.com/ryanaldo34/tacklr/telemetry"
)

func TestAPIStatusError_andUsageParse(t *testing.T) {
	var nilErr *APIStatusError
	if nilErr.Error() == "" || nilErr.ProviderHTTPStatus() != 0 || nilErr.ProviderErrorCode() != "" {
		t.Fatal("nil receiver")
	}
	e := &APIStatusError{Status: 400, Body: "  bad  ", Code: "x"}
	if e.ProviderHTTPStatus() != 400 || e.ProviderErrorCode() != "x" {
		t.Fatal(e)
	}
	if e.Error() == "" {
		t.Fatal("error string")
	}
	e2 := &APIStatusError{Status: 500, Body: ""}
	if e2.Error() == "" {
		t.Fatal("no body")
	}

	// Nested reasoning tokens
	data := `{"response":{"usage":{"input_tokens":1,"output_tokens":2,"output_tokens_details":{"reasoning_tokens":3}}}}`
	u, ok := parseResponseUsage(data)
	if !ok || u.Input != 1 || u.Output != 2 || u.Reasoning != 3 {
		t.Fatalf("%+v %v", u, ok)
	}
	// Flat reasoning
	data2 := `{"response":{"usage":{"input_tokens":1,"output_tokens":2,"reasoning_tokens":4}}}`
	u, ok = parseResponseUsage(data2)
	if !ok || u.Reasoning != 4 {
		t.Fatalf("%+v", u)
	}
	// Zero usage → false
	if _, ok := parseResponseUsage(`{"response":{"usage":{"input_tokens":0,"output_tokens":0}}}`); ok {
		t.Fatal("zero")
	}
	if _, ok := parseResponseUsage(`{`); ok {
		t.Fatal("bad json")
	}
	if _, ok := parseResponseUsage(`{"response":{}}`); ok {
		t.Fatal("nil usage")
	}
}

func TestOpenAIStrategy_telemetryAndMaxTokens(t *testing.T) {
	var nilS *OpenAIInferenceStrategy
	if id := nilS.ModelTelemetryIdentity(); id != (telemetry.ModelIdentity{}) {
		t.Fatalf("%+v", id)
	}
	s := NewOpenAIInferenceStrategy(nil).WithApiKey("k").WithModel("gpt-test").(*OpenAIInferenceStrategy)
	s.WithURL("https://api.openai.com/v1")
	id := s.ModelTelemetryIdentity()
	if id.Model != "gpt-test" || id.Provider != telemetry.GenAIProviderOpenAI {
		t.Fatalf("%+v", id)
	}
	s.WithMaxOutputTokens(-5)
	if s.maxOutputTokens != 0 {
		t.Fatal(s.maxOutputTokens)
	}
	s.WithMaxOutputTokens(4096)
	if s.maxOutputTokens != 4096 {
		t.Fatal(s.maxOutputTokens)
	}
}
