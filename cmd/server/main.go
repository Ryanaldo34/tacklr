package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"

	"gitlab.com/turf-suite/tackle/internal/core"
)

var port *int

func init() {
	port = flag.Int("port", 6969, "Port to run the server")
}

type promptRequest struct {
	Prompt string `json:"prompt"`
}

type sseEvent struct {
	Type      string          `json:"type"`
	Content   string          `json:"content,omitempty"`
	ToolCalls []core.ToolCall `json:"tool_calls,omitempty"`
	Error     string          `json:"error,omitempty"`
}

func main() {
	flag.Parse()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /", handlePrompt)

	s := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}
	log.Printf("Starting server on :%d", *port)
	log.Fatal(s.ListenAndServe())
}

func handlePrompt(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeSSEError(w, flusher, "failed to read request body")
		return
	}

	var req promptRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeSSEError(w, flusher, "invalid JSON: "+err.Error())
		return
	}
	if req.Prompt == "" {
		writeSSEError(w, flusher, "prompt is required")
		return
	}

	strategy := core.NewOpenAIInferenceStrategy(http.DefaultClient).
		WithURL("http://localhost:8080/v1").
		WithModel("gemma-4-26B-A4B-it-UD-Q4_K_M.gguf").
		WithApiKey("not-needed").
		WithResponseStrategy("standard")

	getWeather := &core.Tool{
		Name:        "get_weather",
		Description: "Get the current weather for a given location",
		Handler: func(args struct {
			Location string `json:"location" desc:"City or location name"`
		}) (string, error) {
			conditions := []string{"Sunny", "Cloudy", "Rainy", "Windy", "Snowy"}
			condition := conditions[(len(args.Location))%len(conditions)]
			temp := 60 + (len(args.Location)*7)%30
			return fmt.Sprintf(`{"temperature":%d,"condition":"%s","location":"%s"}`, temp, condition, args.Location), nil
		},
	}
	if err := getWeather.Validate(); err != nil {
		writeSSEError(w, flusher, "tool validation failed: "+err.Error())
		return
	}

	harness := &core.AgentHarness{
		Model:         strategy,
		Tools:         []*core.Tool{getWeather},
		SystemPrompt:  "You are a helpful assistant with access to a get_weather tool. When asked about the weather, use the tool to look it up before responding.",
		WatchDog:      &core.StdioWatchDog{},
		MaxWindowSize: 8192,
	}
	harness.WithStreamingStrategy("buffered")

	events, err := harness.Run(r.Context(), req.Prompt)
	if err != nil {
		writeSSEError(w, flusher, "agent error: "+err.Error())
		return
	}

	for ev := range events {
		data, _ := json.Marshal(toSSEEvent(ev))
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, data)
		flusher.Flush()
	}
}

func toSSEEvent(ev core.StreamEvent) sseEvent {
	e := sseEvent{Type: string(ev.Type), Content: ev.Content, ToolCalls: ev.ToolCalls}
	if ev.Error != nil {
		e.Error = ev.Error.Error()
	}
	return e
}

func writeSSEEvent(w io.Writer, flusher http.Flusher, evType string, data []byte) {
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evType, data)
	flusher.Flush()
}

func writeSSEError(w http.ResponseWriter, flusher http.Flusher, msg string) {
	data, _ := json.Marshal(sseEvent{Type: "error", Error: msg})
	writeSSEEvent(w, flusher, "error", data)
}
