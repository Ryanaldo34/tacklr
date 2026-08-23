package agentbench

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/brain"
	"github.com/ryanaldo34/tacklr/inference"
)

// Config configures a benchmark run.
type Config struct {
	Suites      []string
	CaseFilter  string // optional exact case id
	ModelURL    string
	ModelAPIKey string
	ModelName   string
	// EmbedURL defaults to ModelURL. EmbedAPIKey defaults to ModelAPIKey.
	// EmbedModel defaults to DefaultEmbedModel / OPENAI_EMBEDDING_MODEL.
	EmbedURL    string
	EmbedAPIKey string
	EmbedModel  string
	// LexicalOnly disables the dense channel (no WithEmbedder).
	LexicalOnly bool
	ExaAPIKey   string
	Timeout     time.Duration // per case
	DryRun      bool
}

// Run executes suites and returns a scorecard.
func Run(ctx context.Context, cfg Config) (Report, error) {
	start := time.Now()
	cfg = cfg.withDefaults()
	rep := Report{
		Model:      cfg.ModelName,
		EmbedModel: cfg.embedModelLabel(),
		StartedAt:  start,
		Suites:     map[string]SuiteResult{},
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 4 * time.Minute
	}
	suites := cfg.Suites
	if len(suites) == 0 {
		suites = AllSuites
	}
	exa := strings.TrimSpace(cfg.ExaAPIKey)
	if exa == "" {
		exa = strings.TrimSpace(os.Getenv("EXA_API_KEY"))
	}

	for _, suite := range suites {
		cases := CasesForSuite(suite)
		sr := SuiteResult{Suite: suite, Cases: nil}
		for _, c := range cases {
			if cfg.CaseFilter != "" && c.ID != cfg.CaseFilter {
				continue
			}
			if c.RequiresExa && exa == "" && !cfg.DryRun {
				sr.Skipped++
				sr.Cases = append(sr.Cases, CaseResult{
					ID: c.ID, Suite: c.Suite, Skipped: true, SkipWhy: "EXA_API_KEY not set",
				})
				continue
			}
			if cfg.DryRun {
				sr.Passed++
				note := "dry-run: case registered"
				if c.RequiresExa && exa == "" {
					note = "dry-run: case registered (would skip without EXA_API_KEY)"
				}
				sr.Cases = append(sr.Cases, CaseResult{
					ID: c.ID, Suite: c.Suite, Success: true, Notes: []string{note},
					Scores: map[string]float64{"success": 1},
				})
				continue
			}
			cr := runCase(ctx, cfg, c, exa)
			sr.N++
			sr.Cases = append(sr.Cases, cr)
			if cr.Success {
				sr.Passed++
			} else {
				sr.Failed++
			}
		}
		done := sr.Passed + sr.Failed
		if done > 0 {
			sr.SuccessRate = float64(sr.Passed) / float64(done)
		}
		sr.N = done
		rep.Suites[suite] = sr
	}

	rep.Duration = time.Since(start)
	rep.GatesOK, rep.GateNotes = EvaluateGates(rep, exa != "")
	return rep, nil
}

func (c Config) withDefaults() Config {
	if strings.TrimSpace(c.EmbedURL) == "" {
		c.EmbedURL = c.ModelURL
	}
	if strings.TrimSpace(c.EmbedAPIKey) == "" {
		c.EmbedAPIKey = c.ModelAPIKey
	}
	if strings.TrimSpace(c.EmbedModel) == "" {
		c.EmbedModel = DefaultEmbedModel
	}
	return c
}

func (c Config) embedModelLabel() string {
	if c.LexicalOnly {
		return "lexical_only"
	}
	return strings.TrimSpace(c.EmbedModel)
}

func (c Config) newEmbedder(httpClient *http.Client) brain.QueryEmbedder {
	if c.LexicalOnly {
		return nil
	}
	return &OpenAIEmbedder{
		BaseURL:    c.EmbedURL,
		APIKey:     c.EmbedAPIKey,
		Model:      c.EmbedModel,
		HTTPClient: httpClient,
	}
}

func runCase(ctx context.Context, cfg Config, c Case, exa string) CaseResult {
	caseCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	ns := uuid.New()
	sessionID := "bench-" + c.ID + "-" + uuid.NewString()[:8]
	httpClient := &http.Client{Timeout: 120 * time.Second}

	brainStore := brain.NewMemoryStore()
	graph := brain.NewMemoryGraph()
	opts := []brain.EngineOption{
		brain.WithGraph(graph),
		brain.WithKinds(KindSpecs()...),
		// Fail closed on embed errors so hybrid is real, not silently lexical-only.
		brain.WithConfig(brain.EngineConfig{FailOnEmbedderError: !cfg.LexicalOnly}),
	}
	if emb := cfg.newEmbedder(httpClient); emb != nil {
		opts = append(opts, brain.WithEmbedder(emb))
	}
	eng, err := brain.NewEngine(brainStore, opts...)
	if err != nil {
		return failResult(c, nil, "new engine: "+err.Error())
	}
	scope := brain.Scope{Namespace: &ns}
	// Seed Puts dual-write embeddings when WithEmbedder is set.
	if err := ApplySeed(caseCtx, eng, scope, c.Seed); err != nil {
		return failResult(c, nil, "seed: "+err.Error())
	}

	model := inference.NewOpenAIInferenceStrategy(httpClient)
	model.WithURL(cfg.ModelURL).WithApiKey(cfg.ModelAPIKey).WithModel(cfg.ModelName)
	// Cap completion size (reasoning + visible text). Context window is separate (MaxWindowSize).
	maxOut := 32_768
	if v := strings.TrimSpace(os.Getenv("MAX_OUTPUT_TOKENS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxOut = n
		}
	}
	if maxOut > 0 {
		model.WithMaxOutputTokens(maxOut)
	}
	if effort := strings.TrimSpace(os.Getenv("OPENAI_REASONING_EFFORT")); effort != "" {
		model.WithReasoningLevel(effort)
	}
	if summary := strings.TrimSpace(os.Getenv("OPENAI_REASONING_SUMMARY")); summary != "" {
		model.WithReasoningSummary(summary)
	} else if strings.TrimSpace(os.Getenv("OPENAI_REASONING_EFFORT")) == "" {
		model.WithReasoningSummary("auto")
	}

	maxWindow := 1_000_000 // default matches Luna-class; override with MAX_WINDOW_SIZE
	if v := strings.TrimSpace(os.Getenv("MAX_WINDOW_SIZE")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxWindow = n
		}
	}

	agentOpts := func() tacklr.AgentOptions {
		return tacklr.AgentOptions{
			Config: tacklr.Config{
				MaxWindowSize: maxWindow,
				SystemPrompt:  SharedSystemPrompt,
			},
			SessionID:       sessionID,
			Model:           model,
			Brain:           eng,
			BrainWriteKinds: brain.WriteKinds{Discovery: "Discovery", Fact: "Fact", Memory: "Memory"},
			SearchNamespace: &ns,
			ExaAPIKey:       exa,
		}
	}

	var turns []TurnTrace
	agent, err := tacklr.NewAgent(caseCtx, agentOpts())
	if err != nil {
		return failResult(c, turns, "construct agent: "+err.Error())
	}
	for i, prompt := range c.Turns {
		if c.RestoreSession && i == len(c.Turns)-1 && i > 0 {
			cp, err := agent.Checkpoint()
			if err != nil {
				return failResult(c, turns, "checkpoint: "+err.Error())
			}
			agent.Close()
			loaded, err := tacklr.NewAgent(caseCtx, agentOpts())
			if err != nil {
				return failResult(c, turns, "restore session: "+err.Error())
			}
			if err := loaded.RestoreCheckpoint(*cp); err != nil {
				return failResult(c, turns, "restore checkpoint: "+err.Error())
			}
			agent = loaded
		}
		tr, err := runTurn(caseCtx, agent, prompt, c)
		if err != nil {
			tr.Error = err.Error()
			turns = append(turns, tr)
			return judgeCase(c, eng, scope, turns)
		}
		if c.RestoreSession && i == len(c.Turns)-1 {
			tr.RestoredSess = true
		}
		turns = append(turns, tr)
	}
	return judgeCase(c, eng, scope, turns)
}

func runTurn(ctx context.Context, agent *tacklr.AgentHarness, prompt string, c Case) (TurnTrace, error) {
	start := time.Now()
	tr := TurnTrace{Prompt: prompt}
	ch, err := agent.Run(ctx, prompt)
	if err != nil {
		tr.Duration = time.Since(start)
		return tr, err
	}
	var assistant strings.Builder
	toolByCallID := map[string]*ToolCallRecord{}
	for {
		select {
		case <-ctx.Done():
			tr.Duration = time.Since(start)
			tr.Assistant = assistant.String()
			return tr, ctx.Err()
		case ev, ok := <-ch:
			if !ok {
				tr.Duration = time.Since(start)
				tr.Assistant = assistant.String()
				// Flatten tools
				for _, rec := range toolByCallID {
					tr.Tools = append(tr.Tools, *rec)
				}
				return tr, nil
			}
			switch ev.Type {
			case tacklr.StreamEventMessage, tacklr.StreamEventReasoning:
				if ev.Content != "" {
					assistant.WriteString(ev.Content)
				}
			case tacklr.StreamEventFunctionCall:
				for _, tc := range ev.ToolCalls {
					name := tc.Name
					if name == "" {
						continue
					}
					id := tc.CallID
					if id == "" {
						id = tc.ID
					}
					if id == "" {
						id = name + fmt.Sprintf("-%d", len(toolByCallID))
					}
					toolByCallID[id] = &ToolCallRecord{Name: name, Arguments: tc.Arguments}
				}
			case tacklr.StreamEventToolResult:
				// Content often holds tool output; MessageID may be call id.
				if rec, ok := toolByCallID[ev.MessageID]; ok {
					rec.Result = ev.Content
				} else if ev.Content != "" {
					tr.Tools = append(tr.Tools, ToolCallRecord{Name: "?", Result: ev.Content})
				}
			case tacklr.StreamEventInterrupt:
				tr.Interrupts++
				var payload struct {
					InterruptId string          `json:"interruptId"`
					Type        string          `json:"type"`
					Data        json.RawMessage `json:"data"`
				}
				if err := json.Unmarshal(ev.Data, &payload); err != nil {
					tr.Duration = time.Since(start)
					return tr, fmt.Errorf("interrupt payload: %w", err)
				}
				// Drain rest of channel after interrupt park (channel closes).
				drainEvents(ch)
				resumeBody := buildInterruptResume(payload.InterruptId, payload.Type, payload.Data, c)
				ch2, err := agent.ReturnFromInterrupt(ctx, map[string][]byte{
					payload.InterruptId: resumeBody,
				})
				if err != nil {
					tr.Duration = time.Since(start)
					return tr, fmt.Errorf("resume interrupt: %w", err)
				}
				ch = ch2
			case tacklr.StreamEventError:
				if ev.Error != nil {
					tr.Duration = time.Since(start)
					tr.Assistant = assistant.String()
					return tr, ev.Error
				}
				if ev.Content != "" {
					tr.Duration = time.Since(start)
					tr.Assistant = assistant.String()
					return tr, fmt.Errorf("%s", ev.Content)
				}
			}
		}
	}
}

func drainEvents(ch <-chan tacklr.StreamEvent) {
	for ev := range ch {
		_ = ev
	}
}

func buildInterruptResume(interruptID, typ string, data json.RawMessage, c Case) []byte {
	switch typ {
	case "tool_permission":
		return []byte(`{"optionId":"allow-always"}`)
	case "user_selection_choice":
		idx := c.InterruptSelectionIdx
		if title := strings.TrimSpace(c.InterruptChoiceTitle); title != "" {
			var us struct {
				Options []struct {
					Title string `json:"title"`
				} `json:"options"`
			}
			if json.Unmarshal(data, &us) == nil {
				want := strings.ToLower(title)
				for i, o := range us.Options {
					if strings.Contains(strings.ToLower(o.Title), want) {
						idx = i
						break
					}
				}
			}
		}
		b, _ := json.Marshal(map[string]any{
			"interruptId":  interruptID,
			"selectionIdx": idx,
		})
		return b
	default:
		// Best-effort allow
		return []byte(`{"optionId":"allow-once"}`)
	}
}

func failResult(c Case, turns []TurnTrace, msg string) CaseResult {
	return CaseResult{
		ID: c.ID, Suite: c.Suite, Success: false,
		Notes:  []string{msg},
		Scores: map[string]float64{"success": 0},
		Turns:  turns,
	}
}
