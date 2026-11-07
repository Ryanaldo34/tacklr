//go:build js && wasm

// Command cloudflare is an example Worker that runs Tacklr sessions as
// Durable Objects, and serves ACP over WebSocket from a connection object
// for each socket. Build it with worker/build.sh and run it with wrangler;
// see README.md.
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/anthropic"
	"github.com/ryanaldo34/tacklr/examples/host"
	"github.com/ryanaldo34/tacklr/openai"
	"github.com/ryanaldo34/tacklr/server"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
	"github.com/ryanaldo34/tacklr/session/durableobject/cloudflare"
)

func main() {
	cloudflare.Serve(cloudflare.App{
		Sessions:    "SESSIONS",
		Connections: "CONNECTIONS",
		Config:      config,
		// The same ACP server as the Temporal example's host. Each
		// session's object keeps its ACP wire state.
		Server: func(env cloudflare.Env, c *durableobject.Client) (*server.Server, error) {
			token := env.String("API_TOKEN")
			if token == "" {
				return nil, errors.New("API_TOKEN is not set")
			}
			return host.New(c, newModel(env), c.WireStore(), token), nil
		},
	})
}

// newModel is the model the env names: a Claude model with
// ANTHROPIC_API_KEY, else an OpenAI-compatible one with OPENAI_API_KEY.
func newModel(env cloudflare.Env) tacklr.Model {
	if key := env.String("ANTHROPIC_API_KEY"); key != "" {
		model := anthropic.New(http.DefaultClient).WithApiKey(key).WithModel(env.String("MODEL"))
		if url := env.String("ANTHROPIC_BASE_URL"); url != "" {
			model = model.WithURL(url)
		}
		return model
	}
	model := openai.New(http.DefaultClient).WithApiKey(env.String("OPENAI_API_KEY")).WithModel(env.String("MODEL"))
	if url := env.String("OPENAI_BASE_URL"); url != "" {
		model = model.WithURL(url)
	}
	return model
}

// config is the agent every session runs: a model from the env, and
// tools that show a turn, a park, a specialist in its own object and a
// large result.
func config(env cloudflare.Env) (durableobject.Config, error) {
	key, err := base64.StdEncoding.DecodeString(env.String("SEAL_KEY"))
	if err != nil {
		return durableobject.Config{}, errors.New("SEAL_KEY must be 32 bytes in base64")
	}
	sealer, err := session.NewAESGCM(key)
	if err != nil {
		return durableobject.Config{}, err
	}
	model := newModel(env)
	lookup := tacklr.NewTool(tacklr.ToolConfig{Name: "lookup", Description: "Looks up the answer.",
		Handler: func(context.Context) (string, error) { return "42", nil }})
	report := tacklr.NewTool(tacklr.ToolConfig{Name: "report", Description: "Writes the full report.",
		Handler: func(context.Context) (string, error) { return strings.Repeat("r", 3<<20), nil }})
	deploy := tacklr.NewTool(tacklr.ToolConfig{Name: "deploy", Description: "Deploys the change.",
		OnCall:  []tacklr.OnCallFunc{tacklr.ToolPermissionOnCall},
		Handler: func(context.Context) (string, error) { return "deployed", nil }})
	return durableobject.Config{
		Agent: tacklr.AgentOptions{
			Name:          "edge",
			SystemPrompt:  "You help with releases.",
			Model:         model,
			MaxWindowSize: 200_000,
			Tools:         []*tacklr.Tool{lookup, report, deploy},
			Specialists: []*tacklr.Specialist{{
				Name: "researcher", Description: "Researches the answer.", Instructions: "You research.",
				Model: model, Tools: []*tacklr.Tool{lookup},
			}},
		},
		Sealer: sealer,
	}, nil
}
