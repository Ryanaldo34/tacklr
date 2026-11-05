//go:build js && wasm

// Command cloudflare is an example Worker that runs Tacklr sessions as
// Durable Objects. Build it with worker/build.sh and run it with wrangler;
// see README.md.
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"

	"github.com/ryanaldo34/tacklr"
	"github.com/ryanaldo34/tacklr/examples/cloudflare/api"
	"github.com/ryanaldo34/tacklr/openai"
	"github.com/ryanaldo34/tacklr/session"
	"github.com/ryanaldo34/tacklr/session/durableobject"
	"github.com/ryanaldo34/tacklr/session/durableobject/cloudflare"
)

func main() {
	cloudflare.Serve(cloudflare.App{
		Binding: "SESSIONS",
		Config:  config,
		Front: func(env cloudflare.Env, c session.Client) (http.Handler, error) {
			token := env.String("API_TOKEN")
			if token == "" {
				return nil, errors.New("API_TOKEN is not set")
			}
			return api.Handler(c, token), nil
		},
	})
}

// config is the agent every session runs: a model from the env, and
// tools that show a turn, a park, a specialist and a large result.
func config(env cloudflare.Env) (durableobject.Config, error) {
	key, err := base64.StdEncoding.DecodeString(env.String("SEAL_KEY"))
	if err != nil {
		return durableobject.Config{}, errors.New("SEAL_KEY must be 32 bytes in base64")
	}
	sealer, err := session.NewAESGCM(key)
	if err != nil {
		return durableobject.Config{}, err
	}
	model := openai.New(http.DefaultClient).WithApiKey(env.String("OPENAI_API_KEY")).WithModel(env.String("MODEL"))
	if url := env.String("OPENAI_BASE_URL"); url != "" {
		model = model.WithURL(url)
	}
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
				Name: "researcher", Description: "Researches and deploys.", Instructions: "You research.",
				Model: model, Tools: []*tacklr.Tool{deploy},
			}},
		},
		Sealer: sealer,
	}, nil
}
