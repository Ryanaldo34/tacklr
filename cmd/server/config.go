package main

import (
	"os"
	"strconv"
)

type serverConfig struct {
	port                   int
	supabaseDBURL          string
	openAIBaseURL          string
	openAIModel            string
	openAIAPIKey           string
	openAIResponseStrategy string
	agentMaxWindowSize     int
	agentStreamingStrategy string
	logLevel               string
	googleOAuthClientID    string
	googleOAuthClientSecret string
	googleOAuthRedirectURL string
}

func loadServerConfig() serverConfig {
	return serverConfig{
		port:                    envInt("PORT", 6969),
		supabaseDBURL:           env("SUPABASE_DB_URL", ""),
		openAIBaseURL:           env("OPENAI_BASE_URL", "http://localhost:8080/v1"),
		openAIModel:             env("OPENAI_MODEL", "gemma-4-26B-A4B-it-UD-Q4_K_M.gguf"),
		openAIAPIKey:            env("OPENAI_API_KEY", "not-needed"),
		openAIResponseStrategy:  env("OPENAI_RESPONSE_STRATEGY", "standard"),
		agentMaxWindowSize:      envInt("AGENT_MAX_WINDOW_SIZE", 8192),
		agentStreamingStrategy:  env("AGENT_STREAMING_STRATEGY", "buffered"),
		logLevel:                env("LOG_LEVEL", "info"),
		googleOAuthClientID:     env("GOOGLE_OAUTH_CLIENT_ID", ""),
		googleOAuthClientSecret: env("GOOGLE_OAUTH_CLIENT_SECRET", ""),
		googleOAuthRedirectURL:  env("GOOGLE_OAUTH_REDIRECT_URL", ""),
	}
}

func env(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envInt(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return n
}
