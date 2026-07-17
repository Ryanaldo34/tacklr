package server

import (
	"net/http"

	"github.com/ryanaldo34/tacklr/stores"
)

type Server struct {
	store    stores.BaseStore
	provider AgentProvider
}

func New(store stores.BaseStore, provider AgentProvider) *Server {
	return &Server{
		store:    store,
		provider: provider,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /", s.handlePrompt)
	mux.HandleFunc("GET /", s.handleWebSocketPrompt)
	mux.HandleFunc("POST /resume", s.handleResume)
	mux.HandleFunc("GET /resume", s.handleWebSocketResume)
	return mux
}
