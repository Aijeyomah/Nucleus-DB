package controlplane

import (
	"encoding/json"
	"log"
	"net/http"
)

type Server struct{
	state *State
	mux *http.ServeMux
}

func NewServer(s *State) *Server{
	srv := &Server{
		state: s,
		mux: http.NewServeMux(),
	}
	srv.routes()
	return srv
}

func(s *Server) routes(){
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/cluster/map", s.handleClusterMap)
}


func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request){
    writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleClusterMap(w http.ResponseWriter, _ *http.Request){
	cm := s.state.GetClusterMap()
	writeJSON(w, http.StatusOK, cm)

}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request){
	s.mux.ServeHTTP(w, r)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[cp] write json: %v", err)
	}
}
