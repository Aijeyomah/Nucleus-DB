package controlplane

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

type Server struct {
	state *State
	mux   *http.ServeMux
}

type desiredAddReq struct {
	NodeID  string `json:"node_id"`
	ShardID int    `json:"shard_id"`
	HTTP    string `json:"http"`
	Raft    string `json:"raft"`
}
type registerReq struct {
	NodeID   string `json:"node_id"`
	ShardID  int    `json:"shard_id"`
	HTTP     string `json:"http"`
	Raft     string `json:"raft"`
	RoleHint string `json:"role_hint"`
	Term     uint64 `json:"term"`
}

func NewServer(s *State) *Server {
	srv := &Server{
		state: s,
		mux:   http.NewServeMux(),
	}
	srv.routes()
	return srv
}

type desiredRemoveReq struct {
	NodeID  string `json:"node_id"`
	ShardID int    `json:"shard_id"`
}

func (s *Server) routes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/cluster/map", s.handleClusterMap)
	s.mux.HandleFunc("/cluster/live", s.handleClusterLive)
	s.mux.HandleFunc("/register", s.handleRegister)
	s.mux.HandleFunc("/heartbeat", s.handleHeartbeat)
	s.mux.HandleFunc("/membership/desired", s.handleDesiredGet)
	s.mux.HandleFunc("/membership/add", s.handleDesiredAdd)
	s.mux.HandleFunc("/membership/remove", s.handleDesiredRemove)

}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleClusterMap(w http.ResponseWriter, _ *http.Request) {
	cm := s.state.GetClusterMap()
	writeJSON(w, http.StatusOK, cm)

}

func (s *Server) handleClusterLive(w http.ResponseWriter, _ *http.Request) {
	lc := s.state.LiveCluster()
	writeJSON(w, http.StatusOK, lc)
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req registerReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	s.state.UpsertNode(&NodeInfo{
		NodeID:   req.NodeID,
		ShardID:  req.ShardID,
		HTTP:     req.HTTP,
		Raft:     req.Raft,
		RoleHint: req.RoleHint,
		Term:     req.Term,
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "ts": time.Now().UnixMilli()})
}

type heartbeatReq struct {
	NodeID   string `json:"node_id"`
	ShardID  int    `json:"shard_id"`
	RoleHint string `json:"role_hint"`
	Term     uint64 `json:"term"`
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req heartbeatReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	s.state.UpsertNode(&NodeInfo{
		NodeID:   req.NodeID,
		ShardID:  req.ShardID,
		RoleHint: req.RoleHint,
		Term:     req.Term,
	})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDesiredGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, s.state.DesiredSnapshot())
}

func (s *Server) handleDesiredAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req desiredAddReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if req.NodeID == "" || req.ShardID < 0 || req.Raft == "" || req.HTTP == "" {
		http.Error(w, "missing fields", http.StatusBadRequest)
		return
	}
	if err := s.state.SetDesiredAdd(DesiredReplica{
		NodeID: req.NodeID, ShardID: req.ShardID, HTTP: req.HTTP, Raft: req.Raft,
	}); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDesiredRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req desiredRemoveReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if req.NodeID == "" || req.ShardID < 0 {
		http.Error(w, "missing fields", http.StatusBadRequest)
		return
	}
	if err := s.state.SetDesiredRemove(req.ShardID, req.NodeID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[cp] write json: %v", err)
	}
}
