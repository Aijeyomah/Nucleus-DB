package httpserver

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Aijeyomah/NucleusDB/internals/store"
)

type Proposer interface {
	IsLeader() bool
	LeaderRaftAddr() string
	ProposePut(op store.PutOp, timeout time.Duration) error
}

type Options struct {
	NodeId         string
	ShardId        int
	KV             *store.InMemory
	Raft           Proposer
	MaxByteValue   int
	MaxByteKey     int
	ProposeTimeout time.Duration
	// Advertised HTTP address (this node) to help clients (optional)
	AdvertiseHTTP string
	// Optional: map from raftAddr -> httpBase (for better redirect hints)
	PeerHTTP map[string]string
}

type Server struct {
	opt Options
	mu  *http.ServeMux
}

func New(opt Options) *Server {
	if opt.ProposeTimeout <= 0 {
		opt.ProposeTimeout = 5 * time.Second
	}
	if opt.MaxByteKey <= 0 {
		opt.MaxByteKey = 4096
	}
	if opt.MaxByteValue <= 0 {
		opt.MaxByteValue = 1024
	}
	server := &Server{
		mu:  http.NewServeMux(),
		opt: opt,
	}
	server.routes()
	return server

}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.ServeHTTP(w, r)
}

func (s *Server) routes() { // Todo: see how to warp each handler to get the latency
	s.mu.HandleFunc("/health", s.handleHeath)
	s.mu.HandleFunc("/whoami", s.handleWhoami)
	s.mu.HandleFunc("/kv/", s.handleKV)
}

func (s *Server) handleHeath(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleWhoami(w http.ResponseWriter, _ *http.Request) {
	role := "follower"
	if s.opt.Raft != nil && s.opt.Raft.IsLeader() {
		role = "leader"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":        s.opt.NodeId,
		"shard_id":       s.opt.ShardId,
		"role":           role,
		"advertise_http": s.opt.AdvertiseHTTP,
	},
	)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	// To ensure strong consistency, only the leader will accept writes
	if s.opt.Raft == nil || !s.opt.Raft.IsLeader() {
		s.notLeader(w)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, int64(s.opt.MaxByteValue))
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	val := string(body)

	clientId := "client"
	requestId := fmt.Sprintf("%d", time.Now().UnixNano())

	op := store.NewPutOp(key, val, clientId, requestId)

	if err := s.opt.Raft.ProposePut(op, 5*time.Second); err != nil {
		http.Error(w, "raft apply failed: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":    true,
		"key":   key,
		"bytes": len(body),
	})
}

func (s *Server) handleGet(w http.ResponseWriter, _ *http.Request, key string) {
	// Strict: leader-only reads for linearizability
	if s.opt.Raft == nil || !s.opt.Raft.IsLeader() {
		s.notLeader(w)
		return
	}
	val, ok := s.opt.KV.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"key":   key,
		"value": val,
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, _ *http.Request, key string) {
	s.opt.KV.Delete(key) //Todo:  check if we need to confirm the key exist. nd if this Delete is compartable with the code or maybe i need to implemnt manualy
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true,
	})
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")

	if err := s.validateKey(key); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed) // TODO: handle CAS
	}
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("[http] write json error: %v", err)
	}
}

func (s *Server) notLeader(w http.ResponseWriter) {
	leaderRaft := ""
	leaderHttp := ""

	if s.opt.Raft != nil {
		leaderRaft = s.opt.Raft.LeaderRaftAddr()
		if s.opt.PeerHTTP != nil {
			if val, ok := s.opt.PeerHTTP[leaderRaft]; ok {
				leaderHttp = val
			}
		}
	}
	writeJSON(w, http.StatusConflict, map[string]any{
		"error":       "not_leader",
		"leader_raft": leaderRaft,
		"leader_http": leaderHttp,
		"shard_id":    s.opt.ShardId,
	})

}

func (s *Server) validateKey(key string) error {
	keyByteSize := len(key)
	if key == "" {
		return fmt.Errorf("missing key")
	}
	if keyByteSize > s.opt.MaxByteKey {
		return fmt.Errorf("key too large: limit=%d got=%d", s.opt.MaxByteKey, keyByteSize)
	}
	// safely validate path to slash "/""  forbid slashes inside key to keep routing simple
	if strings.Contains(key, "/") {
		return fmt.Errorf("invalid key: must not contain '/'")
	}
	return nil
}
