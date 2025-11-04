package httpserver

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/Aijeyomah/NucleusDB/internals/store"
)

type Options struct {
	NodeId          string
	ShardId         int
	IsLeader        bool
	LeaderAdvertise string
	KV              *store.InMemory
	MaxByteValue    int
	MaxByteKey      int
}

type Server struct {
	opt Options
	mu  *http.ServeMux
}

func New(opt Options) *Server {
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
	WriteJson(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleWhoami(w http.ResponseWriter, _ *http.Request) {
	role := "follower"
	if s.opt.IsLeader {
		role = "leader"
	}
	WriteJson(w, http.StatusOK, map[string]any{
		"node_id":   s.opt.NodeId,
		"shard_id":  s.opt.ShardId,
		"role":      role,
		"leader_at": s.opt.LeaderAdvertise,
	},
	)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	// To ensure strong consistency, only the leader will accept writes
	if !s.opt.IsLeader {
		WriteJson(w, http.StatusConflict, map[string]any{
			"error":        "not_leader",
			"leader_addr":  s.opt.LeaderAdvertise,
			"shard_id":     s.opt.ShardId,
			"redirectable": s.opt.LeaderAdvertise != "",
		})
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
	s.opt.KV.Put(key, val)

	WriteJson(w, http.StatusOK, map[string]any{
		"ok":    true,
		"key":   key,
		"value": val,
	})
}

func (s *Server) handleGet(w http.ResponseWriter, _ *http.Request, key string) {
	val, ok := s.opt.KV.Get(key)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	WriteJson(w, http.StatusOK, map[string]any{
		"key":   key,
		"value": val,
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, _ *http.Request, key string) {
	s.opt.KV.Delete(key) //Todo:  check if we need to confirm the key exist. nd if this Delete is compartable with the code or maybe i need to implemnt manualy
	WriteJson(w, http.StatusOK, map[string]any{
		"ok": true,
	})
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")

	if err := s.validateKey(key); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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

func WriteJson(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Printf("[http] write json error: %v", err)
	}
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
