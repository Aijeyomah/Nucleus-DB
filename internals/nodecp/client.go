package nodecp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Registrar struct {
	BaseURL string
	Client  *http.Client
}

func New(base string) *Registrar {
	return &Registrar{
		BaseURL: base,
		Client:  &http.Client{Timeout: 3 * time.Second},
	}
}

func (r *Registrar) Register(nodeID string, shardID int, httpAddr, raftAddr, role string, term uint64) error {
	body := map[string]any{
		"node_id":   nodeID,
		"shard_id":  shardID,
		"http":      httpAddr,
		"raft":      raftAddr,
		"role_hint": role,
		"term":      term,
	}
	return r.post("/register", body)
}

func (r *Registrar) Heartbeat(nodeID string, shardID int, role string, term uint64) error {
	body := map[string]any{
		"node_id":   nodeID,
		"shard_id":  shardID,
		"role_hint": role,
		"term":      term,
	}
	return r.post("/heartbeat", body)
}

func (r *Registrar) post(path string, body map[string]any) error {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, r.BaseURL+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("cp %s status %d", path, resp.StatusCode)
	}
	return nil
}
