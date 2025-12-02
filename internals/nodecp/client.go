package nodecp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	base string
	hc   *http.Client
}

type DesiredReplica struct {
	NodeID  string `json:"node_id"`
	ShardID int    `json:"shard_id"`
	HTTP    string `json:"http"`
	Raft    string `json:"raft"`
}

func New(base string) *Client {
	return &Client{
		base: base,
		hc:   &http.Client{Timeout: 3 * time.Second},
	}
}

type DesiredMembership struct {
	ByShard map[int]map[string]DesiredReplica `json:"by_shard"`
	Retired map[int]bool                      `json:"retired_shards"`
}

func (r *Client) Register(nodeID string, shardID int, httpAddr, raftAddr, role string, term uint64) error {
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

func (r *Client) Heartbeat(nodeID string, shardID int, role string, term uint64, raft, httpAddr string) error {
	body := map[string]any{
		"node_id":   nodeID,
		"shard_id":  shardID,
		"role_hint": role,
		"term":      term,
		"raft":      raft,
		"http":      httpAddr,
	}
	return r.post("/heartbeat", body)
}

func (r *Client) post(path string, body map[string]any) error {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, r.base+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("cp %s status %d", path, resp.StatusCode)
	}
	return nil
}

func (c *Client) Desired() (DesiredMembership, error) {
	var out DesiredMembership
	req, _ := http.NewRequest(http.MethodGet, c.base+"/membership/desired", nil)
	resp, err := c.hc.Do(req)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("desired: http %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, err
	}
	return out, nil
}

func (c *Client) DesiredAdd(rep DesiredReplica) error {
	b, _ := json.Marshal(rep)
	resp, err := c.hc.Post(c.base+"/membership/add", "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("desired add: http %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) DesiredRemove(shardID int, nodeID string) error {
	body := map[string]any{"shard_id": shardID, "node_id": nodeID}
	b, _ := json.Marshal(body)
	resp, err := c.hc.Post(c.base+"/membership/remove", "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("desired remove: http %d", resp.StatusCode)
	}
	return nil
}
