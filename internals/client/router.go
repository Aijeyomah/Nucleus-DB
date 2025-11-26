package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/Aijeyomah/NucleusDB/internals/cluster"
	"github.com/Aijeyomah/NucleusDB/internals/hashring"
)

type Router struct {
	httpClient *http.Client
	ring       *hashring.Ring
	cluster    cluster.ClusterMap
	mu         sync.RWMutex
	leaderHTTP map[int]string
}

type NotLeaderError struct {
	ShardID    int
	LeaderHTTP string
	Msg        string
}

func (e *NotLeaderError) Error() string {
	return e.Msg
}

func NewRouter(cm cluster.ClusterMap) *Router {
	return &Router{
		httpClient: &http.Client{Timeout: 3 * time.Second},
		ring:       hashring.New(cm.ShardIDs(), hashring.DefaultReplicas),
		cluster:    cm,
		leaderHTTP: make(map[int]string),
	}
}

func (r *Router) shardForKey(key string) (int, error) {
	id, ok := r.ring.GetShard(key)
	if !ok {
		return 0, fmt.Errorf("no shards configured")
	}
	return id, nil
}

// Returns cached leader if known, else first replica as fallback.
func (r *Router) pickNodeHTTP(shardID int) (string, error) {
	r.mu.RLock()
	if h, ok := r.leaderHTTP[shardID]; ok && h != "" {
		r.mu.RUnlock()
		return h, nil
	}
	r.mu.RUnlock()

	sh := r.cluster.ShardByID(shardID)
	if sh == nil || len(sh.Nodes) == 0 {
		return "", fmt.Errorf("no replicas for shard %d", shardID)
	}
	// fallback: first replica http addr
	return sh.Nodes[0].HTTP, nil
}

func (r *Router) updateLeader(shardID int, leaderHTTP string) {
	if leaderHTTP == "" {
		return
	}
	r.mu.Lock()
	r.leaderHTTP[shardID] = leaderHTTP
	r.mu.Unlock()
}

// Put does: key -> shard -> leader, follows one not_leader redirect.
func (r *Router) Put(ctx context.Context, key, value string) (string, error) {
	shardID, err := r.shardForKey(key)
	if err != nil {
		return "", err
	}
	body := []byte(value)

	for attempt := 0; attempt < 2; attempt++ {
		base, err := r.pickNodeHTTP(shardID)
		if err != nil {
			return "", err
		}
		url := fmt.Sprintf("%s/kv/%s", base, key)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
		if err != nil {
			return "", err
		}
		resp, err := r.httpClient.Do(req)
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return "", nil
		}

		if resp.StatusCode == http.StatusConflict {
			var payload struct {
				Error      string `json:"error"`
				LeaderHTTP string `json:"leader_http"`
				ShardID    int    `json:"shard_id"`
			}
			_ = json.NewDecoder(resp.Body).Decode(&payload)
			if payload.Error == "not_leader" && payload.LeaderHTTP != "" {
				r.updateLeader(shardID, payload.LeaderHTTP)
				continue
			}
		}
		data, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("put failed: status=%d body=%s", resp.StatusCode, string(data))
	}
	return "", &NotLeaderError{
		ShardID:    shardID,
		LeaderHTTP: r.leaderHTTP[shardID],
		Msg:        "get failed after redirect attempts",
	}

}

func (r *Router) Get(ctx context.Context, key string) (string, error) {
	ShardId, err := r.shardForKey(key)
	if err != nil {
		return "", err
	}

	for attempts := 0; attempts < 2; attempts++ {
		base, err := r.pickNodeHTTP(ShardId)
		if err != nil {
			return "", err
		}
		url := fmt.Sprintf("%s/kv/%s", base, key)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return "", err
		}

		resp, err := r.httpClient.Do(req)
		if err != nil {
			return "", err
		}
		if resp.StatusCode == http.StatusOK {
			var payload struct {
				Value string `json:"value"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
				return "", err
			}
			return payload.Value, nil
		}

		if resp.StatusCode == http.StatusNotFound {
			return "", fmt.Errorf("not found")
		}

		if resp.StatusCode == http.StatusConflict {
			var payload struct {
				Error      string `json:"error"`
				LeaderHTTP string `json:"leader_http"`
				ShardID    int    `json:"shard_id"`
			}

			_ = json.NewDecoder(resp.Body).Decode(&payload)
			if payload.Error == "not_leader" && payload.LeaderHTTP != "" {
				r.updateLeader(ShardId, payload.LeaderHTTP)
				continue
			}
		}
		data, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("get failed: status=%d body=%s", resp.StatusCode, string(data))
	}
	return "", &NotLeaderError{
		ShardID:    ShardId,
		LeaderHTTP: r.leaderHTTP[ShardId],
		Msg:        "get failed after redirect attempts",
	}
}
