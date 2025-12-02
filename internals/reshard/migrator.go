package reshard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Aijeyomah/NucleusDB/internals/cluster"
	"github.com/Aijeyomah/NucleusDB/internals/hashring"
)

var BATCH_SIZE = 200

type KV interface {
	Snapshot() map[string]string
	Delete(key string)
}

// Raft role used to gate leader-only migration
type RoleReader interface {
	IsLeader() bool
	LeaderRaftAddr() string
}

// LiveCluster minimal we need from control-plane
type LiveShardNode struct {
	NodeID string `json:"node_id"`
	HTTP   string `json:"http"`
}

type LiveShard struct {
	ID         int             `json:"id"`
	Nodes      []LiveShardNode `json:"nodes"`
	LeaderHint string          `json:"leader_hint"`
}

type LiveCluster struct {
	Shards []LiveShard `json:"shards"`
}

type Migrator struct {
	SourceShardID int
	KV            KV
	RaftRole      RoleReader
	ControlBase   string // http://localhost:9000
	HTTP          *http.Client
	// for batch controls
	BatchSize int
	Sleep     time.Duration
}

func NewMigrator(sourceShardID int, kv KV, role RoleReader, controlPlane string) *Migrator {
	return &Migrator{
		SourceShardID: sourceShardID,
		KV:            kv,
		RaftRole:      role,
		ControlBase:   controlPlane,
		BatchSize:     BATCH_SIZE,
		Sleep:         250 * time.Millisecond,
		HTTP:          &http.Client{Timeout: 10 * time.Second},
	}
}

func (m *Migrator) fetchPlan() (active bool, newMap *cluster.ClusterMap, epoch int64, err error) {
	req, _ := http.NewRequest(http.MethodGet, m.ControlBase+"/reshard/plan", nil)
	resp, err := m.HTTP.Do(req)
	if err != nil {
		return false, nil, 0, err
	}
	defer resp.Body.Close()
	var out struct {
		Active bool `json:"active"`
		Plan   *struct {
			New   cluster.ClusterMap `json:"new_cluster_map"`
			Epoch int64              `json:"epoch"`
		} `json:"plan"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, nil, 0, err
	}

	if !out.Active || out.Plan == nil {
		return false, nil, 0, nil
	}
	return true, &out.Plan.New, out.Plan.Epoch, nil
}

func (m *Migrator) fetchCurrentMap() (*cluster.ClusterMap, error) {
	req, _ := http.NewRequest(http.MethodGet, m.ControlBase+"/cluster/map", nil)
	resp, err := m.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var cm cluster.ClusterMap
	if err := json.NewDecoder(resp.Body).Decode(&cm); err != nil {
		return nil, err
	}
	return &cm, nil
}

func (m *Migrator) fetchLive() (LiveCluster, error) {
	req, _ := http.NewRequest(http.MethodGet, m.ControlBase+"/cluster/live", nil)
	resp, err := m.HTTP.Do(req)
	if err != nil {
		return LiveCluster{}, err
	}
	defer resp.Body.Close()
	var lc LiveCluster
	if err := json.NewDecoder(resp.Body).Decode(&lc); err != nil {
		return LiveCluster{}, err
	}
	return lc, nil
}

// blocking pass: scan snapshot and migrate keys owned by SourceShardID in old ring
// that move to a different shard in new ring.
func (m *Migrator) migrateOnce() (moved, total int64, err error) {
	curr, err := m.fetchCurrentMap()
	if err != nil {
		return 0, 0, fmt.Errorf("fetch current: %w", err)
	}
	active, newcm, _, err := m.fetchPlan()
	if err != nil || !active || newcm == nil {
		return 0, 0, fmt.Errorf("no active plan")
	}
	oldRing := hashring.New(curr.ShardIDs(), hashring.DefaultReplicas)
	newRing := hashring.New(newcm.ShardIDs(), hashring.DefaultReplicas)

	snap := m.KV.Snapshot()
	log.Printf("[migrator] shard=%d snapshot_keys=%d", m.SourceShardID, len(snap))
	total = int64(len(snap))
	if total == 0 {
		return 0, 0, nil
	}
	type kv struct{ Key, Value string }
	buckets := map[int][]kv{} // targetShard -> kvs

	for k, v := range snap {
		oldShard, _ := oldRing.GetShard(k)
		if oldShard != m.SourceShardID {
			log.Printf("[migrator] skipping key=%s oldShard=%d source=%d", k, oldShard, m.SourceShardID)
			continue
		}
		newShard, _ := newRing.GetShard(k)
		if newShard == oldShard {
			log.Printf("[migrator] skipping key=%s new shard matches old=%d", k, oldShard)
			continue
		}
		if oldShard == m.SourceShardID && newShard != oldShard {
			log.Printf("[migrator] key=%s old=%d new=%d bucketed", k, oldShard, newShard)
		}
		buckets[newShard] = append(buckets[newShard], kv{Key: k, Value: v})
	}
	if len(buckets) == 0 {
		return 0, total, nil
	}
	// fetch live cluster to resolve leader HTTP for each target shard
	live, err := m.fetchLive()
	if err != nil {
		return 0, total, fmt.Errorf("fetch live: %w", err)
	}
	leaderHTTP := map[int]string{} // shard -> leader http base
	for _, sh := range live.Shards {
		if sh.LeaderHint == "" {
			continue
		}
		var httpBase string
		for _, n := range sh.Nodes {
			if n.NodeID == sh.LeaderHint {
				httpBase = n.HTTP
				break
			}
		}
		if httpBase != "" {
			leaderHTTP[sh.ID] = strings.TrimRight(httpBase, "/")
		}
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for shardID, kvs := range buckets {
		dest := leaderHTTP[shardID]
		if dest == "" {
			log.Printf("[migrator] shard=%d missing dest, skip %d keys", shardID, len(kvs))
			continue
		}
		// send in batches
		for i := 0; i < len(kvs); i += m.BatchSize {
			end := i + m.BatchSize
			if end > len(kvs) {
				end = len(kvs)
			}
			body, _ := json.Marshal(kvs[i:end])
			req, _ := http.NewRequest(http.MethodPost, dest+"/migrate/ingest", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				log.Printf("[migrator] POST %s failed: %v", dest, err)
				continue
			}

			if resp.StatusCode/100 != 2 {
				body, _ := io.ReadAll(resp.Body)
				log.Printf("[migrator] POST %s status=%d body=%s", dest, resp.StatusCode, body)
			}
			resp.Body.Close()

			if resp.StatusCode/100 == 2 {
				batch := kvs[i:end]
				moved += int64(len(batch))
				for _, item := range batch {
					m.KV.Delete(item.Key)
				}
			}
			time.Sleep(m.Sleep)
		}
	}
	return moved, total, nil
}

func (m *Migrator) Run(ctx context.Context, report func(moved, total int64)) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if !m.RaftRole.IsLeader() {
				log.Printf("[migrator] shard=%d skipping tick; not leader", m.SourceShardID)

				continue
			}
			log.Printf("[migrator] shard=%d running migrateOnce", m.SourceShardID)
			moved, total, err := m.migrateOnce()
			if err == nil && total > 0 {
				report(moved, total)
			}
		}
	}
}
