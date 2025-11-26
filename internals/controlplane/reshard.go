package controlplane

import (
	"errors"
	"sync"
	"time"

	"github.com/Aijeyomah/NucleusDB/internals/cluster"
)

// describes the desired new cluster layout to move to. it will be a full replacement of the cluster map.
type ReshardPlan struct {
	New    cluster.ClusterMap `json:"new_cluster_map"`
	Epoch  int64              `json:"epoch"`
	Reason string             `json:"reason,omitempty"`
}

// Per shard migration progress to be reported by nodes
type ShardProgress struct {
	SourceShardID int   `json:"source_shard_id"`
	Moved         int64 `json:"moved"`
	Total         int64 `json:"total"`
	LastUpdateMS  int64 `json:"last_update_ms"`
}

type ReshardState struct {
	mu       sync.RWMutex
	active   bool
	plan     *ReshardPlan
	progress map[int]ShardProgress // progress per shardID
}

func (rs *ReshardState) GetPlan() (*ReshardPlan, bool) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	if !rs.active || rs.plan == nil {
		return nil, false
	}
	return rs.plan, true
}

func (rs *ReshardState) SetPlan(p *ReshardPlan) error {
	if p == nil {
		return errors.New("no plan provided")
	}
	if len(p.New.Shards) == 0 {
		return errors.New("plan has no shards")
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.active = true
	rs.plan = p
	rs.progress = make(map[int]ShardProgress)
	return nil
}

// Deactivate plan
func (rs *ReshardState) Clear() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.active = false
	rs.plan = nil
	rs.progress = nil
}

func (rs *ReshardState) Report(prog ShardProgress) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if !rs.active {
		return
	}
	prog.LastUpdateMS = time.Now().UnixMilli()
	rs.progress[prog.SourceShardID] = prog
}

func (rs *ReshardState) Snapshot() (active bool, plan *ReshardPlan, progress map[int]ShardProgress) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	if !rs.active || rs.plan == nil {
		return false, nil, nil
	}
	cp := make(map[int]ShardProgress, len(rs.progress))

	for k, v := range rs.progress {
		cp[k] = v
	}
	return true, rs.plan, cp
}
