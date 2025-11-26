package controlplane

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Aijeyomah/NucleusDB/internals/cluster"
	"gopkg.in/yaml.v3"
)

// Dynamic node info kept in-memory with TTL.
type NodeInfo struct {
	NodeID   string `json:"node_id"`
	ShardID  int    `json:"shard_id"`
	HTTP     string `json:"http"`
	Raft     string `json:"raft"`
	RoleHint string `json:"role_hint"` //leader/follower/""
	Term     uint64 `json:"term"`

	LastSeen time.Time `json:"-"`
}

// Live view = static (cluster.yaml) + healthy dynamic nodes.
type State struct {
	mu             sync.RWMutex
	cluster        cluster.ClusterMap
	dyn            map[int]map[string]*NodeInfo
	ttl            time.Duration
	leaderPerShard map[int]string
	desired        map[int]map[string]DesiredReplica
	reshard        ReshardState
}

type DesiredReplica struct {
	NodeID  string `json:"node_id"`
	ShardID int    `json:"shard_id"`
	HTTP    string `json:"http"`
	Raft    string `json:"raft"`
}

type DesiredMembership struct {
	// represents what the cluster is supposed to look like not what is currently alive.
	ByShard map[int]map[string]DesiredReplica `json:"by_shard"`
}

func (s *State) Desired() DesiredMembership {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := DesiredMembership{ByShard: map[int]map[string]DesiredReplica{}}
	for _, sh := range s.cluster.Shards {
		if out.ByShard[sh.ID] == nil {
			out.ByShard[sh.ID] = map[string]DesiredReplica{}
		}
		if _, ok := out.ByShard[sh.ID]; ok && len(out.ByShard[sh.ID]) == 0 {
			for _, n := range sh.Nodes {
				out.ByShard[sh.ID][n.NodeID] = DesiredReplica{
					NodeID:  n.NodeID,
					ShardID: sh.ID,
					HTTP:    n.HTTP,
					Raft:    n.Raft,
				}
			}
		}
	}
	return out
}

// LoadStatic keeps static map and prepares dynamic overlay.
// ttl controls how long nodes remain healthy without heartbeat.
func LoadStatic(path string, ttl time.Duration) (*State, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cluster map: %w", err)
	}
	var cm cluster.ClusterMap
	if err := yaml.Unmarshal(b, &cm); err != nil {
		return nil, fmt.Errorf("parse cluster map: %w", err)
	}
	return &State{
		cluster:        cm,
		dyn:            make(map[int]map[string]*NodeInfo),
		ttl:            ttl,
		leaderPerShard: make(map[int]string),
		desired:        make(map[int]map[string]DesiredReplica),
	}, nil
}

// in control plane state:
func (s *State) ensureDesiredShard(shardID int) {
	if s.desired[shardID] == nil {
		s.desired[shardID] = make(map[string]DesiredReplica)
	}
	if len(s.desired[shardID]) == 0 {
		if sh := s.cluster.ShardByID(shardID); sh != nil {
			for _, n := range sh.Nodes {
				s.desired[shardID][n.NodeID] = DesiredReplica{
					NodeID: n.NodeID, ShardID: shardID, HTTP: n.HTTP, Raft: n.Raft,
				}
			}
		}
	}
}

func (s *State) SetDesiredAdd(rep DesiredReplica) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if rep.NodeID == "" {
		return errors.New("missing node_id")
	}
	s.ensureDesiredShard(rep.ShardID)
	s.desired[rep.ShardID][rep.NodeID] = rep
	return nil
}

func (s *State) SetDesiredRemove(shardID int, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureDesiredShard(shardID)
	delete(s.desired[shardID], nodeID)
	return nil
}

// returns the current desired map.
func (s *State) DesiredSnapshot() DesiredMembership {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := DesiredMembership{ByShard: map[int]map[string]DesiredReplica{}}
	for _, sh := range s.cluster.Shards {
		s.ensureDesiredShard(sh.ID)
		dst := make(map[string]DesiredReplica, len(s.desired[sh.ID]))
		for k, v := range s.desired[sh.ID] {
			dst[k] = v
		}
		out.ByShard[sh.ID] = dst
	}
	return out
}

// GetClusterMap returns static map (as-is). The API layer will
// compose a "live" response that includes only healthy nodes.
func (s *State) GetClusterMap() cluster.ClusterMap {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cluster
}

// UpsertNode is called by /register and /heartbeat.
func (s *State) UpsertNode(n *NodeInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dyn[n.ShardID] == nil {
		s.dyn[n.ShardID] = make(map[string]*NodeInfo)
	}
	existing, ok := s.dyn[n.ShardID][n.NodeID]
	now := time.Now()

	if ok {
		// Only overwrite when incoming values are non-empty / non-zero.
		if n.HTTP != "" {
			existing.HTTP = n.HTTP
		}
		if n.Raft != "" {
			existing.Raft = n.Raft
		}
		if n.RoleHint != "" {
			existing.RoleHint = n.RoleHint
		}
		if n.Term != 0 {
			existing.Term = n.Term
		}
		existing.LastSeen = now
	} else {
		n.LastSeen = now
		s.dyn[n.ShardID][n.NodeID] = n
	}

	if n.RoleHint == "leader" {
		s.leaderPerShard[n.ShardID] = n.NodeID
	}
}

// Prune removes nodes whose heartbeat expired.
func (s *State) Prune() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for shard, nodes := range s.dyn {
		for id, info := range nodes {
			if now.Sub(info.LastSeen) > s.ttl {
				delete(nodes, id)
				if s.leaderPerShard[shard] == id {
					delete(s.leaderPerShard, shard)
				}
			}
		}
		if len(nodes) == 0 {
			delete(s.dyn, shard)
		}
	}
}

// LiveCluster returns the static map filtered by dynamic health.
// Only nodes that are currently healthy (have recent heartbeat) are included.
// A best-effort "leader_hint" is attached per shard.
type LiveClusterShardNode struct {
	NodeID string `json:"node_id"`
	HTTP   string `json:"http"`
	Raft   string `json:"raft"`
}

type LiveClusterShard struct {
	ID         int                    `json:"id"`
	Nodes      []LiveClusterShardNode `json:"nodes"`
	LeaderHint string                 `json:"leader_hint,omitempty"`
}

type LiveCluster struct {
	ReplicationFactor int                `json:"replication_factor"`
	Shards            []LiveClusterShard `json:"shards"`
}

func (s *State) LiveCluster() LiveCluster {
	s.mu.RLock()
	defer s.mu.RUnlock()

	lc := LiveCluster{
		ReplicationFactor: s.cluster.ReplicationFactor,
	}

	for _, sh := range s.cluster.Shards {
		lsh := LiveClusterShard{ID: sh.ID}

		// Include every node that has sent a heartbeat (dynamic view).
		seen := make(map[string]struct{})
		if healthy := s.dyn[sh.ID]; healthy != nil {
			for _, info := range healthy {
				lsh.Nodes = append(lsh.Nodes, LiveClusterShardNode{
					NodeID: info.NodeID,
					HTTP:   info.HTTP,
					Raft:   info.Raft,
				})
				seen[info.NodeID] = struct{}{}
			}
		}

		// this is to optionally display static nodes that have no heartbeat. might comment later since this can show non-helathy statics
		for _, sn := range sh.Nodes {
			if _, ok := seen[sn.NodeID]; ok {
				continue
			}

		}

		if leader, ok := s.leaderPerShard[sh.ID]; ok {
			lsh.LeaderHint = leader
		}

		lc.Shards = append(lc.Shards, lsh)
	}

	return lc
}

// This will swap the current cluster layout with the plan's new layout
func (s *State) ApplyReshardCutover() {
	s.mu.Lock()
	defer s.mu.Unlock()

	active, plan, _ := s.reshard.Snapshot()
	if !active || plan == nil {
		return
	}
	// Replace static layout with new one
	s.cluster = plan.New
	// Reset leader hints they will repopulate via heartbeats from nodes.
	s.leaderPerShard = make(map[int]string)
	// Clear dynamic table for unknown nodes, they'll re-register again.
	s.dyn = make(map[int]map[string]*NodeInfo)
	s.reshard.Clear()
}
