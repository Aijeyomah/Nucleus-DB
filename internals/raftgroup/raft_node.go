package raftgroup

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"os"
	"sort"
	"time"

	"github.com/Aijeyomah/NucleusDB/internals/store"
	"github.com/hashicorp/raft"
)

// holds Raft settings for per replica in a shard
type Config struct {
	NodeID         string
	RaftAddr       raft.ServerAddress
	DataDir        string
	PeerAddress    []raft.ServerAddress // all peers in this shard, including self
	SnapshotRetain int
}

type Node struct {
	raft      *raft.Raft
	transport *raft.NetworkTransport
	fsm       *FSM
}

func NewNode(cfg *Config, fsm *FSM) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir raft data: %w", err)
	}

	addr, err := net.ResolveTCPAddr("tcp", string(cfg.RaftAddr))
	if err != nil {
		return nil, fmt.Errorf("resolve raft addr: %w", err)
	}
	trans, err := raft.NewTCPTransport(addr.String(), addr, 5, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	stores, err := OpenStores(cfg.DataDir, max(1, cfg.SnapshotRetain))
	if err != nil {
		_ = trans.Close()
		return nil, fmt.Errorf("open stores: %w", err)
	}

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(cfg.RaftAddr) // must match what we use in Servers[]
	rc.SnapshotInterval = 30 * time.Second
	rc.SnapshotThreshold = 8192

	node := &Node{
		transport: trans,
		fsm:       fsm,
	}

	// Small log cache in front of log storage
	logCache, err := raft.NewLogCache(512, stores.LogStore)
	if err != nil {
		_ = trans.Close()
		return nil, fmt.Errorf("log cache: %w", err)
	}

	r, err := raft.NewRaft(rc, fsm, logCache, stores.StableStore, stores.SnapshotStore, trans)
	if err != nil {
		_ = trans.Close()
		return nil, fmt.Errorf("new raft: %w", err)
	}
	node.raft = r

	// Detect if this node already has any Raft state
	hasState, err := raft.HasExistingState(stores.LogStore, stores.StableStore, stores.SnapshotStore)
	if err != nil {
		return nil, fmt.Errorf("check existing raft state: %w", err)
	}

	// Only the lexicographically-first address bootstraps the cluster once, when empty.
	if !hasState && isLexicographicallyFirst(cfg.RaftAddr, cfg.PeerAddress) {
		servers := make([]raft.Server, 0, len(cfg.PeerAddress))
		for _, pa := range cfg.PeerAddress {
			servers = append(servers, raft.Server{
				ID:      raft.ServerID(pa),
				Address: pa,
			})
		}
		f := r.BootstrapCluster(raft.Configuration{Servers: servers})
		if f.Error() != nil && f.Error() != raft.ErrCantBootstrap {
			return nil, fmt.Errorf("bootstrap: %w", f.Error())
		}
	}

	return node, nil
}

func (n *Node) Shutdown() error {
	if n.raft != nil {
		f := n.raft.Shutdown()
		if f.Error() != nil {
			return f.Error()
		}
	}
	if n.transport != nil {
		return n.transport.Close()
	}
	return nil
}

func (n *Node) IsLeader() bool {
	return n.raft != nil && n.raft.State() == raft.Leader
}

func (n *Node) LeaderRaftAddr() string {
	if n.raft == nil {
		return ""
	}
	return string(n.raft.Leader())
}

// encodes PutOp and proposes to raft, waits for commit
func (n *Node) ProposePut(op store.PutOp, timeout time.Duration) error {
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(op); err != nil {
		return err
	}
	f := n.raft.Apply(b.Bytes(), timeout)
	return f.Error()
}

func (n *Node) Role() string {
	if n.raft == nil {
		return ""
	}
	if n.raft.State() == raft.Leader {
		return "leader"
	}
	return "follower"
}

func (n *Node) Term() uint64 {
	if n.raft == nil {
		return 0
	}
	return n.raft.CurrentTerm()
}

func (n *Node) LastContactMS() int64 {
	if n.raft == nil {
		return 1 << 30 // this will be considered as stale
	}
	t := n.raft.LastContact()
	if t.IsZero() {
		return 1 << 30
	}
	return time.Since(t).Milliseconds()
}

// this will issue a linearizability barrier against the current leader.
func (n *Node) Barrier(timeout time.Duration) error {
	if n.raft == nil {
		return fmt.Errorf("raft not initialized")
	}
	f := n.raft.Barrier(timeout)
	return f.Error()
}

// choose the smallest address as "first" bootstrap node
func isLexicographicallyFirst(self raft.ServerAddress, peers []raft.ServerAddress) bool {
	cp := append([]raft.ServerAddress(nil), peers...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	return len(cp) > 0 && string(cp[0]) == string(self)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
