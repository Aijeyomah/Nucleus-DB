package raftgroup

import (
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

type Stores struct{
	LogStore raft.LogStore
	SnapshotStore raft.SnapshotStore
	StableStore raft.StableStore
}

func OpenStores(dir string, retain int) (*Stores, error){
	if err := os.MkdirAll(dir, 0o755); err != nil{
		return nil, err
	}
	boltPath := filepath.Join(dir, "raft.db")
	bs, err := raftboltdb.NewBoltStore(boltPath)
	if err != nil {
		return nil, err
	}

	ss, err := raft.NewFileSnapshotStore(dir, retain, os.Stderr)
	if err != nil {
		return nil, err
	}

	return &Stores{
		LogStore: bs,
		StableStore: bs,
		SnapshotStore: ss,
	}, nil

}