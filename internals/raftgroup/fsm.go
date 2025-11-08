package raftgroup

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"sync"

	"github.com/Aijeyomah/NucleusDB/internals/store"
	"github.com/hashicorp/raft"
)

func init() {
	gob.Register(store.PutOp{})
	gob.Register(map[string]string{})
}

type FSM struct {
	KV   *store.InMemory
	mu   sync.Mutex
	seen map[string]struct{} // (ClientID, RequestID) -> seen
}

type KvSnapshot struct {
	state map[string]string
}

func NewFSM(kv *store.InMemory) *FSM {
	return &FSM{
		KV:   kv,
		seen: make(map[string]struct{}),
	}
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	var op store.PutOp

	if err := gob.NewDecoder(bytes.NewReader(l.Data)).Decode(&op); err != nil {
		return nil
	}
	if op.Key == "" {
		return nil
	}

	if op.ClientID != "" && op.RequestID != "" {
		id := fmt.Sprintf("%s:%s", op.ClientID, op.RequestID)

		f.mu.Lock()
		if f.seen == nil {
			f.seen = make(map[string]struct{})
		}
		if _, ok := f.seen[id]; ok {
			f.mu.Unlock()
			return nil
		}
		f.seen[id] = struct{}{}
		f.mu.Unlock()
	}

	f.KV.Put(op.Key, op.Value)
	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	state := f.KV.Snapshot()
	return &KvSnapshot{state: state}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	dec := gob.NewDecoder(rc)
	state := make(map[string]string)

	if err := dec.Decode(&state); err != nil {
		return err
	}
	f.KV.Restore(state)
	return nil
}

func (s *KvSnapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	if err := enc.Encode(&s.state); err != nil {
		_ = sink.Cancel()
		return err
	}
	if _, err := sink.Write(buf.Bytes()); err != nil {
		_ = sink.Cancel()
		return err
	}
	return nil

}

func (s *KvSnapshot) Release() {}
