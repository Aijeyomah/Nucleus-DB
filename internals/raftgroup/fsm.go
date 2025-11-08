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

func (f *FSM) Apply(l *raft.Log) interface{} {
	dec := gob.NewDecoder(bytes.NewReader(l.Data))

	var op store.PutOp
	if err := dec.Decode(&op); err != nil && op.Key == "" {
		return nil
	}
	// handle idenpotency to prevent mu -> but will check if this causes latency
	if op.ClientID != "" && op.RequestID != "" {
		id := fmt.Sprintf("%s:%s", op.ClientID, op.RequestID)
		f.mu.Lock()

		if _, ok := f.seen[id]; ok {
			// ignore processing this request
			f.mu.Unlock()
			return nil
		}
		f.seen[id] = struct{}{}
		f.mu.Unlock()
	}
	// apply to KV
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
