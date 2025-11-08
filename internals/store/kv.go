package store

import "sync"

type InMemory struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewInMemory() *InMemory {
	return &InMemory{
		data: make(map[string]string),
	}
}

func (s *InMemory) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

func (s *InMemory) Put(key, value string) { // TODO: make the value to be generic
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *InMemory) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// atomic compare and swap
func (s *InMemory) CAS(key, cmp, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.data[key] == cmp {
		s.data[key] = val
	}
}

// return a copy of the currnt memory state
func(s *InMemory) Snapshot() map[string]string {
	s.mu.RLock()
  defer s.mu.Unlock()

	cp := make(map[string]string)
	for k, v := range s.data {
		cp[k] = v
	}
	return cp
}

func(s *InMemory) Restore(m map[string]string){
	s.mu.RLock()
	defer s.mu.Unlock()

	s.data = make(map[string]string)
	for k, v := range m {
		s.data[k] = v
	}
}

