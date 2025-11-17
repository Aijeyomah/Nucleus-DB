package controlplane

import (
	"fmt"
	"os"
	"sync"

	"github.com/Aijeyomah/NucleusDB/internals/cluster"
	"gopkg.in/yaml.v3"
)

type State struct {
	mu      sync.RWMutex
	cluster cluster.ClusterMap
}

func LoadStatic(path string) (*State, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read cluster map: %w", err)
	}
	var cm cluster.ClusterMap
	if err := yaml.Unmarshal(b, &cm); err != nil {
		return nil, fmt.Errorf("parse cluster map: %w", err)
	}
	return &State{cluster: cm}, nil
}

func (s *State) GetClusterMap() cluster.ClusterMap {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cluster
}
