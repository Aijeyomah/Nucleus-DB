package hashring

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

type shardPoint struct {
	hash    uint32
	ShardID int
}

type Ring struct {
	mu      sync.RWMutex
	points  []shardPoint
	replica int
}

func New(shardIDs []int, replica int) *Ring {
	if replica <= 0 {
		replica = 50
	}
	r := &Ring{replica: replica}
	for _, id := range shardIDs {
		r.addShard(id)
	}
	return r
}

func (r *Ring) addShard(shardID int) {
	for i := 0; i < r.replica; i++ {
		h := hashKey(strconv.Itoa(shardID) + ":" + strconv.Itoa(i))
		r.points = append(r.points, shardPoint{hash: h, ShardID: shardID})
	}
}

func (r *Ring) GetShard(keyID string) (int, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.points) == 0 {
		return 0, false
	}
	h := hashKey(keyID)

	idx := sort.Search(len(r.points), func(i int) bool {
		return r.points[i].hash >= h
	})

	if idx == len(r.points) {
		idx = 0
	}
	return r.points[idx].ShardID, true
}

func hashKey(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
