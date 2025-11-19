package cluster

type Node struct {
	NodeID   string `json:"node_id" yaml:"node_id"`
	ShardID  int    `json:"shard_id" yaml:"shard_id"`
	RaftAddr string `json:"raft" yaml:"raft"`
	HTTPAddr string `json:"http" yaml:"http"`
}

type Shard struct {
	ID    int    `json:"id" yaml:"id"`
	Nodes []Node `json:"nodes" yaml:"nodes"`
}

type ClusterMap struct {
	Shards            []Shard `json:"shards" yaml:"shards"`
	ReplicationFactor int     `json:"replication_factor" yaml:"replication_factor"`
}

func (c *ClusterMap) ShardIDs() []int {
	out := make([]int, 0, len(c.Shards))
	for _, s := range c.Shards {
		out = append(out, s.ID)
	}
	return out
}

func (c *ClusterMap) ShardByID(id int) *Shard {
	for i := range c.Shards {
		if c.Shards[i].ID == id {
			return &c.Shards[i]
		}
	}
	return nil
}
