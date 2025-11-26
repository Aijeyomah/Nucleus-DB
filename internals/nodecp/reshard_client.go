package nodecp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Aijeyomah/NucleusDB/internals/cluster"
)

type ReshardClient struct {
	base string
	hc   *http.Client
}

func NewReshardClient(base string) *ReshardClient {
	return &ReshardClient{
		base: strings.TrimRight(base, "/"),
		hc:   &http.Client{Timeout: 5 * time.Second},
	}
}

type reshardPlanResp struct {
	Active bool `json:"active"`
	Plan   *struct {
		New   cluster.ClusterMap `json:"new_cluster_map"`
		Epoch int64              `json:"epoch"`
	} `json:"plan"`
}

func (c *ReshardClient) GetPlan() (active bool, newMap *cluster.ClusterMap, epoch int64, err error) {
	req, _ := http.NewRequest(http.MethodGet, c.base+"/reshard/plan", nil)
	resp, err := c.hc.Do(req)
	if err != nil {
		return false, nil, 0, err
	}
	defer resp.Body.Close()
	var out reshardPlanResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, nil, 0, err
	}
	if !out.Active || out.Plan == nil {
		return false, nil, 0, nil
	}
	return true, &out.Plan.New, out.Plan.Epoch, nil
}

func (c *ReshardClient) ReportProgress(sourceShard int, moved, total int64) {
	body, _ := json.Marshal(map[string]any{
		"source_shard_id": sourceShard,
		"moved":           moved,
		"total":           total,
	})
	_, _ = c.hc.Post(c.base+"/reshard/status", "application/json", bytes.NewReader(body))
}

func (c *ReshardClient) Cutover() error {
	req, _ := http.NewRequest(http.MethodPost, c.base+"/reshard/cutover", nil)
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode%100 != 2 {
		return fmt.Errorf("cutover failed: %s", resp.Status)
	}
	return nil
}
