package config

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

var (
	HTTPADDR = ":8080"
)

type Config struct {
	NodeId       string `yaml:"node_id"`
	ShardId      int    `yaml:"shard_id"`
	HTTPAddr     string `yaml:"http_addr"`
	ControlPlane string `yaml:"control_plane"`
}

type CLI struct {
	NodeID       string
	ShardId      int
	HTTPAddr     string
	ControlPlane string
}

type Options struct {
	File          string
	EnvPrefix     string
	CLI           CLI
	GenerateIfNil bool
}

func Load(opts Options) (*Config, error) {
	var cfg Config

	if opts.File != "" {
		bytes, err := os.ReadFile(opts.File)
		if err != nil {
			return nil, fmt.Errorf("read nfig file %w", err)
		}
		if err := yaml.Unmarshal(bytes, &cfg); err != nil {
			return nil, fmt.Errorf("parse config file: %w", err)
		}
	}
	if opts.CLI.NodeID != "" {
		cfg.NodeId = opts.CLI.NodeID
	}
	if opts.CLI.HTTPAddr != "" {
		cfg.HTTPAddr = opts.CLI.NodeID
	}
	if opts.CLI.ShardId != 0 {
		cfg.ShardId = opts.CLI.ShardId
	}
	if opts.CLI.ControlPlane != "" {
		cfg.ControlPlane = opts.CLI.ControlPlane
	}

	if cfg.NodeId == "" && opts.GenerateIfNil {
		cfg.NodeId = generateRandNodeId()
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = HTTPADDR
	}
	return &cfg, nil
}

func generateRandNodeId() string {
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	return fmt.Sprintf("node-%06d", r.Intn(999999))
}
