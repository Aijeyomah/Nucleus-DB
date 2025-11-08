package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Aijeyomah/NucleusDB/internals/api/httpserver"
	"github.com/Aijeyomah/NucleusDB/internals/config"
	"github.com/Aijeyomah/NucleusDB/internals/raftgroup"
	"github.com/Aijeyomah/NucleusDB/internals/store"
	"github.com/hashicorp/raft"
)

// might change to gin but just learning how net http works
func main() {
	var (
		ConfigFile      = flag.String("config", "", "optional path to config file (yaml)")
		NodeId          = flag.String("node-id", "", "id of this node, must be unique in cluster")
		ShardID         = flag.Int("shard-id", 0, "shard this node belongs to")
		HttpAddr        = flag.String("http-addr", "", "http listen address")
		FlagControlAddr = flag.String("control-plane", "", "control plane address, for example http://localhost:9000")

		flagRaftAddr  = flag.String("raft-addr", "127.0.0.1:9001", "raft TCP addr for this node")
		flagRaftPeers = flag.String("raft-peers", "127.0.0.1:9001,127.0.0.1:9002,127.0.0.1:9003", "comma-separated raft peer addrs (include self)")
		flagRaftDir   = flag.String("raft-dir", "./data/raft", "dir for raft db & snapshots")
		flagSnapKeep  = flag.Int("raft-snapshot-retain", 2, "snapshots to retain")

		flagAdvertiseHTTP = flag.String("advertise-http", "http://localhost:8080", "this node's public HTTP base")
		flagPeerHTTP      = flag.String("peer-http", "", "optional mapping raftAddr=httpBase,comma separated")
		flagMaxValueBytes = flag.Int("max-value-bytes", 1024, "max PUT body bytes")
		flagMaxKeyBytes   = flag.Int("max-key-bytes", 4096, "max key bytes")
		flagProposeTOms   = flag.Int("propose-timeout-ms", 5000, "raft propose timeout ms")
	)

	flag.Parse()

	cfg, err := config.Load(config.Options{
		File:          *ConfigFile,
		EnvPrefix:     "KV_",
		CLI:           config.CLI{NodeID: *NodeId, ShardId: *ShardID, HTTPAddr: *HttpAddr, ControlPlane: *FlagControlAddr},
		GenerateIfNil: true,
	})
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	// will priotize CLI values provides over defaults in YAML
	nodeId := resolve(*NodeId, cfg.NodeId)
	httpAddr := resolve(*HttpAddr, cfg.HTTPAddr)
	// controlPlane := resolve(*FlagControlAddr, cfg.ControlPlane)

	shardId := *ShardID
	if shardId == 0 {
		shardId = cfg.ShardId
	}

	// raft config
	peerStrs := splitCSV(*flagRaftPeers)
	peers := []raft.ServerAddress{}
	for _, p := range peerStrs {
		peers = append(peers, raft.ServerAddress(p))
	}

	peerHTTP := formatPeerHttp(*flagPeerHTTP)
	kv := store.NewInMemory()

	fsm := raftgroup.NewFSM(kv)
	rnode, err := raftgroup.NewNode(&raftgroup.Config{
		NodeID:         nodeId,
		RaftAddr:       raft.ServerAddress(*flagRaftAddr),
		DataDir:        *flagRaftDir,
		PeerAddress:    peers,
		SnapshotRetain: *flagSnapKeep,
	}, fsm)
	if err != nil {
		log.Fatalf("raft node: %v", err)
	}

	handler := httpserver.New(httpserver.Options{
		NodeId:         nodeId,
		ShardId:        shardId,
		KV:             kv,
		Raft:           rnode,
		MaxByteValue:   *flagMaxValueBytes,
		MaxByteKey:     *flagMaxKeyBytes,
		ProposeTimeout: time.Duration(*flagProposeTOms) * time.Millisecond,
		PeerHTTP:       peerHTTP,
		AdvertiseHTTP:  *flagAdvertiseHTTP,
	})

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf(`[boot] {"node_id":"%s","shard_id":%d,"http":%q,"raft":%q,"peers":%q,"dir":%q}`,
		nodeId, shardId, httpAddr, *flagRaftAddr, *flagRaftPeers, *flagRaftDir)

	errChan := make(chan error, 1)
	go func() {
		log.Printf("[http] serving on %s", httpAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errChan <- fmt.Errorf("http server: %w", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		log.Printf("[shutdown] signal=%s", sig)

	case err := <-errChan:
		log.Fatalf("[fatal] %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("[warn] http shutdown: %v", err)
	} else {
		log.Printf("[ok] http stopped")
	}

}

// returns the first non-empty string from left to right.
func resolve(vals ...string) string {
	for _, val := range vals {
		if val != "" {
			return val
		}
	}
	return ""
}

func splitCSV(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func formatPeerHttp(s string) map[string]string {
	m := map[string]string{}
	if strings.TrimSpace(s) == "" {
		return m
	}
	for _, kv := range strings.Split(s, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" || !strings.Contains(kv, "=") {
			continue
		}
		parts := strings.SplitN(kv, "=", 2)
		m[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	return m
}
