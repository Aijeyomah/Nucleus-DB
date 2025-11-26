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
	"github.com/Aijeyomah/NucleusDB/internals/nodecp"
	"github.com/Aijeyomah/NucleusDB/internals/raftgroup"
	"github.com/Aijeyomah/NucleusDB/internals/reshard"
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

	// long-lived context for background loops (e.g., heartbeat)
	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()

	cfg, err := config.Load(config.Options{
		File:          *ConfigFile,
		EnvPrefix:     "KV_",
		CLI:           config.CLI{NodeID: *NodeId, ShardId: *ShardID, HTTPAddr: *HttpAddr, ControlPlane: *FlagControlAddr},
		GenerateIfNil: true,
	})
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// prioritize CLI over YAML defaults
	nodeId := resolve(*NodeId, cfg.NodeId)
	httpAddr := resolve(*HttpAddr, cfg.HTTPAddr)

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

	controlPlane := resolve(*FlagControlAddr, cfg.ControlPlane)
	if strings.TrimSpace(controlPlane) != "" {
		// start migrator loop
		mig := reshard.NewMigrator(shardId, kv, rnode, controlPlane)
		reporter := func(moved, total int64) {
			rc := nodecp.NewReshardClient(controlPlane)
			rc.ReportProgress(shardId, moved, total)
		}
		ctx, cancelMig := context.WithCancel(context.Background())
		defer cancelMig()
		go mig.Run(ctx, reporter)

		reg := nodecp.New(controlPlane)

		role := "follower"
		if rnode.IsLeader() {
			role = "leader"
		}

		// Register once
		if err := reg.Register(
			nodeId, shardId,
			*flagAdvertiseHTTP, string(*flagRaftAddr),
			role, 0, // might replace with rnode.Term() if i need to expose
		); err != nil {
			log.Printf("[cp] register failed: %v", err)
		} else {
			log.Printf("[cp] registered")
		}

		// Heartbeat loop
		go func(ctx context.Context) {
			t := time.NewTicker(2 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					currRole := "follower"
					if rnode.IsLeader() {
						currRole = "leader"
					}
					if err := reg.Heartbeat(nodeId, shardId, currRole, 0, *flagRaftAddr, *flagAdvertiseHTTP); err != nil {
						log.Printf("[cp] heartbeat err: %v", err)
					}
				case <-ctx.Done():
					return
				}
			}
		}(runCtx)

		// align live Raft config with desired membership from control plane.
		go func(ctx context.Context) {
			t := time.NewTicker(3 * time.Second)
			defer t.Stop()

			for {
				select {
				case <-t.C:
					if !rnode.IsLeader() {
						continue
					}
					desired, err := reg.Desired()
					if err != nil {
						log.Printf("[cp] desired fetch err: %v", err)
						continue
					}
					want := desired.ByShard[shardId]
					if want == nil {
						//skipping cos Nothing desired for this shard yet
						continue
					}
					// Current Raft configuration (keyed by ServerID which we set to raft address)
					cur, err := rnode.CurrentConfig()
					if err != nil {
						log.Printf("[raft] get config err: %v", err)
						continue
					}

					curByRaft := make(map[string]raft.ServerID, len(cur))
					for sid, addr := range cur {
						curByRaft[string(addr)] = sid
					}

					wantByRaft := make(map[string]nodecp.DesiredReplica, len(want))
					for _, rep := range want { // want is map[nodeID]DesiredReplica
						wantByRaft[rep.Raft] = rep
					}

					for raftAddr, rep := range wantByRaft {
						if _, present := curByRaft[raftAddr]; present {
							continue
						}
						log.Printf("[mship] add learner shard=%d raft=%s node=%s", shardId, raftAddr, rep.NodeID)
						if err := rnode.AddReplicaLearner(
							raft.ServerID(raftAddr),
							raft.ServerAddress(raftAddr),
							5*time.Second,
						); err != nil {
							log.Printf("[mship] add learner failed raft=%s err=%v", raftAddr, err)
							// Keep going. The next tick will retry.
						}
					}

					if err := rnode.Barrier(5 * time.Second); err != nil {
						log.Printf("[raft] barrier before promote failed: %v", err)
						// Skip promotions this tick. We will retry next tick.
						continue
					}

					for raftAddr := range wantByRaft {
						if err := rnode.PromoteReplica(
							raft.ServerID(raftAddr),
							raft.ServerAddress(raftAddr),
							5*time.Second,
						); err != nil {
							log.Printf("[mship] promote raft=%s err=%v", raftAddr, err)
						}
					}

					// Refresh current config after possible membership changes
					cur, err = rnode.CurrentConfig()
					if err != nil {
						log.Printf("[raft] get config err after promote: %v", err)
						continue
					}
					curByRaft = make(map[string]raft.ServerID, len(cur))
					for sid, addr := range cur {
						curByRaft[string(addr)] = sid
					}

					//  removing any extras that are not desired
					for sid, addr := range cur {
						raftAddr := string(addr)

						// Keep if desired
						if _, keep := wantByRaft[raftAddr]; keep {
							continue
						}
						if raftAddr == rnode.LeaderRaftAddr() {
							log.Printf("[mship] skip remove leader raft=%s", raftAddr)
							continue
						}

						if len(cur) <= 1 {
							continue
						}

						log.Printf("[mship] remove shard=%d raft=%s", shardId, raftAddr)
						if err := rnode.RemoveReplica(sid, 5*time.Second); err != nil {
							log.Printf("[mship] remove failed raft=%s err=%v", raftAddr, err)
						}
					}

				case <-ctx.Done():
					return
				}
			}
		}(runCtx)

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

	// stop background loops
	runCancel()

	// graceful HTTP shutdown
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
