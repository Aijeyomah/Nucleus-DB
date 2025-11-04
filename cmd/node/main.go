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
	"syscall"
	"time"

	"github.com/Aijeyomah/NucleusDB/internals/api/httpserver"
	"github.com/Aijeyomah/NucleusDB/internals/config"
	"github.com/Aijeyomah/NucleusDB/internals/store"
)

// might change to gin but just learning how net http works
func main() {
	var (
		ConfigFile      = flag.String("config", "", "optional path to config file (yaml)")
		NodeId          = flag.String("node-id", "", "id of this node, must be unique in cluster")
		ShardID         = flag.Int("shard-id", 0, "shard this node belongs to")
		HttpAddr        = flag.String("http-addr", "", "http listen address")
		FlagControlAddr = flag.String("control-plane", "", "control plane address, for example http://localhost:9000")

		// Leader simulation for v1 (manual until Raft is implemented)
		flagIsLeader     = flag.Bool("leader", true, "treat this node as leader for its shard (temporary for v1)")
		flagLeaderAdvert = flag.String("leader-addr", "", "leader HTTP base URL to advertise to clients when not leader (e.g., http://localhost:8080)")
		flagMaxByteValue = flag.Int("max-value-bytes", 1024, "maximum allowed request body size for PUT (bytes)")
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
	controlPlane := resolve(*FlagControlAddr, cfg.ControlPlane)
	shardId := *ShardID
	if shardId == 0 {
		shardId = cfg.ShardId
	}
	log.Printf("starting node. node_id=%s shard=%d http_addr=%s control_plane=%s leader=%v leader_advert=%s", nodeId, shardId, httpAddr, controlPlane, *flagIsLeader, *flagLeaderAdvert)

	kv := store.NewInMemory()

	handler := httpserver.New(httpserver.Options{
		NodeId:          nodeId,
		ShardId:         shardId,
		IsLeader:        *flagIsLeader,
		LeaderAdvertise: *flagLeaderAdvert,
		KV:              kv,
		MaxByteValue:    *flagMaxByteValue,
		MaxByteKey:      4096, // This is a reasonable value
	})

	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

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
