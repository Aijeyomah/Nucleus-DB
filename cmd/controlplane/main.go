package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/Aijeyomah/NucleusDB/internals/controlplane"
)

func main() {
	var (
		flagAddr      = flag.String("addr", ":9000", "control plane listen addr")
		flagConfig    = flag.String("cluster-config", "./cluster.yaml", "path to static cluster map yaml")
		flagTTLms     = flag.Int("node-ttl-ms", 5000, "node heartbeat TTL in ms")
		flagJanitorMs = flag.Int("janitor-ms", 1000, "prune interval in ms")
	)
	flag.Parse()

	state, err := controlplane.LoadStatic(*flagConfig, time.Duration(*flagTTLms)*time.Millisecond)
	if err != nil {
		log.Fatalf("load cluster map: %v", err)
	}
	srv := controlplane.NewServer(state)

	stop := make(chan struct{})
	controlplane.StartJanitor(state, time.Duration(*flagJanitorMs)*time.Millisecond, stop)

	server := &http.Server{
		Addr:              *flagAddr,
		Handler:           srv,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("[cp] listening on %s", *flagAddr)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("[cp] server error: %v", err)
	}
}
