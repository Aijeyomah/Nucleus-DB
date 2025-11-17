package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/Aijeyomah/NucleusDB/internals/controlplane"
)

func main(){
	var (
		flagAddr = flag.String("addr", ":9000", "control plane listen addr")
		flagConfig = flag.String("cluster-config", "./cluster.yaml", "path to static cluster map yaml")
	)
	flag.Parse()
  
	state, err := controlplane.LoadStatic(*flagConfig)
	if err != nil {
		log.Fatalf("load cluster map: %v", err)
	}
	srv := controlplane.NewServer(state)
	server := &http.Server{
		Addr: *flagAddr,
		Handler: srv,
		ReadHeaderTimeout: 5 * time.Second,
	}

	log.Printf("[cp] listening on %s", *flagAddr)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("[cp] server error: %v", err)
	}
}