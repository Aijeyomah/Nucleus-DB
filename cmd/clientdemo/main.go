package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Aijeyomah/NucleusDB/internals/client"
	"github.com/Aijeyomah/NucleusDB/internals/cluster"
)

func main() {
	// fetch cluster map
	resp, err := http.Get("http://localhost:9000/cluster/map")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	var cm cluster.ClusterMap
	if err := json.NewDecoder(resp.Body).Decode(&cm); err != nil {
		panic(err)
	}

	r := client.NewRouter(cm)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	keys := []string{"user:1", "order:9", "cart:22", "user:2"}
	for _, k := range keys {
		if _, err := r.Put(ctx, k, "val-"+k); err != nil {
			panic(fmt.Errorf("put %s: %w", k, err))
		}
		v, err := r.Get(ctx, k)
		if err != nil {
			panic(fmt.Errorf("get %s: %w", k, err))
		}
		fmt.Printf("%s -> %s\n", k, v)
	}
}
