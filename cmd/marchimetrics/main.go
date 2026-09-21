// Command marchimetrics is a single-process time series database:
// ingest -> store -> query, modeled after the VictoriaMetrics
// single-node storage core. See PLAN.md and docs/LEARNING.md.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/imarchuang/marchimetrics/storage"
)

var (
	httpListenAddr = flag.String("httpListenAddr", ":8428",
		"TCP address to listen for HTTP connections")
	storageDataPath = flag.String("storageDataPath", "marchimetrics-data",
		"Directory where time series data is stored")
	flushInterval = flag.Duration("inmemoryDataFlushInterval", 5*time.Second,
		"How often in-memory samples are flushed to disk parts. "+
			"A crash can lose at most this much data (durability window)")
)

func main() {
	flag.Parse()

	store, err := storage.Open(*storageDataPath)
	if err != nil {
		log.Fatalf("cannot open storage at %q: %s", *storageDataPath, err)
	}
	log.Printf("storage opened at %q (flush interval %s)", store.Path(), *flushInterval)
	store.StartFlushLoop(*flushInterval)

	srv := newServer(store)
	go func() {
		log.Printf("listening on %s", *httpListenAddr)
		if err := http.ListenAndServe(*httpListenAddr, srv.routes()); err != nil {
			log.Fatalf("http server error: %s", err)
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")
	if err := store.Close(); err != nil {
		log.Printf("storage close error: %s", err)
	}
}
