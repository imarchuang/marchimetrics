// Command marchimetrics is a single-process time series database:
// ingest -> store -> query, modeled after the VictoriaMetrics
// single-node storage core. See PLAN.md and docs/LEARNING.md.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
	mergeThreshold = flag.Int("smallPartsMergeThreshold", 3,
		"A day partition with at least this many small parts merges them into one big part after a flush")
	retentionPeriod = flag.String("retentionPeriod", "0",
		"How long to keep data, e.g. 7d. Day partitions whose entire day is older are dropped. 0 keeps data forever")
)

// parseRetentionDays parses the -retentionPeriod value: "0" (forever) or
// "<n>d". Day granularity matches the partition layout.
func parseRetentionDays(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	if !strings.HasSuffix(s, "d") {
		return 0, fmt.Errorf("want a day count like 7d (or 0), got %q", s)
	}
	n, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
	if err != nil || n < 0 {
		return 0, fmt.Errorf("want a day count like 7d (or 0), got %q", s)
	}
	return n, nil
}

func main() {
	flag.Parse()

	store, err := storage.Open(*storageDataPath)
	if err != nil {
		log.Fatalf("cannot open storage at %q: %s", *storageDataPath, err)
	}
	log.Printf("storage opened at %q (flush interval %s)", store.Path(), *flushInterval)
	store.SmallPartsMergeThreshold = *mergeThreshold
	retentionDays, err := parseRetentionDays(*retentionPeriod)
	if err != nil {
		log.Fatalf("bad -retentionPeriod: %s", err)
	}
	store.RetentionDays = retentionDays
	if retentionDays > 0 {
		log.Printf("retention: %dd (checked at startup and hourly)", retentionDays)
	}
	store.StartFlushLoop(*flushInterval)
	store.StartRetentionLoop(time.Hour)

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
