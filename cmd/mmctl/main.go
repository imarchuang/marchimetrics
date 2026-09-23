// mmctl is the marchimetrics inspection tool. The on-disk parts are
// binary (gorilla-encoded), so this is the human-readable window into
// them — the role od/cat played before compression.
//
// Usage:
//
//	mmctl inspect <part-dir> [-storageDataPath PATH] [-series ID] [-samples] [-limit N]
//
// <part-dir> is partitions/YYYYMMDD/parts/NNNNNN under the data path.
// Labels are resolved by replaying the registry from -storageDataPath
// (default: derived from the part dir).
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/imarchuang/marchimetrics/storage"
)

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "inspect":
		err = runInspect(os.Stdout, os.Args[2:])
	default:
		usage(os.Stderr)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "mmctl:", err)
		os.Exit(1)
	}
}

func usage(w io.Writer) {
	fmt.Fprintln(w, `mmctl — marchimetrics inspection tool

Commands:
  inspect <part-dir>   decode a part and print its contents

Flags (inspect):
  -storageDataPath PATH  data root (default: derived from part-dir)
  -series ID             only this SeriesID
  -samples               print every sample, not just per-series summary
  -limit N               with -samples, cap samples printed per series`)
}

func runInspect(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("inspect", flag.ContinueOnError)
	dataPath := fs.String("storageDataPath", "", "data root (default: derived from part dir)")
	seriesFilter := fs.Uint64("series", 0, "only this SeriesID")
	showSamples := fs.Bool("samples", false, "print every sample")
	limit := fs.Int("limit", 20, "max samples per series with -samples")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("inspect needs exactly one part dir")
	}
	partDir := fs.Arg(0)

	root := *dataPath
	if root == "" {
		// part dir is <data>/partitions/<day>/parts/<name>
		root = filepath.Clean(filepath.Join(partDir, "..", "..", "..", ".."))
	}
	reg, err := storage.LoadRegistry(root)
	if err != nil {
		return fmt.Errorf("cannot load registry from %s: %w", root, err)
	}

	meta, data, err := storage.InspectPart(partDir)
	if err != nil {
		return err
	}

	enc := meta.Encoding
	if enc == "" {
		enc = "raw"
	}
	fmt.Fprintf(w, "part: %s\n", partDir)
	fmt.Fprintf(w, "tier: %s  encoding: %s\n", meta.Tier, enc)
	fmt.Fprintf(w, "time range: %s .. %s\n",
		time.UnixMilli(meta.MinTime).UTC().Format(time.RFC3339),
		time.UnixMilli(meta.MaxTime).UTC().Format(time.RFC3339))
	fmt.Fprintf(w, "series: %d  samples: %d\n", meta.SeriesCount, meta.SamplesCount)

	ids := make([]uint64, 0, len(data))
	for id := range data {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		if *seriesFilter != 0 && id != *seriesFilter {
			continue
		}
		samples := data[id]
		labels, ok := reg.Labels(id)
		name := fmt.Sprintf("SeriesID %d", id)
		if ok {
			name = formatLabels(labels)
		}
		fmt.Fprintf(w, "\n%s  (%d samples)\n", name, len(samples))
		if !*showSamples {
			if len(samples) > 0 {
				first, last := samples[0], samples[len(samples)-1]
				fmt.Fprintf(w, "  first: %s = %g\n", time.UnixMilli(first.Timestamp).UTC().Format(time.RFC3339), first.Value)
				fmt.Fprintf(w, "  last:  %s = %g\n", time.UnixMilli(last.Timestamp).UTC().Format(time.RFC3339), last.Value)
			}
			continue
		}
		n := len(samples)
		if *limit > 0 && n > *limit {
			n = *limit
		}
		for _, sm := range samples[:n] {
			fmt.Fprintf(w, "  %s  %g\n", time.UnixMilli(sm.Timestamp).UTC().Format(time.RFC3339), sm.Value)
		}
		if n < len(samples) {
			fmt.Fprintf(w, "  ... (%d more)\n", len(samples)-n)
		}
	}
	return nil
}

// formatLabels renders a label set in Prometheus notation.
func formatLabels(labels []storage.Label) string {
	var name string
	var rest []string
	for _, l := range labels {
		if l.Name == storage.MetricNameLabel {
			name = l.Value
			continue
		}
		rest = append(rest, fmt.Sprintf(`%s=%q`, l.Name, l.Value))
	}
	if len(rest) == 0 {
		return name
	}
	return fmt.Sprintf("%s{%s}", name, strings.Join(rest, ", "))
}
