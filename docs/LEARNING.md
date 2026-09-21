# LEARNING — VictoriaMetrics → marchimetrics

marchimetrics is a learning re-implementation of the VictoriaMetrics (VM)
single-node storage core. Method: read VM source
(`~/workspace_victoria/VictoriaMetrics`, mainly `lib/storage/`), keep the
LSM DNA, cut everything not needed to **ingest → store → query time series**.
**No VM code is vendored** — we re-implement a thin subset with simpler
formats (JSON manifests instead of binary metaindex, etc.).

## Concept mapping

| VictoriaMetrics | marchimetrics MVP |
|---|---|
| `Storage → partition (month) → part → block` | `Storage → partition (day) → part → block` |
| TSID (MetricID etc., binary) | SeriesID (hash of canonical label set) |
| `metaindex.bin` / `index.bin` (binary) | `meta.json` / `series.index` (JSON for v0) |
| `timestamps.bin` + `values.bin` per part | same idea, shared per part (or per-series dirs for v0) |
| mergeset IndexDB (`lib/mergeset`) | `series/names.json` + `series/inverted.json` |
| `parts.json` manifest | `manifest.json` per day partition |
| inmemory part → small → big merge | mem buffer → small part → compaction to big |
| `-inmemoryDataFlushInterval` | same flag name; durability window on crash |

## Source study list

| Topic | VM path |
|---|---|
| Filenames / dirs | `lib/storage/filenames.go` |
| TSID | `lib/storage/tsid.go` |
| AddRows | `lib/storage/storage.go` |
| raw rows → inmemory | `lib/storage/raw_row.go`, `partition.go` |
| inmemory part | `lib/storage/inmemory_part.go` |
| merge | `lib/storage/merge.go` |
| part / block search | `lib/storage/part.go`, `block.go` |
| IndexDB (skim ideas only) | `lib/storage/index_db.go` |

## Non-goals (deliberately omitted)

- Cluster split (`vminsert` / `vmselect` / `vmstorage`), HA, replication
- Full mergeset IndexDB with rotation, composite and per-day indexes
- PromQL functions — only `metric{label="value"}` equality selectors
- Dedup (`-dedup.minScrapeInterval`), downsampling, retention filters
- Cardinality bloom limiters, precisionBits lossy encoding
- Snapshots / backup, vmselect fan-out
- VictoriaMetrics wire format / on-disk binary compatibility
