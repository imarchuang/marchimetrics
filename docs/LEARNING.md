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
| `timestamps.bin` + `values.bin` per part | same idea, shared per part |
| mergeset IndexDB (`lib/mergeset`) | `series/names.json` persisted; inverted index rebuilt in memory on open |
| `parts.json` manifest | `manifest.json` per day partition |
| inmemory part → small → big merge | mem buffer → small part → compaction to big |
| `-inmemoryDataFlushInterval` | same flag name; durability window on crash |

## Source study list

| Topic | VM path |
|---|---|
| Filenames / dirs | `lib/storage/filenames.go` |
| TSID (see [TSID.md](TSID.md)) | `lib/storage/tsid.go` |
| AddRows | `lib/storage/storage.go` |
| raw rows → inmemory | `lib/storage/raw_row.go`, `partition.go` |
| inmemory part | `lib/storage/inmemory_part.go` |
| merge | `lib/storage/merge.go` |
| part / block search | `lib/storage/part.go`, `block.go` |
| IndexDB (skim ideas only) | `lib/storage/index_db.go` |

## Design decisions (implemented)

Captured from the PR2/PR3 reviews so the reasoning does not sink into PR
descriptions.

### Shared bin files, one index entry per series block

Each part stores all series in two shared files: `timestamps.bin` (raw LE
int64) and `values.bin` (raw LE float64). Because both use fixed 8-byte
cells, a single `{offset, count}` pair in `series.index` addresses a
series' block in both files at once. Encoding is deliberately raw — VM
does delta/varint encoding and compression here; that stays a non-goal.
(The per-series-dirs alternative from PLAN.md was skipped: 10k series per
flush would mean 20k tiny files per part.)

### Atomic part publishing

Flush writes `.publishing-NNNNNN/`, `os.Rename`s it into place, then
appends to `manifest.json` (itself tmp+rename). The manifest is the
source of truth: part dirs it does not list (crash between rename and
manifest save) are ignored orphans, and stale `.publishing-*` dirs are
removed at open. Part IDs are never reused (next = max existing + 1).

### Flush swaps before it writes

Flush exchanges the mem buffer for a fresh one under lock, then writes to
disk without holding it — appends never block on disk IO. A failed flush
returns the samples to the buffer so the next attempt retries them.

### Registry persistence: names.json only

Only the forward mapping (SeriesID → label set) is persisted, at flush
time; the inverted index is derived from it on open — one source of
truth, one less file to keep consistent. Crash consistency holds because
series registered after the last flush lose their (unflushed) samples
together with their registry entries.

### Query stats semantics

The `X-Marchimetrics-*` headers count the disk tier only — the stats
exist to make disk IO cost visible; scanning the mem buffer is cheap by
design. Two consequences worth remembering:

- `PointsScanned > PointsReturned` signals poor selectivity (whole blocks
  decoded, few points in the window) — the motivation for per-block time
  indexes later on.
- A part skipped via its `meta.json` time range does not count as
  scanned, so an all-zero stats line proves that pruning worked.

### Compaction: merge under the partition write lock

When a partition accumulates `SmallPartsMergeThreshold` small parts
(default 3, checked after each flush; `POST /internal/force_merge`
bypasses the threshold), they merge into one `tier: big` part: read all
blocks, concat per series, time-sort, write a new part, then swap the
manifest and delete the old dirs. The swap phase runs under the
partition's write lock while queries scan under the read lock, so a query
always sees a consistent part set — never a mix of old and new, never a
deleted file. VM instead refcounts parts and deletes asynchronously (see
`lib/storage/partition.go`) — worth revisiting if this lock ever shows up
as a bottleneck. Duplicate (series, timestamp) pairs are kept as-is
(dedup is a non-goal), and big parts are not re-merged in the MVP.

## Non-goals (deliberately omitted)

- Cluster split (`vminsert` / `vmselect` / `vmstorage`), HA, replication
- Full mergeset IndexDB with rotation, composite and per-day indexes
- PromQL functions — only `metric{label="value"}` equality selectors
- Dedup (`-dedup.minScrapeInterval`), downsampling, retention filters
- Cardinality bloom limiters, precisionBits lossy encoding
- Snapshots / backup, vmselect fan-out
- VictoriaMetrics wire format / on-disk binary compatibility
