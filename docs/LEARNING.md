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

### IndexDB as append-only segments

The series registry used to persist as one `series/names.json` that every
flush rewrote in full — a growing restart tax and a rewrite of mostly
unchanged bytes. It is now an LSM of its own: each flush appends only the
series registered since the last flush as one immutable segment under
`series/indexdb/NNNNNN.seg` (delta-encoded IDs + varint length-prefixed
label strings), and Open replays the segments in order to rebuild the
forward and inverted indexes. Crash safety mirrors part publishing:
segment write goes through a `.tmp` + rename, and a torn tail record
(crash between segment rename and the data part it accompanied) is
tolerated at replay — the registry simply stops at the last complete
record, which matches the at-most-one-flush durability window. This is
the thin stand-in for VM's mergeset IndexDB; rotation and per-day index
partitions (which would let retention actually forget old series) are
deliberately left out — see Non-goals.

### Index retention: forget the inverted index, keep the identity

The next question after append-only segments is whether retention should
make the registry *forget* expired series. Two designs:

**VM's answer — the index expires with the data.** The inverted index
(label → TSID) is partitioned by generation and dropped wholesale with
rotation; a series' identity does not depend on the index because the
data parts reference the TSID directly.

- *Pros:* index memory tracks the active time window, not all of history
  (this is how VM survives millions of series); data and its index share
  one lifecycle, so there are no orphan index entries; high-cardinality
  bursts (e.g. a request-ID label) vanish completely once expired.
- *Cons:* a big complexity step — when data outlives its index
  generation, reverse lookup needs the labels redundantly stored in the
  data part, or you accept "old data is addressable by TSID only, not by
  label". Query semantics get subtle (does `{job="api"}` still match a
  series on day 1 whose day-1 index entry is gone but which is still
  being written on day 30?), and `Labels(id)` can fail entirely once
  every index generation for a series has expired.

**Our simplification — the forward map never expires.** `ID → labels`
stays global and permanent; only the inverted index (`label → ID`) is
partitioned by day and expires with retention.

- *Pros:* simple — the forward map needs no partitioning or eviction;
  a series' identity and `Labels(id)` never disappear, so data parts
  store no redundant labels and there are no edge cases; the semantics
  fit in one sentence ("data expires by day, series registrations are
  kept forever"); it builds directly on the append-only segments above.
- *Cons:* memory still grows with the total number of distinct series
  ever seen (the forward map never shrinks), so the "restart tax" is
  only half solved — the inverted half can be forgotten, the forward
  half cannot; a high-cardinality burst permanently enlarges the
  registry; and it diverges from what VM actually does, so the
  index-lifecycle lesson is deferred.

**Decision: take the simplification for the MVP.** It buys "retention can
prune the index" with a small change (day-partition the inverted index)
while avoiding the "data alive but labels unresolvable" complexity. At
MVP scale a permanent forward map is fine (100k series ≈ tens of MB). If
the registry ever outgrows memory, that is the moment to revisit VM's
design — with a much clearer understanding of why it exists.

**Implemented (PR-B).** The inverted index lives in
`series/inverted/YYYYMMDD/` segments (same binary format as the forward
segments). `Append` adds the series to each covered day's *in-memory*
inverted index so unflushed data stays findable; `Flush` persists only
the not-yet-persisted `(day, SeriesID)` pairs (`persisted` set) *before*
writing the data part — an inverted entry pointing at unwritten data is
harmless, while data whose index entry is lost would be invisible after a
restart. `ApplyRetention` deletes the day's inverted directory along with
the data partition and calls `Registry.DropDay`. A series active on days
1 and 30 keeps its day-30 findability when day 1 expires; a series seen
only on day 1 becomes unfindable by label, but `Labels(id)` still
resolves — the identity never expires.

## Non-goals (deliberately omitted)

- Cluster split (`vminsert` / `vmselect` / `vmstorage`), HA, replication
- Full mergeset IndexDB with rotation, composite and per-day indexes
- PromQL functions — only `metric{label="value"}` equality selectors
- Dedup (`-dedup.minScrapeInterval`), downsampling, retention filters
- Cardinality bloom limiters, precisionBits lossy encoding
- Snapshots / backup, vmselect fan-out
- VictoriaMetrics wire format / on-disk binary compatibility
