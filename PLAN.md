# marchimetrics MVP plan

Mirror of the marchilogs approach: read VictoriaMetrics (`~/workspace_victoria/VictoriaMetrics`), keep the LSM DNA, cut everything that is not needed to **ingest → store → query time series**.

Primary reference: `VictoriaMetrics/lib/storage/`
Cluster roles (`vminsert` / `vmselect` / `vmstorage`) are **out of scope** for MVP (single process like early marchilogs).

---

## 1. What VictoriaMetrics is doing (relevant slice)

### Roles (skip cluster for MVP)

| Component | Role | MVP |
|---|---|---|
| Single-node `victoria-metrics` | insert + select + storage | **Yes — one binary** |
| `vmstorage` / `vminsert` / `vmselect` | cluster split | Later |
| `vmagent` | scrape/remote_write client | Optional later |

### On-disk (from `lib/storage/filenames.go`)

```
<data>/
  data/
    small/YYYY_MM/<partID>/   # monthly partition, small parts
      parts.json              # active Small/Big lists (≈ marchilogs manifest)
      metadata.json
      metaindex.bin
      index.bin               # block headers ordered by TSID
      timestamps.bin
      values.bin
    big/YYYY_MM/<partID>/     # merged larger parts
  indexdb/YYYY_MM/            # mergeset: labels ↔ MetricID/TSID (heavy)
```

Hierarchy: **Storage → month partition → parts (mem / small / big) → blocks** (one TSID’s samples in a time slice).

### Write path (simplified)

```
samples
  → resolve/create TSID (label set → id)
  → buffer raw rows (not always query-visible)
  → flush to inmemory part (~seconds)
  → flush inmemory → small/ on disk (durability)
  → merge small → big (LSM)
  → update indexdb for new series
```

### Read path (simplified)

```
PromQL / label matchers
  → IndexDB: matchers → []TSID
  → for each partition/part: metaindex → index.bin → timestamps+values
  → merge series by TSID/time
```

### Deliberately skip (same spirit as marchilogs)

- Full `lib/mergeset` IndexDB + rotation + composite/day indexes
- Dedup (`-dedup.minScrapeInterval`), downsampling, replication
- Cardinality bloom limiters, precisionBits lossy encoding
- Snapshots/backup, vmselect fan-out

**Keep:** month (or day) partitions, immutable parts, `parts.json` manifest, mem→disk flush, small→big merge, **some** label→series index, block scan by series id.

---

## 2. marchimetrics vs marchilogs (mental map)

| Concept | marchilogs | marchimetrics |
|---|---|---|
| Identity | stream tags (`service=api`) | **label set** → **SeriesID / TSID** |
| Partition | day `YYYYMMDD` | month `YYYY_MM` (MVP may use **day** to stay closer to marchilogs) |
| Row | log line | `(timestamp, float64 value)` for one series |
| Part files | per-stream dirs + `.col` | shared `timestamps.bin` + `values.bin` + index (VM style) **or** per-series columns (simpler, slower) |
| Catalog | day `indexdb.json` tag→parts | **series index**: metric name + labels → SeriesID; inverted tag→SeriesID |
| Bloom | `_msg` trigram skip | optional later |
| Query | contains + stream eq | **instant / range** by metric+labels; no full PromQL at first |

Recommendation: **reuse marchilogs engineering habits** (Go module, `storage/`, HTTP, docker, manifests, compaction worker, query stats), but **change the data model to SeriesID-sorted samples**.

---

## 3. MVP product goals

**In:**

1. Simple JSON or Prometheus text ingest: `{metric, labels, samples:[{t,v}]}`
2. Persist under `-storageDataPath`
3. `GET /api/v1/query_range` **subset**: only `metric{label=value}` selectors (no rate/sum yet)
4. Background flush + small→big merge + retention by day
5. Docker + inspectable dirs (like marchilogs)

**Out:**

- PromQL functions, recording rules, scrape
- Cluster, HA, dedup, downsampling
- Exact VictoriaMetrics wire format / part binary compatibility

**Pass bar:** N series × M points; query one series over a window; kill -9 with flush interval ≤5s loses only the tip; merge reduces part count; query stats show parts/blocks/points scanned.

---

## 4. Proposed on-disk layout (MVP)

Prefer **day partitions** (easier debug; can move to month later):

```
<data>/
  partitions/YYYYMMDD/
    manifest.json          # {"parts":["000001","000007"]}
    indexdb.json           # optional day-local inverted helpers
    parts/000001/
      meta.json            # time range, series count, tier
      series.index         # SeriesID → block offset (JSON ok for v0)
      timestamps.bin
      values.bin
  series/
    inverted.json          # "__name__=http_requests": [1,2], "job=api": [1]
    names.json             # id → {name, labels}
```

**v0 simplification (fastest):** one directory per series inside the part (`blocks/<id>/{t.bin,v.bin}`) — same idea as marchilogs streams. Upgrade to shared bins when IO hurts.

---

## 5. Implementation slices (PRs)

### PR0 — Scaffold

- `go mod`, `cmd/marchimetrics`, `Dockerfile`, `docker-compose.yml`
- `storage.Open`, healthz, flags: `-storageDataPath`, flush interval
- `LEARNING.md`: VM mapping + non-goals

### PR1 — Series ID + append buffer

- Canonical labels → hash → `SeriesID`
- `Append(samples)` into mem buffers
- In-memory search works **before** flush

### PR2 — Flush to immutable parts + manifest

- Atomic `.publishing-*` → rename; append `manifest.json`
- Day partition from sample timestamp
- `meta.json` with `tier: small`

### PR3 — Query range (selector only)

- Parse `http_requests_total{job="api"}` (metric + equality labels)
- Resolve SeriesIDs via inverted index
- Scan mem + disk parts; merge by time
- Query stats headers

### PR4 — Compaction

- Merge ≥N small parts → one big; manifest swap
- Force merge HTTP

### PR5 — Retention + durability knobs

- Drop old day dirs
- Optional WAL for tip (default off)
- Document flush interval = durability window

### PR6 (optional) — Prometheus import / remote_write

---

## 6. Package layout

```
marchimetrics/
  cmd/marchimetrics/main.go
  storage/
    storage.go
    series.go
    partition.go
    part.go
    manifest.go
    compaction.go
    retention.go
    query.go
  docs/LEARNING.md
  PLAN.md
```

---

## 7. VictoriaMetrics source study list

| Topic | Path |
|---|---|
| Filenames / dirs | `lib/storage/filenames.go` |
| TSID | `lib/storage/tsid.go` |
| AddRows | `lib/storage/storage.go` |
| raw rows → inmemory | `lib/storage/raw_row.go`, `partition.go` |
| inmemory part | `lib/storage/inmemory_part.go` |
| merge | `lib/storage/merge.go` |
| part / block search | `lib/storage/part.go`, `block.go` |
| IndexDB (skim ideas only) | `lib/storage/index_db.go` |

Do **not** vendor VM; reimplement a thin subset.

---

## 8. First week checklist

1. Init module + HTTP hello
2. Series registry (labels → id) with tests
3. Append + in-memory query_range
4. Flush one part; reopen; query still works
5. Two flushes + force merge; point count unchanged
6. Docker volume; inspect data dir

Stop before PromQL or cluster.

---

## 9. Success criteria

- [ ] Ingest 10k series × 100 points
- [ ] Query one series, correct values
- [ ] Part merge without dup/loss
- [ ] Kill during tip window loses ≤ flush interval
- [ ] LEARNING.md lists VM features omitted
