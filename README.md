# marchimetrics

A minimal time-series database built for learning — the marchilogs approach applied to metrics: read [VictoriaMetrics](https://github.com/VictoriaMetrics/VictoriaMetrics) source, keep the LSM DNA, and cut everything that is not needed to **ingest → store → query time series**.

Single process, day partitions, immutable parts, label-set → SeriesID index, and a small `query_range` subset. No cluster, no PromQL functions, no wire-format compatibility.

## Quickstart

```bash
go run ./cmd/marchimetrics -storageDataPath=/tmp/mm-data
# or: docker compose up --build
```

### Prometheus remote_write

Point a real Prometheus or vmagent at marchimetrics:

```yaml
remote_write:
  - url: http://localhost:8428/api/v1/write
```

The endpoint speaks the standard protocol — a snappy-compressed
`prompb.WriteRequest` — and returns `204` with an
`X-Marchimetrics-Points-Ingested` header. Series without a metric name
are skipped without failing the batch.

### JSON import

Ingest (JSON; timestamps in **milliseconds**; single object or array):

```bash
curl -s localhost:8428/api/v1/import -d '{
  "metric": "http_requests_total",
  "labels": {"job": "api", "instance": "h1:9090"},
  "samples": [{"t": 1789954446000, "v": 1.5}, {"t": 1789954461000, "v": 2.5}]
}'
```

Query (selector subset only; `start`/`end` as unix seconds or RFC3339):

```bash
curl -s 'http://localhost:8428/api/v1/query_range?query=http_requests_total{job="api"}&start=1789954300&end=1789954600'
```

Every query reports its disk cost in response headers (disk tier only;
the in-memory buffer is cheap by design):

```
X-Marchimetrics-Parts-Scanned    # parts opened (meta.json time range overlapped)
X-Marchimetrics-Blocks-Scanned   # series blocks decoded from those parts
X-Marchimetrics-Points-Scanned   # samples decoded, before time filtering
X-Marchimetrics-Points-Returned  # samples in the final merged result
```

Imported data is queryable immediately from the in-memory buffer, and is
flushed to immutable day-partition parts every `-inmemoryDataFlushInterval`
(default 5s) or on demand:

```bash
curl -XPOST localhost:8428/internal/force_flush
# inspect: /tmp/mm-data/partitions/YYYYMMDD/{manifest.json,parts/000001/...}
```

Compaction: once a day partition accumulates `-smallPartsMergeThreshold`
(default 3) small parts, they merge into one `tier: big` part
automatically after a flush — or on demand:

```bash
curl -XPOST localhost:8428/internal/force_merge
```

A crash loses at most one flush interval of samples; everything flushed
survives `kill -9`. Design notes: [docs/TSID.md](docs/TSID.md).

## Durability & retention

- Imported samples are queryable immediately but live only in memory until
  the next flush — a crash loses at most `-inmemoryDataFlushInterval`
  (default 5s) of data. **There is no WAL** (deliberate MVP cut; the flush
  interval *is* the durability window).
- `-retentionPeriod=7d` drops whole day partitions whose entire day is
  older than the retention period, checked at startup and hourly.
  Day-granular, so up to ~24h of slack — the same trade-off VictoriaMetrics
  makes with month-sized partitions. Default `0` keeps data forever.
- The series registry lives in `series/indexdb/` as append-only segments —
  each flush writes only the series registered since the last flush (no
  more full-file `names.json` rewrite). It is still not pruned by retention
  in the MVP; it grows with the total number of distinct label sets ever
  seen. Restart replays the segments to rebuild the in-memory indexes.

See [PLAN.md](PLAN.md) for the full MVP plan, VictoriaMetrics source study list, and implementation slices.
