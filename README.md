# marchimetrics

A minimal time-series database built for learning — the marchilogs approach applied to metrics: read [VictoriaMetrics](https://github.com/VictoriaMetrics/VictoriaMetrics) source, keep the LSM DNA, and cut everything that is not needed to **ingest → store → query time series**.

Single process, day partitions, immutable parts, label-set → SeriesID index, and a small `query_range` subset. No cluster, no PromQL functions, no wire-format compatibility.

## Quickstart

```bash
go run ./cmd/marchimetrics -storageDataPath=/tmp/mm-data
# or: docker compose up --build
```

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

A crash loses at most one flush interval of samples; everything flushed
survives `kill -9`. Design notes: [docs/TSID.md](docs/TSID.md).

See [PLAN.md](PLAN.md) for the full MVP plan, VictoriaMetrics source study list, and implementation slices.
