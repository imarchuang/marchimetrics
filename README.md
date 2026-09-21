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

Imported data is queryable immediately — flush to immutable disk parts
arrives in PR2. Design notes: [docs/TSID.md](docs/TSID.md).

See [PLAN.md](PLAN.md) for the full MVP plan, VictoriaMetrics source study list, and implementation slices.
