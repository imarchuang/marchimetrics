# marchimetrics

A minimal time-series database built for learning — the marchilogs approach applied to metrics: read [VictoriaMetrics](https://github.com/VictoriaMetrics/VictoriaMetrics) source, keep the LSM DNA, and cut everything that is not needed to **ingest → store → query time series**.

Single process, day partitions, immutable parts, label-set → SeriesID index, and a small `query_range` subset. No cluster, no PromQL functions, no wire-format compatibility.

See [PLAN.md](PLAN.md) for the full MVP plan, VictoriaMetrics source study list, and implementation slices.
