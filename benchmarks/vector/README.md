# The vector benchmark

RavenDB against PostgreSQL 17 with pgvector 0.8 and Elasticsearch 9.5. The question: what is the latency histogram at a matched recall, and how many queries per second does each product sustain?

## Prerequisites

- The .NET 10 SDK on the client (`dotnet` on PATH, or under `~/.dotnet`).
- The target: the external RavenDB server for `ravendb`, or Docker for the containerized `ravendb-7`, `pgvector` and `elasticsearch` targets when no endpoint answers.
- For pgvector, a role that may create databases: the harness creates the fresh run database through the given connection. Without that right, create the database on the database host and pass `--database`. The server must also have one free connection per reader, since the pool holds one connection per in-flight statement; the harness refuses by name when `max_connections` is too low, and the compose service sets it for the scenario's reader count.
- Disk for the set under the scenario's `DataDirectory` (outside the repository). The raw size of a set is its vector count times its dimensions times 4 bytes, before any index: 1M vectors of 768 dimensions is about 3 GB, and the 1536-dimension set is about 6 GB.
- Optional: node_exporter on the database host, for server CPU and memory on every step.

## The one command

```
./benchmarks/vector/run.sh --target <ravendb|ravendb-7|pgvector|elasticsearch>
```

The script is endpoint-driven. It probes the target's endpoint first. It starts a container from this folder's compose file only when no `--url` is given and the default endpoint does not answer. A caller-supplied `--url` is used as given and starts nothing. Every option other than `--target` is forwarded to the `vector` command, and every option that sets a scenario key is recorded in the result under `Vector.Overrides`.

| Target | Default endpoint | Started from compose |
|---|---|---|
| `ravendb` | http://localhost:8081 | never |
| `ravendb-7` | http://localhost:8087 (`RAVENDB7_PORT`) | `ravendb-7` |
| `pgvector` | postgresql://bench:bench@localhost:5432/bench (`PGVECTOR_PORT`) | `pgvector` |
| `elasticsearch` | http://localhost:9200 (`ELASTICSEARCH_PORT`) | `elasticsearch`, a fresh cluster on every run without `--url` |

The `pgvector` service publishes 5432 like the ycsb `postgresql` service, so only one of them runs at a time unless `PGVECTOR_PORT` moves it. The compose project name (`COMPOSE_PROJECT_NAME`) decides the container names, so two runs on one host use two project names and two ports.

Before the load, the run checks that available memory and free disk under the data directory each hold twice the raw base (vectors x dimensions x 4 bytes), and it stops by name when either is short.

The shipped scenario runs the plan's default set, cohere-768-1m. A small set for a quick run is any set with a cap on its base vectors. The under-insert slice, `InsertRate` times `Warmup` plus `Duration`, is held out of that cap and must leave vectors to load, so a small cap also lowers the insert rate, for example:

```
./benchmarks/vector/run.sh --target ravendb --dataset glove-100-angular --vector-count-cap 20000 --insert-rate 100
```

## Elasticsearch

The `elasticsearch` service runs `elasticsearch:9.5.3` as one node with security off, like the other two services, and the self-generated trial licence. The heap is the image default, half the host memory, unless `ELASTICSEARCH_JAVA_OPTS` passes JVM options such as a smaller heap for a shared host; the result records it as `jvm.mem.heap_max_in_bytes`. A cluster gets one 30-day trial, so without `--url` the script starts a fresh cluster in its own compose project on every run and removes it and its data on exit; a port that already answers is refused rather than reused. `ELASTICSEARCH_DATA_DIR` names a host directory for the data (the script makes a fresh subdirectory per run); unset, a named volume holds it. Elasticsearch refuses to allocate a shard when its data disk is above the disk watermark, so on a nearly full root disk point `ELASTICSEARCH_DATA_DIR` at another disk rather than change the watermark.

The target talks to the REST API over raw HTTP. The load creates a fresh index with one shard, no replicas and `index.translog.durability=request`, bulk-loads it through `_bulk`, refreshes, and returns when the count matches and a kNN query answers. A search asks for ids only. The mapping similarity follows the set's metric: `cosine`, `l2_norm`, or `max_inner_product` for dot. Round one does not force-merge.

The scenario key `ElasticsearchIndexKind` (`--elasticsearch-index-kind`) is the `index_options.type` the benchmark always sends, so the row never depends on the default of a given version. Every other index option stays at the vendor default.

- `hnsw`: float32 HNSW. The knob is `num_candidates`.
- `bbq_hnsw`: BBQ (1-bit quantized vectors, rescored) over HNSW, the kind Elasticsearch 9.5 picks itself for this field shape. The knob is `num_candidates`.
- `bbq_disk`: DiskBBQ. The knob is `visit_percentage`. It needs an enterprise or trial licence; under any other licence the run fails by name before the index is created and names `xpack.license.self_generated.type=trial`.

The effort families are `Efforts.elasticsearch-<kind>` in the scenario. The default of `num_candidates` is the vendor's `min(1.5 * k, 10000)`, 15 at the shipped k of 10; the default of `visit_percentage` is the vendor's figure of about 1% per shard for every million vectors, 1 for the shipped set. `visit_percentage` takes a fraction, so its low setting is 0.5. The Elasticsearch settings record `index_kind.sent`, `index_kind.vendor_default` (the kind an unset field resolves to, probed on the server before the load), every resolved `mapping.index_options.*` value with `.source` saying set by the benchmark or vendor default, the index settings, `segments.count` after the load, the heap and the licence type, status and expiry.

For Elasticsearch, `filtered` passes the label in the kNN `filter` clause, so the filter applies during the search. `under-insert` truth is an approximation: an acknowledged write becomes searchable only after the next refresh, and `TruthStatement` names `index.refresh_interval` as the server reports it. The stored size is `_stats primaries.store.size_in_bytes`.

## Quantized rows

`VectorStorage` and `RowLabel` name the quantization on every row, so a quantized row never reads as a float32 row. The Elasticsearch `bbq_hnsw` and `bbq_disk` rows are quantized; the RavenDB and pgvector rows are float32.

## The three Docker situations

- Docker is not installed: the script says so and names the compose command to run on another host, then `--url`.
- Docker is installed but the daemon is not reachable: the script says so and names the `docker` group, sudo or `--url`.
- The compose start fails or the container does not become ready: the script says which target and how long it waited.

## The database host is not the client

Run the target and node_exporter on the database host, and the script on the client:

```
# database host
docker compose -f benchmarks/vector/docker-compose.yml up -d pgvector
docker run -d --net=host --pid=host -v /:/host:ro,rslave quay.io/prometheus/node-exporter:latest --path.rootfs=/host

# client
./benchmarks/vector/run.sh --target pgvector --url postgresql://bench:bench@db-host:5432/bench --node-exporter-url http://db-host:9100/metrics
```

For `ravendb-7`, set `RAVENDB_HOST=db-host` on the database host before `up`, so the server advertises a URL the client can reach.

## What each row means

Every run leaves `results/<target>-<run id>-<run>.json` with the resolved scenario, the overrides, the target, its version and image digest, the durability parity setting, the product settings as the server reports them, the per-step table, the HdrHistogram paths and the machine fingerprint.

- `load`: wall time from the first vector sent to an index that answers queries, the peak host memory node_exporter saw during the load, and the stored size by the product's own statistic.
- `recall`: recall@k and queries per second at the low, default and high effort settings, each named by the product's own knob (`numberOfCandidates` for RavenDB, `hnsw.ef_search` for pgvector, `num_candidates` or `visit_percentage` for Elasticsearch). The row names the lowest setting that reached `RecallThreshold`, or states that none did. Each point also records the rows the queries returned and how many queries returned fewer than k; recall divides by k either way.
- `readers`: `Readers` concurrent readers at the setting the recall row named. A closed-loop step finds the sustained rate, then a fixed-rate step at that rate measures latency from the scheduled time, p50 to p99.99. When no setting reached the threshold, the run uses the high setting and says so in `EffortStatement`. A step where the load host saturated carries the client-bound marking and must not be published.
- `filtered`: the same search with an equality filter that keeps `FilterSelectivity` of the loaded set. The labels are drawn with the scenario seed. The truth is the exact top k within the label. `RowCountPerQuery` records the rows every query returned, because an index can return fewer than k under a filter. For pgvector the statement also names `hnsw.iterative_scan` and `hnsw.max_scan_tuples` as the server reports them, since they decide whether a filtered HNSW scan keeps looking past its first candidates.
- `under-insert`: queries at `UnderInsertQueryRate` while `InsertRate` vectors per second are inserted from a slice of the set that is never loaded and never a query. Each query's truth is the loaded base plus every insert acknowledged before that query was sent. For pgvector that truth is exact, because the HNSW index is updated inside the inserting transaction. For RavenDB it is an approximation, because RavenDB acknowledges a write before its vector index contains it; `TruthStatement` names the rule per target. The row reports query p99, that recall, and the quiet recall at the same setting.

## The cross-check

For pgvector on `CrossCheck.Dataset` (glove-100-angular), the `recall` row also carries `CrossCheck`. ann-benchmarks.com publishes only IVFFlat pgvector points for that set, so the check compares like with like: after the other runs, the harness replaces the HNSW index with the published index kind and build options (`PublishedIndexKind`, `PublishedBuildOptions`), searches at the published setting (`PublishedSearchValue` for the kind's knob, `ivfflat.probes`), first inserts every held-out slice vector the under-insert run did not insert, so the index holds the whole train split, and scores against the set's published neighbours (`TruthSource`, `SearchedVectors`). The published IVFFlat point is not the pgvector default build: round one builds HNSW with no `WITH (...)` options, and the IVFFlat point uses `lists=1000`. The row states this in `DefaultBuild`, derived from the published index kind and options against the round-one index definition as the server reports it. The row states the published figure, its source, the published settings, the index definition as the server reports it, the tolerance, and "near" or "not near". The IVFFlat build holds its `lists` centroids in `maintenance_work_mem`, which a `lists=1000` build needs beyond the shipped 64 MB, so `CrossCheck.BuildSession` names the session settings applied only during that build; they are reset after it, recorded in the row as `BuildSession`, and never in force for round one. The published series is captured in `evidence/` and pinned by `EvidenceSha256`, so a reader can check the figure. The published figure is for the whole set, so a run with `VectorCountCap` still fills every field but states the verdict as skipped. Run it with:

```
./benchmarks/vector/run.sh --target pgvector --dataset glove-100-angular
```

## Truth

Recall is always scored against product-neutral truth: the set's published neighbours, or exact float32 brute force computed outside any product and cached next to the data. The insert slice is removed from that truth for the quiet runs, so `TruthDepth` must leave at least k neighbours per query.

Round one stores and searches full float32 vectors on both products. Index build parameters stay at each vendor's default and are recorded as the server reports them. The compose file carries no tuning of our own.

## Costs

The cost of a run follows from the scenario: the load writes the whole set once (or `VectorCountCap` vectors of it; a capped set gets brute-force truth over the capped base, computed once and cached), recall runs `QueryCount` queries three times, readers and under-insert each run `Warmup + Duration` per step, the pgvector pool opens one connection per reader, the cross-check builds a second index over the whole set, and the brute-force truth of a set without a query split reads the base once per query batch. No figure measured on a shared development box belongs in this file or in the defaults.

## Adding a dataset

A set implements `IVectorDataset`: base vectors, query vectors with true neighbours, the metric and the dimensions, from files pinned by URL and SHA-256. A set with a published split goes into `VectorSets.Published`; a set without one derives from `HeldOutVectorDataset`, which holds the queries out of the load and computes the truth. Name it in `Dataset`.

## Adding a target

A target implements `IVectorTarget` over a transport that serves `VectorSearchOperation`: the load into a fresh store, the insert operation, the product's effort knob, its stored size and the settings it reports. It refuses a metric it cannot serve in its constructor, before any load. Add its name to `VectorRunner.BuildTarget`, its effort settings to `scenario.json`, and a case to `run.sh`.
