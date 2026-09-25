# The vector benchmark

RavenDB against PostgreSQL 17 with pgvector 0.8. The question: what is the latency histogram at a matched recall, and how many queries per second does each product sustain?

## Prerequisites

- The .NET 10 SDK on the client (`dotnet` on PATH, or under `~/.dotnet`).
- The target: the external RavenDB server for `ravendb`, or Docker for the containerized `ravendb-7` and `pgvector` targets when no endpoint answers.
- For pgvector, a role that may create databases: the harness creates the fresh run database through the given connection. Without that right, create the database on the database host and pass `--database`. The server must also have one free connection per reader, since the pool holds one connection per in-flight statement; the harness refuses by name when `max_connections` is too low, and the compose service sets it for the scenario's reader count.
- Disk for the set under the scenario's `DataDirectory` (outside the repository). The raw size of a set is its vector count times its dimensions times 4 bytes, before any index: 1M vectors of 768 dimensions is about 3 GB, and the 1536-dimension set is about 6 GB.
- Optional: node_exporter on the database host, for server CPU and memory on every step.

## The one command

```
./benchmarks/vector/run.sh --target <ravendb|ravendb-7|pgvector>
```

The script is endpoint-driven. It probes the target's endpoint first. It starts a container from this folder's compose file only when no `--url` is given and the default endpoint does not answer. A caller-supplied `--url` is used as given and starts nothing. Every option other than `--target` is forwarded to the `vector` command, and every option that sets a scenario key is recorded in the result under `Vector.Overrides`.

| Target | Default endpoint | Started from compose |
|---|---|---|
| `ravendb` | http://localhost:8081 | never |
| `ravendb-7` | http://localhost:8087 (`RAVENDB7_PORT`) | `ravendb-7` |
| `pgvector` | postgresql://bench:bench@localhost:5432/bench (`PGVECTOR_PORT`) | `pgvector` |

The `pgvector` service publishes 5432 like the ycsb `postgresql` service, so only one of them runs at a time unless `PGVECTOR_PORT` moves it. The compose project name (`COMPOSE_PROJECT_NAME`) decides the container names, so two runs on one host use two project names and two ports.

Before the load, the run checks that available memory and free disk under the data directory each hold twice the raw base (vectors x dimensions x 4 bytes), and it stops by name when either is short.

The shipped scenario runs the plan's default set, cohere-768-1m. A small set for a quick run is any set with a cap on its base vectors, for example:

```
./benchmarks/vector/run.sh --target ravendb --dataset glove-100-angular --vector-count-cap 20000
```

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
- `recall`: recall@k and queries per second at the low, default and high effort settings, each named by the product's own knob (`numberOfCandidates` for RavenDB, `hnsw.ef_search` for pgvector). The row names the lowest setting that reached `RecallThreshold`, or states that none did. Each point also records the rows the queries returned and how many queries returned fewer than k; recall divides by k either way.
- `readers`: `Readers` concurrent readers at the setting the recall row named. A closed-loop step finds the sustained rate, then a fixed-rate step at that rate measures latency from the scheduled time, p50 to p99.99. When no setting reached the threshold, the run uses the high setting and says so in `EffortStatement`. A step where the load host saturated carries the client-bound marking and must not be published.
- `filtered`: the same search with an equality filter that keeps `FilterSelectivity` of the loaded set. The labels are drawn with the scenario seed. The truth is the exact top k within the label. `RowCountPerQuery` records the rows every query returned, because an index can return fewer than k under a filter. For pgvector the statement also names `hnsw.iterative_scan` and `hnsw.max_scan_tuples` as the server reports them, since they decide whether a filtered HNSW scan keeps looking past its first candidates.
- `under-insert`: queries at `UnderInsertQueryRate` while `InsertRate` vectors per second are inserted from a slice of the set that is never loaded and never a query. Each query's truth is the loaded base plus every insert acknowledged before that query was sent. For pgvector that truth is exact, because the HNSW index is updated inside the inserting transaction. For RavenDB it is an approximation, because RavenDB acknowledges a write before its vector index contains it; `TruthStatement` names the rule per target. The row reports query p99, that recall, and the quiet recall at the same setting.

## The cross-check

For pgvector on `CrossCheck.Dataset` (glove-100-angular), the `recall` row also carries `CrossCheck`. ann-benchmarks.com publishes only IVFFlat pgvector points for that set, so the check compares like with like: after the other runs, the harness replaces the HNSW index with the published index kind and build options (`PublishedIndexKind`, `PublishedBuildOptions`), searches at the published setting (`PublishedSearchValue` for the kind's knob, `ivfflat.probes`), and scores against the loaded base plus every acknowledged insert. The row states the published figure, its source, the published settings, the index definition as the server reports it, the tolerance, and "near" or "not near". The published series is captured in `evidence/` and pinned by `EvidenceSha256`, so a reader can check the figure. The published figure is for the whole set, so a run with `VectorCountCap` reports the check as skipped. Run it with:

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
