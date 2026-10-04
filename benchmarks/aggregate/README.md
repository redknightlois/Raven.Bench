# The aggregate benchmark

RavenDB map-reduce indexes against MongoDB 8.0 Community aggregation pipelines. The question: over grouped counts and sums while writes keep coming, what throughput and latency does each product sustain, and how fresh is each answer?

## Prerequisites

- The .NET 10 SDK on the client (`dotnet` on PATH, or under `~/.dotnet`).
- The target: the external RavenDB server for `ravendb`, or Docker for the containerized `ravendb-7` and `mongodb` services when no endpoint answers.
- A writable `dataDirectory` outside the repository. The run writes the emitted set's manifest there (count and checksum per scenario), and refuses a directory inside the repository.
- Optional: node_exporter on the database host, for server CPU during every step and server CPU and disk writes during the build.

## The one command

```
./benchmarks/aggregate/run.sh --target <ravendb|ravendb-7|mongodb|mongodb-indexed>
```

The script is endpoint-driven, exactly as the vector folder's script. It probes the target's endpoint first. It starts a container from this folder's compose file only when no `--url` is given and the default endpoint does not answer. A caller-supplied `--url` is used as given and makes no Docker call. Every option other than `--target` is forwarded to the `aggregate` command, and every option that sets a scenario key is recorded in the result under `Aggregate.Overrides`, beside `Aggregate.ResolvedScenario`.

- `ravendb`: http://localhost:8081, never started by the script.
- `ravendb-7`: http://localhost:8087 (`RAVENDB7_PORT`), compose service `ravendb-7`.
- `mongodb` and `mongodb-indexed`: mongodb://localhost:27017 (`MONGODB_PORT`), compose service `mongodb`. The two targets are one server and one transport; `mongodb-indexed` differs only in creating the indexes in `src/RavenBench.Core/Aggregate/Indexes/mongodb`. Only `filtered-group` has a supporting index (`category_1_region_1_amount_1`), because its pipeline starts with a `$match` on `category`. The `count-by-category` and `sum-by-region` pipelines start with `$group` over the whole collection, which no index serves, so those two shapes have no index on either MongoDB target.

The `mongodb` service publishes 27017 like the ycsb `mongodb` service, so only one of them runs at a time unless `MONGODB_PORT` moves it.

The shipped scenario holds the plan's defaults (10M documents of 1 KB, 100 categories, 10,000 regions). A small run for a quick check sets the document count on the command line, for example:

```
./benchmarks/aggregate/run.sh --target mongodb-indexed --documents 20000 --write-rate 200 --duration 10s
```

## The three Docker situations

- Docker is not installed: the script says so and names the compose command to run on another host, then `--url`.
- Docker is installed but the daemon is not reachable: the script says so and names the `docker` group, sudo or `--url`.
- The compose start fails or the container does not become ready: the script says which target and how long it waited.

## The database host is not the client

Run the target and node_exporter on the database host, and the script on the client:

```
# database host
docker compose -f benchmarks/aggregate/docker-compose.yml up -d mongodb
docker run -d --net=host --pid=host -v /:/host:ro,rslave quay.io/prometheus/node-exporter:latest --path.rootfs=/host

# client
./benchmarks/aggregate/run.sh --target mongodb --url mongodb://db-host:27017 --node-exporter-url http://db-host:9100/metrics
```

For `ravendb-7`, set `RAVENDB_HOST=db-host` on the database host before `up`, so the server advertises a URL the client can reach. When node_exporter does not answer, the run continues and every server figure names why it is unavailable; it never records a zero in its place.

## What each row means

Every run leaves `results/<target>-<run id>-<run>.json` in the extended summary format of the ycsb and vector runs: the resolved scenario and the overrides, the target, its version and image digest, the durability parity setting, the emitted set (`DataSet`: count, checksum, distribution and cardinalities), the per-step table, the HdrHistogram paths, the machine fingerprint, and the server columns with their source.

- `build`: loads the seeded set, then creates the RavenDB map-reduce indexes (`src/RavenBench.Core/Aggregate/Indexes/ravendb`, one per shape) or the `mongodb-indexed` supporting indexes, and waits until every shape answers and, on RavenDB, every index reports non-stale in the server's index statistics. It reports the wall time split into load and index time, server CPU and disk bytes written during the build (node_exporter, host-wide), and the on-disk size by the product's own statistic. A figure that cannot be read carries `Unavailable` with the reason.
- `count-by-category`: documents per category over the whole collection, top `countTopN`.
- `sum-by-region`: sum of `amount` per region, top `regionTopN`.
- `filtered-group`: sum of `amount` per region for one category, top `regionTopN`. The category is the emitted category whose share of the set is nearest `filterSelectivity`; the result names it in `FilterCategory`.
- Each of the three query rows runs a closed loop at `concurrency` to find the ceiling, then the fixed-rate runner at `FixedRateFraction` (0.8) of that ceiling (`FixedRate`), measuring latency from the scheduled time, p50 to p99.99. The fixed-rate latency is the latency of a server with headroom below its ceiling; a rate at the full ceiling would measure mostly queue wait. The result records the fraction beside the rate. Each step records queries per second and server CPU. A step where the load host saturated carries the client-bound marking and must not be published.
- `under-write`: count-by-category at `underWriteQueryRate`, first quiet, then under two concurrent writers for the whole step. The bulk writers update `amount` and `category` at `writeRate`, with up to `writers` updates in flight (`--writers`), because one update in flight caps the rate at one over the write latency. The probe writer moves one document at a time into the tracked category at `underWriteQueryRate`, one update per query interval, and freshness comes from it alone. The bulk writers never move a document into or out of the tracked category: they move documents only between the other categories, so the tracked count moves only with the probe. The two writers never update the same document. The run reports query p99 and throughput for both steps and each writer on its own: `Writer` for the bulk writers and `Probe` for the probe, each with the requested and the held rate (`RequestedPerSecond`, `HeldPerSecond`, acknowledged writes over the writer's measured duration), the write latency (send to acknowledgement) and `Writers`, the configured number of writers. When a writer did not hold the requested rate, its `HeldRequested` is false and `Shortfall` states by how much and why. A failed write stops the run with the error.

Every query step records `StepAnswers`: the answers and the stale answers, counted from the stale flag of each response. MongoDB never marks an answer stale, so its count is zero, present in the result. No query waits for a non-stale answer; the policy is named on every row as `QueryPolicy`, because waiting hides the cost this benchmark exists to show.

## Freshness

Freshness has one definition for every product: the time from a write's acknowledgement until the first query answer received that reflects it. It is measured from the answers, never from an index ETag, a stale flag or an assumption that a pipeline is always fresh.

The probe writer makes every probe write observable in the count-by-category answer. The tracked group is the category with the most documents after the load. The baseline is the server's own count-by-category answer for it, read before the first probe write; when that answer is stale or differs from the generated count (the collection holds documents the run did not load), the run fails with `InvalidDataException` instead of measuring against a wrong baseline. Every probe write moves one document from another category into it, one write in flight, so the tracked count after the k-th acknowledged probe write is the baseline plus k. The bulk writers never touch the tracked category, so nothing else changes its count, and a bulk write never contributes a freshness sample. Each bulk writer owns its documents and one target category: it moves a document into the target, then moves a target document back to the category just left, so its target is at most one above its start and no other category ever rises. A category is the target of fewer bulk writers than its distance below the tracked count, and a run with more writers than that room fails fast. So the tracked category stays first in the answer and any top N shows it; `Freshness.AnswersWithTrackedGroupNotFirst` counts the answers where it was not, and is zero. An answer reflects write k when its count for the tracked category is at least baseline plus k. An answer received before the acknowledgement never counts as a reflection, so no freshness value is negative. Acknowledgements and answers are timed with the monotonic clock the fixed-rate runner schedules with.

`Freshness` reports the observed writes as a distribution (p50, p90, p99, max), the observed count, and the unobserved count: writes no answer reflected before the run ended. It is its own field and is not folded into throughput or latency. Its resolution is the query interval, one over `underWriteQueryRate`.

## Costs

The cost of a run follows from the scenario: the build writes `documentCount` documents of `documentSize` bytes once and builds one index per shape, each query row runs `warmup + duration` twice (closed loop, then fixed rate), under-write runs it twice more, and the bulk writers update `writeRate` documents per second, and the probe `underWriteQueryRate`, for the second of those. The probe never reuses a document, and it takes every other document outside the tracked category, so the set needs at least twice `underWriteQueryRate` times `warmup + duration` documents outside it, and a shorter set fails before the step. The probe holds the ids of only that many documents. A step that runs past its window and uses every one stops the probe without an error: the build and query results are kept, and `Probe.StopReason` and `Probe.Acknowledged` state that the probe stopped early and how many probe writes it sent. No figure measured on a shared development box belongs in this file or in the defaults.

Without `--keep-data`, a run removes what it created and nothing else. On RavenDB, a database the run created is deleted. In a database or a MongoDB collection that existed before the run, the run deletes the documents it loaded and the indexes that did not exist before it. A target that already holds a document with an id the run loads fails before the load with `AggregateIdCollisionException`, because the load would overwrite it and the cleanup would remove it. The parity check follows the same rule for its sample.

## Adding a query shape

A shape is a `GroupedAggregateOperation`: a group field, a count or a sum over a named field, an optional typed equality or range filter, and a top N. Add its name and operation to `AggregateShapes`, a RavenDB map-reduce index whose name is the operation's `IndexName` under `src/RavenBench.Core/Aggregate/Indexes/ravendb`, and, only when the shape's pipeline starts with a `$match` or `$sort` on the index's leading key, the MongoDB index that supports it under `src/RavenBench.Core/Aggregate/Indexes/mongodb`, named after the shape. The transports translate the operation; no RQL or pipeline is written anywhere else. The runner picks the shape up from `AggregateShapes.All`, and `parity --aggregate` compares it on every product.
