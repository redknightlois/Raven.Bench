# ycsb: RavenDB versus PostgreSQL, MongoDB Community and DocumentDB

This folder runs the YCSB document benchmark from one command. The benchmark inserts a seeded
keyspace, then runs the four YCSB workloads against it and writes one result JSON per run. The
same code, scenario and document generator drive every target, so a row can be compared only
against another row with a matching machine fingerprint.

## Prerequisites

- The .NET 10 SDK.
- Docker, for the five containerized targets. Docker is optional on the client: a run against a
  target that already answers needs no Docker at all.
- A reachable RavenDB server for the `ravendb` target. The local server is
  `http://localhost:8081`. Point the benchmark only at a server you intend to use.

Port 8080 belongs to a different RavenDB server on this machine. The benchmark never contacts it,
and neither the script nor the compose file addresses it.

Nothing must be edited before the first run.

## The one command

```
./benchmarks/ycsb/run.sh --target <ravendb|ravendb-6|ravendb-7|postgresql|mongodb|documentdb>
```

The command can be run from any working directory. The script is endpoint-driven:

- It probes the target's endpoint first. When the endpoint already answers, the script uses it and
  starts nothing, with or without Docker on the client.
- It starts the target from this folder's `docker-compose.yml` only when the caller gave no
  `--url` and the target's default localhost endpoint does not answer. It waits until the endpoint
  answers, runs the sequence, and stops only the container it started.
- A caller-supplied `--url` is used as given. A dead caller endpoint fails the run and starts
  nothing.
- `ravendb` is the external development server. The script never starts it.

Every extra option is forwarded to the `ycsb` command unchanged, for example:

```
./benchmarks/ycsb/run.sh --target mongodb --seed 7 --doc-count 1000 --duration 5s
```

The forwarded options are the ones the command accepts: `--scenario`, `--url`, `--database`,
`--seed`, `--doc-count`, `--doc-size`, `--step`, `--distribution`, `--warmup`, `--duration`,
`--output-prefix` and the rest. A caller-supplied `--scenario`, `--url`, `--database` or
`--output-prefix` replaces the script default instead of being appended a second time.

The script prints the result path when the run ends. Every result is written as
`benchmarks/ycsb/results/<target>-<timestamp>-<run>-<shape>-<distribution>-rep<n>.json`, where
`<run>` is `load`, `C`, `A`, `B` or `insert-stream` and `<shape>` is `closed` for a ramp run or
`rate<rate>` for a fixed-rate run. The whole identity is in the name because one invocation
produces several results per run kind, and the same identity is inside each file under the `Ycsb`
block.

## The set one invocation produces

Three scenario keys name the set, beside the single-value keys:

| Key | Domain | Meaning |
|---|---|---|
| `Rates` | a list of numbers, each greater than zero; empty for none | The fixed rates to run after the closed-loop ramp. `Rate` is the one-element spelling of the same key; a scenario names one or the other, never both. |
| `Distributions` | a non-empty list of `uniform`, `zipfian` and `latest` | The distributions workload C runs under. The first value is the one every other run uses. Absent means the single `Distribution` value alone. |
| `Repetitions` | an integer of one or more | How many times every workload row is repeated. |

`--rates`, `--distributions` and `--repetitions` override them on the command line; a
command-line value wins over the file, and the file wins over nothing.

One invocation produces: one load run, then for each repetition workload C under every named
distribution, then A, B and insert-stream under the first named distribution, then one workload C
run per named rate. The count is `1 + repetitions * (distributions + 3 + rates)`. The keyspace is
loaded once: a repetition, a second distribution and a rate never reload it. The checked-in
scenario names two distributions, one rate and three repetitions, which is
`1 + 3 * (2 + 3 + 1) = 19` results.

How to pick a rate: run the invocation once, open a closed-loop C result, read the throughput of
the step the `Knee` field names, and set `Rates` to about 40 percent of it, which is the plan's
operating point. The checked-in `2000` is a starting point for this box, not a measurement of
yours; read it from your own ramp result before you publish a rate row.

Every repetition of a row is kept as its own file. Exactly one of them carries
`Ycsb.IsRowMedian: true`: the median of `Ycsb.MedianStatistic` (the highest step throughput the
result measured) over the row's repetitions. A repetition whose steps the client-bound guard marked
invalid is left out of that selection, because a client-bound number must not be published; a row
whose every repetition is invalid carries no median marking at all. With an even number of
candidates the lower of the two middle results is marked.

Each run draws its own request stream: the seed is derived from the scenario seed and the run's
identity, so two repetitions of one row are two samples while two invocations of one scenario
repeat the same streams. The derived seed is in `Options.Seed`; the scenario's own seed stays in
`Ycsb.ResolvedScenario.Seed`.

## What each row means

| Run | Mix | What it does |
|---|---|---|
| load | 100% bulk insert | Fills the keyspace through the target's bulk path. Its throughput is documents per second, counted over the documents each batch carried, and is never merged into a workload row. |
| C | 100% read | Reads one document by id. |
| A | 50% read, 50% update | Reads one document by id, or updates one field of one document. |
| B | 95% read, 5% update | The same two operations at a read-mostly ratio. |
| insert-stream | 100% insert | Inserts one document per operation, one commit each. This is the fsync path. |

C, A, B and insert-stream are fixed points of one weighted blend. `YcsbBlendFixedPointsTests`
checks that each preset resolves to the mix the run sequence holds for it.

Each result JSON holds the resolved scenario, the target product and server version, the image
reference and digest of the target that actually served the run, the durability parity setting,
the per-step table, the machine fingerprint, and the paths of the per-step HdrHistogram and CSV
files.

## What makes a step valid

Every measured step reports `ClientCpu`: the CPU this load process spent over that step's own
post-warmup measurement window, as a fraction of the load host's total capacity. `MeasuredDuration`
is the measured length of that same window, so throughput, CPU and wall time read against one clock.
A bounded fill that ends before the configured duration reports the window it measured, not the cap.

A step whose load host reached the saturation threshold carries `InvalidReason` and prints an
`INVALID` line on the console naming the step, the ycsb run and why. Such a step measures the load
host, not the server, so its number must not be published. The same threshold decides the result's
`Verdict`, which reads `client-limited (CPU)` for such a run.

Two more fields state what the harness could and could not read:

- `Ycsb.LoadedSize` on the load result: the on-disk size of what the load left behind, with the
  name of the product statistic it was read from (`SizeOnDisk.SizeInBytes` for RavenDB,
  `pg_database_size` for PostgreSQL, `dbStats.totalSize` for MongoDB and DocumentDB). A product
  that exposes none says so by name instead.
- `Ycsb.ServerColumns` on every result: which server columns this run actually collected for this
  product, derived from the steps it produced, and `Sources`, the source of each CPU and memory
  column. Without `--node-exporter-url` a RavenDB run names the columns its admin endpoints filled
  (`ravendb-debug`), and a PostgreSQL, MongoDB or DocumentDB run states that this harness has no
  server column for that product, so an absent key never reads as a lost measurement.
- `--node-exporter-url <url>` points at node_exporter on the database host (for example
  `http://db-host:9100/metrics`). It then fills `ServerCpu` and `ServerMemoryMB` on every step for
  every product, including RavenDB, with `ServerCpuSource` and `ServerMemorySource` set to
  `node_exporter` and `ServerMetricsHostWide` set, because node_exporter reads the whole host. CPU is
  the non-idle share of `node_cpu_seconds_total` over all CPUs between a scrape at the start and one
  at the end of the step (iowait and steal count as busy); memory is MemTotal minus MemAvailable. An
  endpoint that does not answer fails the run before the load. A scrape that fails mid-run leaves the
  step's columns empty and states the reason in `ServerMetricsUnavailable`. SNMP, where enabled,
  keeps its own columns and its priority in the analysis.

## Checking the four products agree

The parity check is a separate command. It is not a benchmark: it writes no result summary and
reports no throughput, latency or percentile.

```
dotnet run --project src/RavenBench -- parity \
  --ravendb-url http://localhost:8081 \
  --postgresql-url postgresql://bench:bench@localhost:5432/bench \
  --mongodb-url mongodb://localhost:27017 \
  --documentdb-url 'mongodb://bench:bench@localhost:10260/?tls=true&tlsInsecure=true' \
  --database ycsb_parity --postgresql-database bench
```

It writes a seeded sample (1,000 documents by default), then runs every typed operation of the
contract against every product: read by id, single insert and the one-field update. RavenDB is the
reference: the other products are compared against what RavenDB stored, and RavenDB itself against
what the seed says each operation must leave. The report has one row per operation and per product,
so a disagreement names the operation and the product instead of stopping at the first one. Full
agreement exits zero; any disagreement, or any product the check could not reach, exits non-zero
and names that product with its endpoint. The check deletes its own sample from each product and
says so, so the next load run does not find ids that already exist. PostgreSQL creates no database
on demand, so `--postgresql-database` names an existing one; the other three are created by the
check.

## Port table

Every service, its host port and its default endpoint. The two containerized RavenDB host ports
are overridable with `RAVENDB6_PORT` and `RAVENDB7_PORT`; their database host name for the public
server URL is `RAVENDB_HOST`, default `localhost`.

| Target | Compose service | Default endpoint | Host port |
|---|---|---|---|
| `ravendb` | none (external server) | `http://localhost:8081` | 8081 |
| `ravendb-6` | `ravendb-6` | `http://localhost:8086` | 8086 |
| `ravendb-7` | `ravendb-7` | `http://localhost:8087` | 8087 |
| `postgresql` | `postgresql` | `postgresql://bench:bench@localhost:5432/bench` | 5432 |
| `mongodb` | `mongodb` | `mongodb://localhost:27017` | 27017 |
| `documentdb` | `documentdb` | `mongodb://bench:bench@localhost:10260/?tls=true&tlsAllowInvalidCertificates=true` | 10260 |

## Docker on the client

The script touches Docker only when it must start a target. Three situations cover the client.

1. **Docker is not installed.** When the script must start a target and finds no `docker`, it says
   Docker is missing and gives the two ways out: install Docker, or start the target on another
   host from this folder's compose file and rerun with `--url <endpoint>`.
2. **Docker is installed but the daemon is not reachable.** The script prints a different message
   that names the fix: join the `docker` group or use `sudo`, or pass `--url` to a target started
   elsewhere. This message is never the "not installed" message.
3. **The database host is not the client.** The compose file is the whole recipe for the database
   host. The client runs `run.sh --target X --url <db-host endpoint>` and never touches Docker.

The two Docker messages appear only when the script must start a container. A run whose endpoint
answers, and a run whose caller supplied `--url`, print neither.

## Running the database on another host

Start the service on the database host from this folder's compose file:

```
docker compose -f benchmarks/ycsb/docker-compose.yml up -d <service>
```

For the two RavenDB services, first set `RAVENDB_HOST` to the database host's name, so the server
advertises `http://<db-host>:<host port>` and the raw transport follows the advertised topology:

```
RAVENDB_HOST=db-host docker compose -f benchmarks/ycsb/docker-compose.yml up -d ravendb-7
```

Then run the client against that host, with no Docker on the client:

```
./benchmarks/ycsb/run.sh --target ravendb-7 --url http://db-host:8087
```

PostgreSQL has no lazy database creation, so the client needs a way to create the per-run database.
The script tries the client's `psql` first, then a local container that publishes the endpoint's
port, then fails and tells the user to create the database on the database host and pass
`--database`. A client without `psql` and without Docker must therefore pass `--database` naming a
database that already exists on the host.

## Rerunning

Each invocation uses a fresh database named `ycsb_<target>_<timestamp>`, so a second invocation
never loads into a keyspace that already holds the `bench/` ids. For PostgreSQL the script creates
that database before the run, because the transport creates the table but does not create the
database. Pass `--database` to run against a database you manage; that database must start with an
empty `bench/` keyspace.

The results directory is not cleaned. A rerun writes a new timestamped set of files and leaves the
earlier set in place. The results directory is ignored by git, so results stay on the machine that
produced them.

The `ravendb` target is the shared development server, and every run leaves its database
`ycsb_ravendb_<timestamp>_<pid>` behind with the loaded keyspace in it, so an evening of runs leaves
one 100,000-document database per run. Delete them when you are done, either from the Studio at
`http://localhost:8081` or with one request per database:

```
curl -X DELETE 'http://localhost:8081/admin/databases?name=ycsb_ravendb_20260922T101500_1234&hard-delete=true'
```

## How to add a target

1. Add the target's name to the `Target` key of `benchmarks/ycsb/scenario.json`, to the
   documentation comment on `YcsbScenario.Target`, and to the `run.sh` target list.
2. Add a case to the target dispatch in `src/RavenBench/Ycsb/YcsbRunner.cs`. Dispatch happens
   before any RavenDB-only setup, so the new target never negotiates HTTP. The dispatch builds the
   transport from `--url` and records the target's durability parity setting in the `RunContext`.
3. Implement `IYcsbTransport` in `src/RavenBench.Core/Transport/`. The contract is read by id,
   insert one document, update one field, bulk load, `PutAsync`, `EnsureDatabaseExistsAsync`,
   `GetDocumentCountAsync`, `ProductName`, `GetServerVersionAsync` and `ReportsWireBytes`.
4. Add the target's local endpoint to `run.sh` and, when it runs in a container, a service to this
   folder's `docker-compose.yml`. The readiness probe is a TCP connect to the endpoint's host and
   port, except for the RavenDB targets, which wait for `/build/version` to answer: a published
   container port accepts a connection before the server behind it can serve a request.
5. Extend the dispatch error message so the unknown-target error names the new target.

The MongoDB and DocumentDB targets share one transport. The PostgreSQL target uses
`Apex.PgClient`; Npgsql is not used in the measured path.
