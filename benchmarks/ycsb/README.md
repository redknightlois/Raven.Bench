# ycsb: RavenDB versus PostgreSQL, MongoDB Community and DocumentDB

This folder runs the YCSB document benchmark from one command. The benchmark inserts a seeded
keyspace, then runs the four YCSB workloads against it and writes one result JSON per run. The
same code, scenario and document generator drive every target, so a row can be compared only
against another row with a matching machine fingerprint.

## Prerequisites

- Docker, for the three containerized targets.
- The .NET 10 SDK.
- A reachable RavenDB server for the `ravendb` target. The local server is
  `http://localhost:8081`. Point the benchmark only at a server you intend to use; no other
  RavenDB instance on this machine is touched.

Nothing must be edited before the first run.

## The one command

```
./benchmarks/ycsb/run.sh --target <ravendb|postgresql|mongodb|documentdb>
```

The command can be run from any working directory. It starts the target from this folder's
`docker-compose.yml` when the target's endpoint does not already answer, waits until the endpoint
answers, runs the sequence, and stops the container it started. A container that already answers
is reused and is not stopped. RavenDB is external: the script starts no RavenDB container.

Every extra option is forwarded to the `ycsb` command unchanged, for example:

```
./benchmarks/ycsb/run.sh --target mongodb --seed 7 --doc-count 1000 --duration 5s
```

The forwarded options are the ones the command accepts: `--scenario`, `--url`, `--database`,
`--seed`, `--doc-count`, `--doc-size`, `--step`, `--distribution`, `--warmup`, `--duration`,
`--output-prefix` and the rest. A caller-supplied `--scenario`, `--url`, `--database` or
`--output-prefix` replaces the script default instead of being appended a second time.

The script prints the result path when the run ends. By default the five results are written as
`benchmarks/ycsb/results/<target>-<timestamp>-<run>.json`, where `<run>` is `load`, `C`, `A`, `B`
or `insert-stream`.

## What each row means

| Run | Mix | What it does |
|---|---|---|
| load | 100% bulk insert | Fills the keyspace through the target's bulk path. Its throughput is the load row and is never merged into a workload row. |
| C | 100% read | Reads one document by id. |
| A | 50% read, 50% update | Reads one document by id, or updates one field of one document. |
| B | 95% read, 5% update | The same two operations at a read-mostly ratio. |
| insert-stream | 100% insert | Inserts one document per operation, one commit each. This is the fsync path. |

Each result JSON holds the resolved scenario, the target product and server version, the image
reference and digest of the target that actually served the run, the durability parity setting,
the per-step table, the machine fingerprint, and the paths of the per-step HdrHistogram and CSV
files.

## Rerunning

Each invocation uses a fresh database named `ycsb_<target>_<timestamp>`, so a second invocation
never loads into a keyspace that already holds the `bench/` ids. For PostgreSQL the script creates
that database in the local container before the run, because the transport creates the table but
does not create the database; a custom `--url` therefore also needs a `--database` that you create
yourself. Pass `--database` to run against a database you manage; that database must start with an
empty `bench/` keyspace.

The results directory is not cleaned. A rerun writes a new timestamped set of five files and
leaves the earlier set in place.

## How to add a target

1. Add the target's name to the `Target` key of `benchmarks/ycsb/scenario.json`, and to the
   documentation comment on `YcsbScenario.Target`.
2. Add a case to the target dispatch in `src/RavenBench/Ycsb/YcsbRunner.cs`. Dispatch happens
   before any RavenDB-only setup, so the new target never negotiates HTTP. The dispatch builds the
   transport from `--url` and records the target's durability parity setting in the `RunContext`.
3. Implement `IYcsbTransport` in `src/RavenBench.Core/Transport/`. The contract is read by id,
   insert one document, update one field, bulk load, `PutAsync`, `EnsureDatabaseExistsAsync`,
   `GetDocumentCountAsync`, `ProductName`, `GetServerVersionAsync` and `ReportsWireBytes`.
4. Add the target's local endpoint and, when it runs in a container, a service to this folder's
   `docker-compose.yml`. The readiness probe is a TCP connect to the endpoint's host and port.
5. Extend the dispatch error message so the unknown-target error names the new target.

The MongoDB and DocumentDB targets share one transport. The PostgreSQL target uses
`Apex.PgClient`; Npgsql is not used in the measured path.
