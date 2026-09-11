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

The results directory is not cleaned. A rerun writes a new timestamped set of five files and
leaves the earlier set in place.

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
