<h1 align="center">
  <img src="https://raw.githubusercontent.com/ravendb/ravendb/HEAD/docs/logoBlue.png" alt="RavenDB" height="72"/>
  <br/>
  Raven.Bench — RavenDB Knee-Finding Benchmark
</h1>

Raven.Bench is a focused load generator for RavenDB that ramps concurrency in a closed loop to automatically detect the “knee” — the point where throughput gains flatten while latency and/or errors start climbing. It’s designed to answer: “How far can we push this setup reliably?” and to plug neatly into stress/perf benches and CI pipelines.

This README is written for engineers who don’t need all the internals — just a clear way to build, run, and integrate results.

**Highlights**
- Closed-loop ramp: `C = start .. end x factor` until knee.
- Two transports: `raw` HTTP and `client` (official RavenDB .NET client).
- Compression modes: identity, gzip, zstd (via client), brotli/deflate (raw).
- Workload mixes: `--reads/--writes/--updates` by weights or percents.
- Key distributions: `uniform`, `zipfian`, `latest`.
- Metrics per step: throughput, p50/p90/p95/p99 (raw and RTT-normalized), error rate, bytes in/out, client CPU, network utilization, and server metrics (when available).
- Knee rule (default): Δthroughput < 5% AND Δp95 > 20% (or errors > 0.5%).
- Structured outputs: JSON summary and per-step CSV.

Note: v0 implements closed-loop only and very limited read scenarios (it was designed to study full writes bottlenecks).

**Why It Works**
- Closed-loop control keeps request rate bounded by actual completions, preventing client-side overload that hides the real limit.
- The ramp grows concurrency geometrically (e.g., x2) to quickly traverse the performance curve.
- A startup calibration measures lightweight endpoints to estimate baseline RTT, enabling RTT-normalized latencies that separate network/stack overhead from load-induced queuing.
- Knee detection flags the last “safe” step before saturating a bottleneck (network/client CPU/server queues/etc.).

**What It’s Not**
- Not a functional test tool; it’s a controlled throughput/latency probe.
- Not an OLTP simulator; payloads are synthetic and simple by design.

**Getting Started**
- Requirements
  - .NET 8 SDK
  - A reachable RavenDB server and database (dev or prod-like). Examples below use `http://localhost:8080` and database `ycsb`.
  - `dotnet build RavenBench.sln -c Release`

**Usage**
- Basic shape
  - `Raven.Bench run --url <server> --database <db> [options]`
- Common options
  - `--reads/--writes/--updates <weight|percent>`: define mix. Examples: `--reads 75 --writes 25` or `--reads 3 --writes 1`.
  - `--warmup <duration>` and `--duration <duration>`: per-step timing, e.g., `20s`, `60s`.
  - `--distribution <uniform|zipfian|latest>`: key selection strategy (default: `uniform`).
  - `--doc-size <bytes|KB|MB>`: payload size for writes/updates (default: `1KB`).
  - `--http-version <auto|1.1|2|3>` and `--strict-http-version`: version negotiation and enforcement.
  - `--transport <raw|client>` and `--compression <identity|gzip|zstd|br|deflate>`:
    - `raw` uses HTTP directly; identity/gzip/br/deflate supported.
    - `client` uses RavenDB .NET client; identity/gzip/zstd supported (zstd recommended for realistic runs).
  - `--concurrency <start..endxfactor>`: geometric ramp (default: `8..512x2`).
    - Examples: `--concurrency 8..32x2` (runs at C=8, 16, 32), `--concurrency 16..128x2` (runs at C=16, 32, 64, 128)
  - `--preload <N>`: pre-insert documents to grow keyspace before the ramp.
  - `--out <file.json>` and `--out-csv <file.csv>`: write structured results.
    - `--verbose`: aggregate and print a summary of top error messages.
- Expert options
  - `--max-errors <percent>`: stop early if error rate exceeds this per step (default: `0.5%`).
  - `--knee-rule dthr=<percent>,dp95=<percent>`: threshold deltas for knee detection (default: `5%,20%`).
  - `--latencies <normalized|raw|both>`: which latencies to print to console.
  - SNMP telemetry (opt-in server monitoring):
    - `--snmp-enabled`: enable SNMP collection (default: false)
    - `--snmp-profile <minimal|extended>`: metric profile (default: minimal, 4 metrics; extended adds IO/load/request counters, 14 metrics)
    - `--snmp-port <int>`: SNMP agent port (default: 161)
    - `--snmp-interval <duration>`: poll interval (default: 250ms)
    - `--snmp-timeout <duration>`: query timeout (default: 5s)
    - See [docs/snmp-metric-catalog.md](docs/snmp-metric-catalog.md) for metric details and troubleshooting
  - `--network-limited` and `--link-mbps <double>`: annotate verdicts for known link speeds.
  - `--raw-endpoint <path-with-{id}>`: with `--transport raw`, test a custom endpoint (e.g., `/databases/db/docs?id={id}`).
  - `--tp-workers/--tp-iocp <int>`: adjust ThreadPool minimums (defaults are high to avoid client-side starvation).


**Quick Starts**
- Expose the hose (identity raw HTTP)
  - `Raven.Bench run --url http://localhost:8080 --database ycsb --reads 75 --writes 25 --distribution uniform --transport raw --compression identity --mode closed --concurrency 8..512x2 --duration 60s --out results.json --out-csv steps.csv`
- Realistic client with compression
  - `Raven.Bench run --url http://localhost:8080 --database ycsb --transport client --compression zstd --reads 75 --writes 25 --concurrency 8..1024x2 --duration 60s --latencies both`
- Zipfian reads and small docs
  - `Raven.Bench run --url http://localhost:8080 --database ycsb --reads 90 --writes 10 --distribution zipfian --doc-size 512B --duration 45s`
- HTTP/3 (strict) or auto negotiate
  - Strict HTTP/3: `--http-version 3 --strict-http-version`
  - Negotiable HTTP/2: `--http-version http2`
  - Auto (default): `--http-version auto`

**Workloads and Distributions**
- Mix
  - Provide `--reads/--writes/--updates` as either weights or percents; values normalize to 100%.
  - Reads are by id when the keyspace has data; inserts grow the keyspace; updates target an existing id.
- Key distributions for reads.
  - `uniform`: equal probability across existing keys.
  - `zipfian`: favors smaller (older) keys.
  - `latest`: favors the most recently inserted portion of the keyspace.

**HTTP Version and Compression**
- Version negotiation
  - Default `--http-version auto` probes HTTP/3, then 2, then 1.1.
  - Use `--strict-http-version` to fail if the requested version isn’t available.
- Compression
  - Raw transport: identity, gzip, brotli, deflate (zstd not supported by .NET decompression).
  - Client transport: identity, gzip, zstd (recommended for realistic client measurements).

**Outputs and Integrations**
- Console report
  - Per-step tables with throughput, error rate, client CPU, network utilization, and server metrics (if accessible).
  - SNMP metrics included when `--snmp-enabled` (gauge metrics: CPU, memory, load; rate metrics: IO ops/sec, requests/sec).
  - Knee panel and a one-line Verdict.
- JSON summary
  - Use `--out results.json` to write a structured `BenchmarkSummary` with options, steps, knee, verdict, HTTP version, compression, and calibration points.
  - SNMP metrics (all gauges and computed rates) included if enabled.
- CSV per-step
  - Use `--out-csv steps.csv` to write per-step metrics, including both raw and normalized latencies.
  - SNMP metrics included when enabled (all profile metrics with per-second rates for counters).- CI / stress benches
  - Parse JSON to extract knee concurrency and throughput, fail builds beyond thresholds, or chart historical trends.
  - Example (jq): `jq -r '.Knee | {C: .Concurrency, Thr: .Throughput, p95: .Raw.P95, p95n: .Normalized.P95}' results.json`
  - Compare CSV across runs to validate regressions after changes.

**Interpreting Results**
- Knee indicates the last reliable step. Past knee, numbers get unstable and are flagged as such.
- Verdicts
  - `network-limited`: high link utilization near configured link speed; consider compression or faster NIC.
  - `client-limited (CPU)`: generator CPU-bound; scale out clients.
  - `unknown`: server attribution needs more counters; see server metrics.
- Normalized vs raw latencies
  - Normalized subtracts an RTT baseline from startup calibration, better isolating load-induced queuing.

**Troubleshooting**
- Connection or 404 errors during calibration
  - Check `--url`, database name, and RavenDB version. For older servers, some endpoints may differ.
- Server metrics show N/A
  - Server counters come from admin/debug endpoints accessed via a RavenDB client. In secured environments, ensure proper certificates/permissions or run in dev mode.
- SNMP validation fails
  - Ensure RavenDB server has `Monitoring.Snmp.Enabled=true` in settings.json and firewall allows UDP port 161.
  - Test connectivity: `snmpwalk -v2c -c ravendb <server-host> .1.3.6.1.4.1.45751`
  - See [docs/snmp-metric-catalog.md](docs/snmp-metric-catalog.md) for detailed troubleshooting.
- High errors early, HTTP/1.x
  - Socket exhaustion can hit at low C with HTTP/1. Consider HTTP/2 or HTTP/3, or decrease step concurrency.
- Identity runs hit network limit fast
  - That’s expected; identity exposes the “hose.” Use client + zstd for realistic production behavior.

**Development**
- Repo structure
  - App: `src/RavenBench` (CLI in `Cli/`, runner in `BenchmarkRunner.cs`, transports in `Transport/`, metrics in `Metrics/`).
  - Tests: `tests/RavenBench.Tests`.
- Build and test
  - `dotnet build -c Release`
  - `dotnet test`
- Running locally
  - `dotnet run --project src/RavenBench -- run --url http://localhost:8080 --database ycsb --reads 75 --writes 25`

**Notes and Limitations**
- v0 supports `--mode closed` only.
- zstd on raw transport is not supported; use `--transport client --compression zstd`.
- Server-side attribution beyond basic CPU/memory/IO requires more counters and will improve over time.
