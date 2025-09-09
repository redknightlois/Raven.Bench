RavenBench — Knee-finding RavenDB benchmark (v0)

What it does (MVP)
- Closed-loop runner with ramped concurrency to find the knee.
- Raw HTTP (identity) and Raven Client (zstd default) transports.
- Workload mixes: define via simple numeric flags (reads/writes/updates) or legacy shorthand.
- Uniform keys, fixed document sizes.
- Records throughput, p50/p90/p95/p99, error rate, bytes in/out, client CPU, network utilization.
- Knee rule: Δthroughput < 5% and Δp95 > 20% (or errors > 0.5%).
- JSON and CSV output.

Build
- Requires .NET 8 SDK.
- To reference RavenDB 7.1 client from source, set env var `RAVENDB71_SRC` to the RavenDB repo root (e.g., `d:\Src\ravendb-71-git`). The build will use `src\Raven.Client\Raven.Client.csproj`.
- Otherwise, it falls back to the `RavenDB.Client` 7.1 NuGet package.

Run examples
- Identity first (expose the hose):
  ravenbench run --url http://localhost:10101 --database ycsb --reads 75 --writes 25 --distribution uniform --compression raw:identity --mode closed --concurrency 8..512x2 --duration 60s --out results.json

- Realistic client (zstd via Raven client):
  ravenbench run --url http://localhost:10101 --database ycsb --compression client:zstd --mode closed --reads 75 --writes 25 --concurrency 8..1024x2 --duration 60s

Mix definition (simpler)
- Prefer numeric flags: `--reads <weight|%> --writes <weight|%> --updates <weight|%>`
  - Examples: `--reads 75 --writes 25` or `--reads 3 --writes 1` (weights normalize to 100%).
- Legacy shorthand like `75R25W` is still accepted, but not required.

Fast tests
- `tests/RavenBench.Tests` includes unit tests for knee detection, distribution, option parsing, and a closed‑loop stubbed ramp. They don’t require a running RavenDB.

Notes
- Raw zstd decoding is not implemented yet; use `raw:identity` to expose network ceilings. `client:zstd` goes through the Raven client for realistic compression.
- Server-bound/async/disk attribution needs server metrics; this is staged next.

