using System.ComponentModel;
using Spectre.Console.Cli;
using RavenBench.Core;

namespace RavenBench.Cli;

public abstract class BaseRunSettings : CommandSettings
{
    [CommandOption("--url")]
    [Description("RavenDB server URL (e.g., http://localhost:8080)")]
    public string? Url { get; init; }

    [CommandOption("--database")]
    [Description("Target database name")]
    public string? Database { get; init; }

    // Preferred numeric weights
    [CommandOption("--reads")]
    [Description("Read operation weight (percentage or count)")]
    public string? Reads { get; init; }

    [CommandOption("--writes")]
    [Description("Write operation weight (percentage or count)")]
    public string? Writes { get; init; }

    [CommandOption("--updates")]
    [Description("Update operation weight (percentage or count)")]
    public string? Updates { get; init; }

    // Vector search options
    [CommandOption("--vector-topk")]
    [Description("Number of nearest neighbors to return (default: 10)")]
    public int VectorTopK { get; init; } = 10;

    [CommandOption("--vector-quantization")]
    [Description("Vector quantization: none, int8, binary (default: none)")]
    public string VectorQuantization { get; init; } = "none";

    [CommandOption("--vector-exact-search")]
    [Description("Use exact vector search instead of approximate (HNSW)")]
    public bool VectorExactSearch { get; init; } = false;

    [CommandOption("--vector-min-similarity")]
    [Description("Minimum similarity threshold (0.0-1.0)")]
    public float VectorMinSimilarity { get; init; } = 0.0f;

    [CommandOption("--vector-dimension")]
    [Description("Vector dimension (128, 384, 768, 1536)")]
    public int VectorDimension { get; init; } = 128;

    [CommandOption("--profile")]
    [Description("Required. Operation profile: mixed, writes, reads, query-by-id, bulk-writes, stackoverflow-reads, stackoverflow-queries, query-users-by-name, vector-search, vector-search-exact")]
    public string? Profile { get; init; }

    [CommandOption("--query-profile")]
    [Description("Query profile for query workloads: equality, range, text-prefix, text-search, text-search-rare, text-search-common, text-search-mixed (default: equality)")]
    public string? QueryProfile { get; init; }

    [CommandOption("--bulk-batch-size")]
    [Description("Number of documents per bulk batch (default: 100)")]
    public int BulkBatchSize { get; init; } = 100;

    [CommandOption("--bulk-depth")]
    [Description("Number of batches to send in parallel (default: 1)")]
    public int BulkDepth { get; init; } = 1;

    [CommandOption("--dataset")]
    [Description("Dataset to import: stackoverflow, clinicalwords100d, clinicalwords300d, clinicalwords600d (auto-downloads and imports)")]
    public string? Dataset { get; init; }

    [CommandOption("--dataset-profile")]
    [Description("Dataset size profile: small (~5GB), half (~20GB), full (~50GB) - automatically sets database name and size")]
    public string? DatasetProfile { get; init; }

    [CommandOption("--dataset-size")]
    [Description("Custom dataset size: 0=full, N=use N post dump files (overridden by --dataset-profile)")]
    public int DatasetSize { get; init; } = 0;

    [CommandOption("--dataset-skip-if-exists")]
    [Description("Skip dataset import if data already exists (default: true)")]
    public bool? DatasetSkipIfExists { get; init; }

    [CommandOption("--force-dataset-import")]
    [Description("Force dataset import even if data exists (overrides --dataset-skip-if-exists)")]
    public bool ForceDatasetImport { get; init; }

    [CommandOption("--dataset-cache-dir")]
    [Description("Directory for caching downloaded dataset files")]
    public string? DatasetCacheDir { get; init; }

    [CommandOption("--distribution")]
    [Description("Key distribution: uniform, zipfian (default: uniform)")]
    public string Distribution { get; init; } = "uniform";

    [CommandOption("--doc-size")]
    [Description("Document size (e.g., 1KB, 10KB, 1MB) (default: 1KB)")]
    public string DocSize { get; init; } = "1KB";

    [CommandOption("--transport")]
    [Description("Transport mode: raw (HTTP direct), client (RavenDB .NET client) (default: raw)")]
    public string Transport { get; init; } = "raw";

    [CommandOption("--compression")]
    [Description("Compression mode: identity, gzip, brotli, zstd (default: identity)")]
    public string Compression { get; init; } = "identity";

    [CommandOption("--step")]
    [Description("Step plan: start..end or start..endxfactor (e.g., 8..512x2)")]
    public string? Step { get; init; }

    [CommandOption("--warmup")]
    [Description("Warmup duration (e.g., 20s, 1m) (default: 20s)")]
    public string Warmup { get; init; } = "20s";

    [CommandOption("--duration")]
    [Description("Step duration (e.g., 60s, 2m) (default: 60s)")]
    public string Duration { get; init; } = "60s";

    [CommandOption("--max-errors")]
    [Description("Maximum error rate before stopping (e.g., 0.5%, 100) (default: 0.5%)")]
    public string MaxErrors { get; init; } = "0.5%";

    [CommandOption("--knee-rule")]
    [Description("Knee detection rule: dthr=X%,dp95=Y% (default: dthr=5%,dp95=20%)")]
    public string KneeRule { get; init; } = "dthr=5%,dp95=20%";

    [CommandOption("--output-prefix")]
    [Description("Output path prefix for all artifacts (creates: {prefix}.json, {prefix}.csv, {prefix}-step-cXXXX.hlog)")]
    public string? OutputDir { get; init; }

    [CommandOption("--out")]
    [Description("Output path for JSON results (use --output-prefix for unified output)")]
    public string? OutJson { get; init; }

    [CommandOption("--out-csv")]
    [Description("Output path for CSV results (use --output-prefix for unified output)")]
    public string? OutCsv { get; init; }

    [CommandOption("--seed")]
    [Description("Random seed for reproducibility (default: 42)")]
    public int Seed { get; init; } = 42;

    [CommandOption("--preload")]
    [Description("Number of documents to preload before benchmark (default: 0)")]
    public int Preload { get; init; } = 0;

    [CommandOption("--raw-endpoint")]
    [Description("Custom raw HTTP endpoint path")]
    public string? RawEndpoint { get; init; }

    [CommandOption("--tp-workers")]
    [Description("ThreadPool worker threads (default: 8192)")]
    public int? TpWorkers { get; init; } = 8192;

    [CommandOption("--tp-iocp")]
    [Description("ThreadPool IOCP threads (default: 8192)")]
    public int? TpIOCP { get; init; } = 8192;

    [CommandOption("--notes")]
    [Description("Custom notes to include in output")]
    public string? Notes { get; init; }

    [CommandOption("--expected-cores")]
    [Description("Expected CPU core count for validation")]
    public int? ExpectedCores { get; init; }

    [CommandOption("--network-limited")]
    [Description("Enable network-limited mode")]
    public bool NetworkLimitedMode { get; init; }

    [CommandOption("--link-mbps")]
    [Description("Network link speed in Mbps (default: 1000)")]
    public double LinkMbps { get; init; } = 1000;

    [CommandOption("--http-version")]
    [Description("HTTP version: auto, 1.1, 2.0, 3.0 (default: auto)")]
    public string HttpVersion { get; init; } = "auto";

    [CommandOption("--strict-http-version")]
    [Description("Enforce strict HTTP version (fail if unavailable)")]
    public bool StrictHttpVersion { get; init; }

    [CommandOption("--verbose")]
    [Description("Enable verbose output")]
    public bool Verbose { get; init; }

    [CommandOption("--latencies")]
    [Description("Latency display: normalized, raw, both (default: normalized)")]
    public string Latencies { get; init; } = "normalized";

    [CommandOption("--snmp-enabled")]
    [Description("Enable SNMP monitoring (default: true)")]
    public bool SnmpEnabled { get; init; } = true;


    [CommandOption("--snmp-port")]
    [Description("SNMP port (default: 161)")]
    public int SnmpPort { get; init; } = 161;

    [CommandOption("--snmp-interval")]
    [Description("SNMP polling interval (e.g., 250ms, 1s)")]
    public string? SnmpInterval { get; init; }

    [CommandOption("--snmp-profile")]
    [Description("SNMP profile: minimal, extended (default: minimal)")]
    public string SnmpProfile { get; init; } = "minimal";

    [CommandOption("--snmp-timeout")]
    [Description("SNMP timeout (e.g., 5s, 10s) (default: 5s)")]
    public string SnmpTimeout { get; init; } = "5s";

    [CommandOption("--histograms-format")]
    [Description("Histogram export format: hlog, csv, both (default: hlog, auto-enabled when CSV output specified)")]
    public string HistogramsFormat { get; init; } = "hlog";

    [CommandOption("--search-engine")]
    [Description("Search engine for indexes: corax (default), lucene. Vector search requires corax.")]
    public string SearchEngine { get; init; } = "corax";

}
