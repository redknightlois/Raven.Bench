using System.Globalization;

namespace RavenBench.Util;

public enum LatencyDisplayType
{
    Normalized,
    Raw,
    Both
}

/// <summary>
/// Configuration options for running a benchmark, including server settings,
/// workload parameters, concurrency settings, and output options.
/// </summary>
public sealed class RunOptions
{
    public required string Url { get; init; }
    public required string Database { get; init; }

    // Mix defined only via numeric flags (weights or percents)
    public double? Reads { get; init; }
    public double? Writes { get; init; }
    public double? Updates { get; init; }
    public string Distribution { get; init; } = "uniform";
    public int DocumentSizeBytes { get; init; } = 1024;
    public string Transport { get; init; } = "raw";
    public string Compression { get; init; } = "identity";
    public string Mode { get; init; } = "closed";
    public int ConcurrencyStart { get; init; } = 8;
    public int ConcurrencyEnd { get; init; } = 512;
    public double ConcurrencyFactor { get; init; } = 2.0;
    public TimeSpan Warmup { get; init; } = TimeSpan.FromSeconds(20);
    public TimeSpan Duration { get; init; } = TimeSpan.FromSeconds(60);
    public double MaxErrorRate { get; init; } = 0.005; // 0.5%
    public double KneeThroughputDelta { get; init; } = 0.05; // 5%
    public double KneeP95Delta { get; init; } = 0.20; // 20%
    public string? OutJson { get; init; }
    public string? OutCsv { get; init; }
    public int Seed { get; init; } = 42;
    public int Preload { get; init; } = 0;
    public string? RawEndpoint { get; init; }
    public int? ThreadPoolWorkers { get; init; } = 8192;
    public int? ThreadPoolIOCP { get; init; } = 8192;
    public string? Notes { get; init; }
    public int? ExpectedCores { get; init; }
    public bool NetworkLimitedMode { get; init; } = false;
    public double LinkMbps { get; init; } = 1000.0; // default 1 Gb
    public string HttpVersion { get; init; } = "auto"; // "1.1" | "2" | "3" | "auto"
    public bool StrictHttpVersion { get; init; } = false; // fail if requested HTTP version is not available
    public bool Verbose { get; init; } = false; // enable detailed error logging
    public LatencyDisplayType LatencyDisplay { get; init; } = LatencyDisplayType.Normalized; // which latencies to display

    public SnmpOptions Snmp { get; init; } = SnmpOptions.Disabled;

    // Required workload profile selection
    public WorkloadProfile Profile { get; init; } = WorkloadProfile.Unspecified;

    // Query profile selection (equality, range, text) for query workloads
    // Defaults to Equality for backward compatibility
    public QueryProfile QueryProfile { get; init; } = QueryProfile.Equality;

    public int BulkBatchSize { get; init; } = 100;
    public int BulkDepth { get; init; } = 1;

    // Dataset management
    public string? Dataset { get; init; }
    public string? DatasetProfile { get; init; } // small, half, full - automatically sets database name and size
    public int DatasetSize { get; init; } = 0; // 0 = full dataset, N = use N post dump files for partial (overridden by DatasetProfile)
    public bool DatasetSkipIfExists { get; init; } = true; // Skip import if dataset appears to exist
    public string? DatasetCacheDir { get; init; }

    // Legacy SNMP properties for backwards compatibility - prefer using Snmp property
    public bool SnmpEnabled => Snmp.Enabled;
    public int SnmpPort => Snmp.Port;
    public TimeSpan SnmpPollInterval => Snmp.PollInterval;
}

public enum WorkloadProfile
{
    Unspecified = 0,
    Mixed,
    Writes,
    Reads,
    QueryById,
    BulkWrites,
    StackOverflowReads,
    StackOverflowQueries,
    QueryUsersByName
}

/// <summary>
/// Query profile type for parameterized query workloads.
/// Determines which query template to use (equality, range, or text search).
/// </summary>
public enum QueryProfile
{
    /// <summary>
    /// Equality queries (e.g., WHERE Name = $name)
    /// </summary>
    Equality = 0,

    /// <summary>
    /// Range queries (e.g., WHERE Reputation BETWEEN $min AND $max)
    /// </summary>
    Range,

    /// <summary>
    /// Prefix text search (e.g., WHERE startsWith(Title, $prefix))
    /// </summary>
    TextPrefix,

    /// <summary>
    /// Full-text search (e.g., WHERE search(Title, $term))
    /// </summary>
    TextSearch,

    /// <summary>
    /// Full-text search using only rare terms (high selectivity)
    /// </summary>
    TextSearchRare,

    /// <summary>
    /// Full-text search using only common terms (low selectivity)
    /// </summary>
    TextSearchCommon,

    /// <summary>
    /// Full-text search mixing rare and common terms (50/50)
    /// </summary>
    TextSearchMixed
}
