using System;
using System.Globalization;
using RavenBench.Core.Workload;

namespace RavenBench.Core;

/// <summary>
/// Search engine type for RavenDB indexes.
/// </summary>
public enum IndexingEngine
{
    /// <summary>
    /// Corax - Modern, high-performance search engine (default).
    /// Required for vector search.
    /// </summary>
    Corax,

    /// <summary>
    /// Lucene - Legacy search engine.
    /// Does not support vector indexing.
    /// </summary>
    Lucene
}

public enum LatencyDisplayType
{
    Normalized,
    Raw,
    Both
}

/// <summary>
/// Represents a step plan with start, end, and multiplication factor.
/// Used for both concurrency steps (closed mode) and rate steps (rate mode).
/// </summary>
public readonly struct StepPlan
{
    public static StepPlan Empty { get; } = new StepPlan(0, 0, 0);

    public int Start { get; }
    public int End { get; }
    public double Factor { get; }

    public StepPlan(int start, int end, double factor)
    {
        Start = start;
        End = end;
        Factor = factor;
    }

    public bool IsValid => Start > 0 && End >= Start && Factor > 0;
    public bool IsEmpty => Start == 0 && End == 0;

    public StepPlan Normalize()
    {
        if (IsValid)
            return this;

        var normalizedStart = Math.Max(1, Start);
        var normalizedEnd = Math.Max(normalizedStart, End);
        var normalizedFactor = Factor <= 0 ? 1.0 : Factor;

        return new StepPlan(normalizedStart, normalizedEnd, normalizedFactor);
    }

    public int Next(int current)
    {
        var factor = Math.Max(Factor, 1.0);
        var next = (int)Math.Max(current * factor, current + 1);
        return Math.Max(next, current + 1);
    }
}

/// <summary>
/// Load shape type for different benchmark modes.
/// </summary>
public enum LoadShape
{
    Closed,
    Rate
}

/// <summary>
/// Configuration options for running a benchmark, including server settings,
/// workload parameters, concurrency settings, and output options.
/// </summary>
public sealed record RunOptions
{
    public required string Url { get; init; }
    public required string Database { get; init; }

    // Step plan and load shape
    public StepPlan Step { get; init; } = new StepPlan(8, 512, 2.0);
    public LoadShape Shape { get; init; }

    // Mix defined only via numeric flags (weights or percents)
    public double? Reads { get; init; }
    public double? Writes { get; init; }
    public double? Updates { get; init; }

    // Vector search options
    public int VectorTopK { get; init; } = 10;
    public VectorQuantization VectorQuantization { get; init; } = VectorQuantization.None;
    public bool VectorExactSearch { get; init; } = false;
    public float VectorMinSimilarity { get; init; } = 0.0f;
    public int VectorDimension { get; init; } = 128; // Default to SIFT1M

    public string Distribution { get; init; } = "uniform";
    public int DocumentSizeBytes { get; init; } = 1024;
    public string Transport { get; init; } = "raw";
    public string Compression { get; init; } = "identity";
    public string Mode { get; init; } = "closed";
    public int? RateWorkers { get; init; } // Max concurrent operations for rate mode (null = auto)
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

    // Query profile selection (voron-equality, index-equality, range, text) for query workloads
    // Defaults to VoronEquality (direct document lookup via id())
    public QueryProfile QueryProfile { get; init; } = QueryProfile.VoronEquality;

    public int BulkBatchSize { get; init; } = 100;
    public int BulkDepth { get; init; } = 1;

    // Dataset management
    public string? Dataset { get; init; }
    public string? DatasetProfile { get; init; } // small, half, full - automatically sets database name and size
    public int DatasetSize { get; init; } = 0; // 0 = full dataset, N = use N post dump files for partial (overridden by DatasetProfile)
    public bool DatasetSkipIfExists { get; init; } = true; // Skip import if dataset appears to exist
    public string? DatasetCacheDir { get; init; }

    public string? OutputDir { get; init; }  // Prefix for all output files when using --output-prefix

    // Legacy SNMP properties for backwards compatibility - prefer using Snmp property
    public bool SnmpEnabled => Snmp.Enabled;
    public int SnmpPort => Snmp.Port;
    public TimeSpan SnmpPollInterval => Snmp.PollInterval;

    public string? LatencyHistogramsDir { get; init; }
    public HistogramExportFormat LatencyHistogramsFormat { get; init; } = HistogramExportFormat.Hlog;

    /// <summary>
    /// Search engine type for indexes. Defaults to Corax.
    /// Vector search profiles require Corax.
    /// </summary>
    public IndexingEngine SearchEngine { get; init; } = IndexingEngine.Corax;
}

public enum WorkloadProfile
{
    Unspecified = 0,
    Mixed,
    Writes,
    Reads,
    QueryById,
    BulkWrites,
    StackOverflowRandomReads,
    StackOverflowTextSearch,
    QueryUsersByName,
    VectorSearch,
    VectorSearchExact
}

public static class WorkloadProfiles
{
    /// <summary>
    /// Returns the indexing engines supported by the given profile.
    /// </summary>
    public static IndexingEngine[] GetSupportedEngines(WorkloadProfile profile)
    {
        return profile switch
        {
            // Vector search only works with Corax
            WorkloadProfile.VectorSearch => [IndexingEngine.Corax],
            WorkloadProfile.VectorSearchExact => [IndexingEngine.Corax],

            // All other profiles support both engines
            _ => [IndexingEngine.Corax, IndexingEngine.Lucene]
        };
    }

    /// <summary>
    /// Checks if the profile supports the specified indexing engine.
    /// </summary>
    public static bool SupportsEngine(WorkloadProfile profile, IndexingEngine engine)
    {
        return GetSupportedEngines(profile).Contains(engine);
    }
}

/// <summary>
/// Query profile type for parameterized query workloads.
/// Determines which query template to use (equality, range, or text search).
/// </summary>
public enum QueryProfile
{
    /// <summary>
    /// Direct Voron document lookup via id() function.
    /// Uses: from @all_docs where id() = $id
    /// </summary>
    VoronEquality = 0,

    /// <summary>
    /// Index-based field lookup by document ID field.
    /// Uses: from Questions where Id = $id
    /// </summary>
    IndexEquality,

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

public enum HistogramExportFormat
{
    Hlog = 0,  // HdrHistogram log format - more data points
    Csv,       // Simple CSV - just the key percentiles
    Both       // Write both files
}
