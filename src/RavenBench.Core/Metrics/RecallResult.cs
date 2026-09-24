namespace RavenBench.Core.Metrics;

/// <summary>
/// Results of recall@K measurement for vector search benchmarks.
/// Recall is the fraction of approximate (HNSW) results that appear in the
/// ground truth (exact brute-force) results at each K cutoff.
/// </summary>
public sealed class RecallResult
{
    /// <summary>
    /// Per-K recall values. Key is K, value is average recall across all query vectors (0.0–1.0).
    /// </summary>
    public required Dictionary<int, double> RecallAtK { get; init; }

    /// <summary>
    /// Number of query vectors used for recall measurement.
    /// </summary>
    public int QueryCount { get; init; }

    /// <summary>
    /// Maximum K value in the ground truth (the depth at which exact search was performed).
    /// </summary>
    public int GroundTruthDepth { get; init; }

    /// <summary>
    /// Whether ground truth was loaded from cache or freshly computed.
    /// </summary>
    public bool GroundTruthCached { get; init; }

    /// <summary>
    /// Time taken to compute ground truth (zero if loaded from cache).
    /// </summary>
    public TimeSpan GroundTruthComputeTime { get; init; }

    /// <summary>
    /// Time taken to run the recall measurement queries.
    /// </summary>
    public TimeSpan MeasurementTime { get; init; }

    /// <summary>The step server columns over the measurement queries. Only node_exporter fills them, so a figure is host-wide.</summary>
    public double? ServerCpu { get; init; }

    public long? ServerMemoryMB { get; init; }

    public string? ServerCpuSource { get; init; }

    public string? ServerMemorySource { get; init; }

    public bool? ServerMetricsHostWide { get; init; }

    public string? ServerMetricsUnavailable { get; init; }
}
