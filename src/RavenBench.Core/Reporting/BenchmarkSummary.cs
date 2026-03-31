using RavenBench.Core;
using RavenBench.Core.Diagnostics;
using RavenBench.Core.Metrics;

namespace RavenBench.Core.Reporting;

public sealed class BenchmarkSummary
{
    public required RunOptions Options { get; init; }
    public required List<StepResult> Steps { get; init; }
    public StepResult? Knee { get; init; }
    public required string Verdict { get; init; }
    public required string ClientCompression { get; init; }
    public required string EffectiveHttpVersion { get; init; }
    public StartupCalibration? StartupCalibration { get; init; }
    public string? Notes { get; init; }

    // SNMP detailed time series data
    public List<SnmpTimeSeries>? SnmpTimeSeries { get; init; }

    // SNMP aggregations across the entire benchmark run
    public SnmpAggregations? SnmpAggregations { get; init; }

    // Full histogram data for each concurrency step
    public List<HistogramArtifact>? HistogramArtifacts { get; init; }

    /// <summary>
    /// Recall@K measurement results for vector search benchmarks.
    /// Null when recall measurement is not enabled (--vector-recall-ks not specified).
    /// When efSearch sweep is used, this holds the result for the default efSearch.
    /// </summary>
    public RecallResult? Recall { get; set; }

    /// <summary>
    /// Recall@K results across multiple efSearch values (sweep mode).
    /// Key is efSearch value, value is the recall result at that efSearch.
    /// Null when --vector-recall-ef-sweep is not specified.
    /// </summary>
    public Dictionary<int, RecallResult>? RecallSweep { get; set; }
}