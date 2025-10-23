using RavenBench.Core;
using RavenBench.Core.Diagnostics;

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
}