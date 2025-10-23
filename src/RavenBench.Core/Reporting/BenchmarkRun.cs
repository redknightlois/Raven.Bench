using RavenBench.Core.Diagnostics;
using RavenBench.Core.Metrics;

namespace RavenBench.Core.Reporting;

public sealed class BenchmarkRun
{
    public required List<StepResult> Steps { get; init; }
    public required double MaxNetworkUtilization { get; init; }
    public required string ClientCompression { get; init; }
    public required string EffectiveHttpVersion { get; init; }
    public StartupCalibration? StartupCalibration { get; init; }
    public List<ServerMetrics>? ServerMetricsHistory { get; init; }
    public List<HistogramArtifact>? HistogramArtifacts { get; init; }
}