using RavenBench.Core.Diagnostics;
using RavenBench.Core.Metrics;
using RavenBench.Core.Workload;

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

    /// <summary>
    /// Vector workload metadata, exposed for recall@K measurement after benchmark completion.
    /// Null for non-vector workloads.
    /// </summary>
    public VectorWorkloadMetadata? VectorMetadata { get; init; }

    /// <summary>
    /// The actual database name used (may differ from opts.Database for dataset-based profiles).
    /// </summary>
    public required string EffectiveDatabase { get; init; }
}