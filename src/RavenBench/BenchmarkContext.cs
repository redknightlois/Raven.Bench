using RavenBench.Metrics;
using RavenBench.Transport;
using RavenBench.Util;
using RavenBench.Workload;

namespace RavenBench;

/// <summary>
/// Encapsulates the shared context for benchmark execution to reduce parameter passing.
/// </summary>
internal sealed class BenchmarkContext
{
    public required ITransport Transport { get; init; }
    public required IWorkload Workload { get; init; }
    public required ProcessCpuTracker CpuTracker { get; init; }
    public required ServerMetricsTracker ServerTracker { get; init; }
    public required Random Rng { get; init; }
    public required RunOptions Options { get; init; }
}

/// <summary>
/// Parameters specific to each benchmark step.
/// </summary>
internal sealed class StepParameters
{
    public required int Concurrency { get; init; }
    public required TimeSpan Duration { get; init; }
    public required bool Record { get; init; }

    /// <summary>
    /// Baseline latency in microseconds, derived from warmup measurements.
    /// Used for coordinated omission correction to calculate expected intervals between requests.
    /// A value of 0 means no baseline is available (e.g., first warmup), so no correction is applied.
    /// </summary>
    public long BaselineLatencyMicros { get; init; }
}
