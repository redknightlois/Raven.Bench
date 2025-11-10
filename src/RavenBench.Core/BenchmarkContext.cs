using RavenBench.Core.Metrics;
using RavenBench.Core.Transport;
using RavenBench.Core.Workload;

namespace RavenBench.Core;

/// <summary>
/// Encapsulates shared context for benchmark execution to reduce parameter passing.
/// </summary>
public sealed class BenchmarkContext
{
    public required RunOptions Options { get; init; }
}

/// <summary>
/// Parameters specific to each benchmark step.
/// </summary>
public sealed class StepParameters
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