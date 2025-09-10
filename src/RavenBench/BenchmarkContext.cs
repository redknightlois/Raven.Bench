using RavenBench.Metrics;
using RavenBench.Transport;
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
    public required Random Rng { get; init; }
}

/// <summary>
/// Parameters specific to each benchmark step.
/// </summary>
internal sealed class StepParameters
{
    public required int Concurrency { get; init; }
    public required TimeSpan Duration { get; init; }
    public required bool Record { get; init; }
}