namespace RavenBench.Core.Metrics;

/// <summary>
/// Describes rolling request-per-second samples gathered during a rate-mode step.
/// Values are derived from a sliding window (default: 3s) to show how tightly the
/// generator tracked the requested throughput throughout the measurement interval.
/// </summary>
public sealed record RollingRateStats
{
    public static RollingRateStats Empty { get; } = new();

    public double Median { get; init; }
    public double Mean { get; init; }
    public double Min { get; init; }
    public double Max { get; init; }
    public double Last { get; init; }
    public int SampleCount { get; init; }

    public bool HasSamples => SampleCount > 0;
}
