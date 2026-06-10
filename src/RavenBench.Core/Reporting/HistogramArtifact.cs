namespace RavenBench.Core.Reporting;

/// <summary>
/// Full histogram data for a concurrency step, embedded in JSON as parallel arrays.
/// </summary>
public sealed class HistogramArtifact
{
    /// <summary>
    /// Percentile points exported for each histogram; resolution increases toward the tail.
    /// </summary>
    public static ReadOnlySpan<double> StandardPercentiles => new[]
    {
        // Most of the distribution - 10% steps is plenty here
        0.0, 10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0,

        // Getting interesting, add a bit more detail
        75.0, 80.0, 85.0, 90.0,

        // Tail starts here - bump to 1% resolution
        91.0, 92.0, 93.0, 94.0, 95.0, 96.0, 97.0, 98.0, 99.0,

        // Now we're in the danger zone - 0.1% resolution
        99.1, 99.2, 99.3, 99.4, 99.5, 99.6, 99.7, 99.8, 99.9,

        // Extreme outliers - 0.01% resolution
        99.90, 99.91, 99.92, 99.93, 99.94, 99.95, 99.96, 99.97, 99.98, 99.99,

        // The really bad ones - 0.001% resolution
        99.990, 99.991, 99.992, 99.993, 99.994, 99.995, 99.996, 99.997, 99.998, 99.999,

        // Worst case observed
        100.0
    };

    public required int Concurrency { get; init; }
    public required long TotalCount { get; init; }
    public required long MaxValueInMicroseconds { get; init; }

    // Parallel arrays: same index = same percentile point.
    public required double[] Percentiles { get; init; }
    public required long[] LatencyInMicroseconds { get; init; }
    public required double[] LatencyInMilliseconds { get; init; }

    // Parallel arrays: BinEdges[i] is the lower bound in microseconds, BinCounts[i] the frequency.
    public long[] BinEdges { get; init; } = Array.Empty<long>();
    public long[] BinCounts { get; init; } = Array.Empty<long>();

    // Populated only when file export is enabled.
    public string? HlogPath { get; init; }
    public string? CsvPath { get; init; }
}
