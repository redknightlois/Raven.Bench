namespace RavenBench.Core.Reporting;

/// <summary>
/// Full histogram data for a concurrency step. We embed the complete percentile distribution
/// directly in JSON so you don't need to mess around with separate hlog/csv files.
/// Data stored as parallel arrays for compact representation.
/// </summary>
public sealed class HistogramArtifact
{
    /// <summary>
    /// Standard percentile points we export. Gets progressively more granular in the tail
    /// because that's where the interesting stuff happens in latency distributions.
    /// Goes from coarse (10% increments) to very fine (0.001% in extreme tail).
    /// Total of 46 points covering P0 through P100.
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

    // These three arrays are aligned - same index = same percentile point
    public required double[] Percentiles { get; init; }
    public required long[] LatencyInMicroseconds { get; init; }
    public required double[] LatencyInMilliseconds { get; init; }

    // Histogram bin data for reconstructing the full distribution
    // Parallel arrays: BinEdges[i] is the lower bound (in microseconds) and BinCounts[i] is the frequency
    public long[] BinEdges { get; init; } = Array.Empty<long>();
    public long[] BinCounts { get; init; } = Array.Empty<long>();

    // File paths are optional - only populated if you enabled file export
    public string? HlogPath { get; init; }
    public string? CsvPath { get; init; }
}