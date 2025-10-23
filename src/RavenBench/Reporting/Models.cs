using RavenBench.Diagnostics;
using RavenBench.Util;
using RavenBench.Metrics;
using System.Text.Json.Serialization;

namespace RavenBench.Reporting;

public readonly struct Percentiles
{
    public double P50 { get; }
    public double P75 { get; }
    public double P90 { get; }
    public double P95 { get; }
    public double P99 { get; }
    public double P999 { get; }

    [JsonConstructor]
    public Percentiles(double p50, double p75, double p90, double p95, double p99, double p999)
    {
        P50 = p50;
        P75 = p75;
        P90 = p90;
        P95 = p95;
        P99 = p99;
        P999 = p999;
    }
}

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

public sealed class SnmpTimeSeries
{
    public required DateTime Timestamp { get; init; }
    public double? MachineCpu { get; init; }
    public double? ProcessCpu { get; init; }
    public long? ManagedMemoryMb { get; init; }
    public long? UnmanagedMemoryMb { get; init; }
    public long? DirtyMemoryMb { get; init; }
    public double? Load1Min { get; init; }
    public double? SnmpIoReadOpsPerSec { get; init; }
    public double? SnmpIoWriteOpsPerSec { get; init; }
    public double? SnmpIoReadBytesPerSec { get; init; }
    public double? SnmpIoWriteBytesPerSec { get; init; }
    public double? ServerSnmpRequestsPerSec { get; init; }
}

public sealed class SnmpAggregations
{
    public double? TotalSnmpIoWriteOps { get; init; }
    public double? AverageSnmpIoWriteOpsPerSec { get; init; }
    public double? TotalSnmpIoReadOps { get; init; }
    public double? AverageSnmpIoReadOpsPerSec { get; init; }
    public double? TotalSnmpIoWriteBytes { get; init; }
    public double? AverageSnmpIoWriteBytesPerSec { get; init; }
    public double? TotalSnmpIoReadBytes { get; init; }
    public double? AverageSnmpIoReadBytesPerSec { get; init; }
}

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

public sealed class StepResult
{
    public int Concurrency { get; init; }
    public double Throughput { get; init; }
    public double ErrorRate { get; init; }
    public long BytesOut { get; init; }
    public long BytesIn { get; init; }

    public Percentiles Raw { get; set; }
    public Percentiles Normalized { get; set; }

    // High-percentile latency metrics for tail analysis (raw values)
    // p99.99 percentile in milliseconds (captures extreme tail behavior)
    public double P9999 { get; set; }

    // Maximum observed latency in milliseconds (worst-case single operation)
    public double PMax { get; set; }

    // Normalized tail metrics (baseline-adjusted, same as Normalized percentiles)
    // These subtract the baseline RTT to show additional latency due to load
    public double NormalizedP9999 { get; set; }
    public double NormalizedPMax { get; set; }

    // Number of actual operations observed (before coordinated omission correction)
    // This includes all completed operations (both successes and errors) that were recorded in the histogram
    public long SampleCount { get; init; }

    // Total histogram count including synthetic samples from coordinated omission correction
    // This will be >= SampleCount when corrections are applied
    public long CorrectedCount { get; set; }

    // Timestamp when maximum latency was observed (null if not tracked)
    public DateTimeOffset? MaxTimestamp { get; set; }

    public double ClientCpu { get; init; }
    public double NetworkUtilization { get; init; }

    // Server-side metrics from RavenDB
    public double? ServerCpu { get; init; }
    public long? ServerMemoryMB { get; init; }
    public double? ServerRequestsPerSec { get; init; }
    public long? ServerIoReadOps { get; init; }
    public long? ServerIoWriteOps { get; init; }
    public long? ServerIoReadKb { get; init; }
    public long? ServerIoWriteKb { get; init; }

    // SNMP gauge metrics
    public double? MachineCpu { get; init; }
    public double? ProcessCpu { get; init; }
    public long? ManagedMemoryMb { get; init; }
    public long? UnmanagedMemoryMb { get; init; }
    public long? DirtyMemoryMb { get; init; }
    public double? Load1Min { get; init; }
    public double? Load5Min { get; init; }
    public double? Load15Min { get; init; }

    // SNMP rate metrics
    public double? SnmpIoReadOpsPerSec { get; init; }
    public double? SnmpIoWriteOpsPerSec { get; init; }
    public double? SnmpIoReadBytesPerSec { get; init; }
    public double? SnmpIoWriteBytesPerSec { get; init; }
    public double? ServerSnmpRequestsPerSec { get; init; }
    public double? SnmpErrorsPerSec { get; init; }

    public string? Reason { get; set; }

    // Query metadata (populated for query workload profiles)
    public long? QueryOperations { get; init; }
    public IReadOnlyDictionary<string, long>? IndexUsage { get; init; }
    public List<IndexUsageSummary>? TopIndexes { get; init; }
    public int? MinResultCount { get; init; }
    public int? MaxResultCount { get; init; }
    public double? AvgResultCount { get; init; }
    public long? TotalResults { get; init; }
    public long? StaleQueryCount { get; init; }
    public Util.QueryProfile? QueryProfile { get; init; }
}

/// <summary>
/// Summarizes index usage for a single index (used in top-N summaries).
/// </summary>
public sealed class IndexUsageSummary
{
    public required string IndexName { get; init; }
    public required long UsageCount { get; init; }
    public double? UsagePercent { get; init; }
}

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

    // File paths are optional - only populated if you enabled file export
    public string? HlogPath { get; init; }
    public string? CsvPath { get; init; }
}
