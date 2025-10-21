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
