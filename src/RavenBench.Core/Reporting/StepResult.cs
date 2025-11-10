using RavenBench.Core;

namespace RavenBench.Core.Reporting;

public sealed class StepResult
{
    public int Concurrency { get; init; }
    public double Throughput { get; init; }
    public double ErrorRate { get; init; }
    
    /// <summary>
    /// Target throughput (RPS) for rate-based benchmarks, null for closed-loop benchmarks.
    /// </summary>
    public double? TargetThroughput { get; init; }
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

    // Number of operations scheduled (may exceed completed if queueing occurs)
    // For rate mode, this shows if the generator kept up with the target RPS
    public long ScheduledOperations { get; init; }

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
    public QueryProfile? QueryProfile { get; init; }
}