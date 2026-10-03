using RavenBench.Core;
using RavenBench.Core.Metrics;

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

    /// <summary>The operations the latency figures hold: failed and timed-out operations add a sample, cancelled ones do not.</summary>
    public string LatencySamples { get; init; } = LoadGeneratorExecution.LatencySamples;

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

    /// <summary>
    /// The CPU the load host spent over this step's measurement window, as a 0..1 fraction of the
    /// host's total capacity as this process sees it. Warmup is outside the window.
    /// </summary>
    public double ClientCpu { get; init; }

    /// <summary>
    /// The measured length of this step's measurement window: the window the throughput divides by
    /// and <see cref="ClientCpu"/> covers. A bounded fill that ends early reports its own length,
    /// not the configured duration cap.
    /// </summary>
    public TimeSpan? MeasuredDuration { get; init; }

    /// <summary>
    /// Why this step must not be published, when it must not be. Absent for a valid step. It is
    /// separate from <see cref="Reason"/>, which carries warmup and ramp-stop reasons.
    /// </summary>
    public string? InvalidReason { get; set; }

    public double NetworkUtilization { get; init; }
    /// <summary>True when NetworkUtilization is derived from measured wire bytes; false when estimated.</summary>
    public bool NetworkBytesMeasured { get; init; }
    public RollingRateStats? RollingRate { get; init; }

    /// <summary>
    /// Rate mode only: send time minus scheduled time, in milliseconds. Latency counts from the
    /// scheduled time, so this is the part of it the client added by not keeping its schedule.
    /// </summary>
    public Percentiles? SendLateness { get; init; }

    // Server-side metrics from RavenDB
    public double? ServerCpu { get; init; }
    public long? ServerMemoryMB { get; init; }

    /// <summary>The source that filled <see cref="ServerCpu"/>: "node_exporter" or "ravendb-debug". Absent when the column is.</summary>
    public string? ServerCpuSource { get; init; }

    /// <summary>The definition behind <see cref="ServerCpu"/>: the core count it is a percent of, and the window it averages over. Absent when no source was read.</summary>
    public string? ServerCpuBasis { get; init; }

    /// <summary>The source that filled <see cref="ServerMemoryMB"/>: "node_exporter" or "ravendb-debug". Absent when the column is.</summary>
    public string? ServerMemorySource { get; init; }

    /// <summary>True when the server CPU and memory figures cover the whole database host, not the database process alone.</summary>
    public bool? ServerMetricsHostWide { get; init; }

    /// <summary>Why a configured server source left the CPU and memory columns absent for this step, prefixed by the source name.</summary>
    public string? ServerMetricsUnavailable { get; init; }
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
