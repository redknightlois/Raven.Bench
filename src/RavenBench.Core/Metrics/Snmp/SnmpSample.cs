using System;

namespace RavenBench.Core.Metrics.Snmp;

/// <summary>
/// Represents a single SNMP telemetry sample with timestamp and raw metric values.
/// </summary>
public sealed class SnmpSample
{
    public DateTime Timestamp { get; init; } = DateTime.UtcNow;

    // CPU metrics
    public double? MachineCpu { get; init; }
    public double? ProcessCpu { get; init; }

    // Memory metrics
    public long? ManagedMemoryMb { get; init; }
    public long? UnmanagedMemoryMb { get; init; }
    public long? DirtyMemoryMb { get; init; }

    // Load averages
    public double? Load1Min { get; init; }
    public double? Load5Min { get; init; }
    public double? Load15Min { get; init; }

    // IO rate metrics (already computed as rates by RavenDB)
    public double? IoReadOpsPerSec { get; init; }
    public double? IoWriteOpsPerSec { get; init; }
    public double? IoReadKbPerSec { get; init; }
    public double? IoWriteKbPerSec { get; init; }

    // Counter: total requests since server start.
    public long? TotalRequests { get; init; }

    public bool IsEmpty =>
        MachineCpu == null &&
        ProcessCpu == null &&
        ManagedMemoryMb == null &&
        UnmanagedMemoryMb == null &&
        DirtyMemoryMb == null &&
        Load1Min == null &&
        IoReadOpsPerSec == null &&
        TotalRequests == null;
}