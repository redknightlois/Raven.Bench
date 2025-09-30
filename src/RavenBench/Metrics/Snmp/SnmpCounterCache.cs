using System;
using System.Collections.Generic;

namespace RavenBench.Metrics.Snmp;

/// <summary>
/// Calculates average rates from SNMP metrics by tracking deltas between samples.
/// For counter-based metrics (TotalRequests), computes average rate over the polling interval.
/// For pre-computed rate metrics (IO ops), passes through the values from RavenDB.
/// </summary>
public sealed class SnmpCounterCache
{
    private readonly object _lock = new();
    private SnmpSample? _previousSample;

    /// <summary>
    /// Converts SNMP sample to rates by calculating averages over the polling interval.
    /// Server requests per second is computed from TotalRequests counter delta.
    /// Returns null on first sample to establish baseline.
    /// </summary>
    public SnmpRates? ComputeRates(SnmpSample newSample)
    {
        lock (_lock)
        {
            if (_previousSample == null)
            {
                _previousSample = newSample;
                return null; // Need at least one sample for baseline
            }

            var elapsedSeconds = (newSample.Timestamp - _previousSample.Timestamp).TotalSeconds;

            // Calculate average requests per second from the TotalRequests counter delta
            double? serverRequestsPerSec = null;
            if (elapsedSeconds > 0)
            {
                var requestDelta = newSample.TotalRequests - _previousSample.TotalRequests;
                if (requestDelta >= 0)
                {
                    serverRequestsPerSec = requestDelta / elapsedSeconds;
                }
            }

            var rates = new SnmpRates
            {
                // Gauge-type metrics (instantaneous values)
                MachineCpu = newSample.MachineCpu,
                ProcessCpu = newSample.ProcessCpu,
                ManagedMemoryMb = newSample.ManagedMemoryMb,
                UnmanagedMemoryMb = newSample.UnmanagedMemoryMb,
                DirtyMemoryMb = newSample.DirtyMemoryMb,
                Load1Min = newSample.Load1Min,
                Load5Min = newSample.Load5Min,
                Load15Min = newSample.Load15Min,

                // Rate metrics (calculated from counter deltas for average over polling interval)
                IoReadOpsPerSec = newSample.IoReadOpsPerSec,
                IoWriteOpsPerSec = newSample.IoWriteOpsPerSec,
                IoReadBytesPerSec = newSample.IoReadKbPerSec != null ? newSample.IoReadKbPerSec * 1024 : null,  // Convert KB to bytes
                IoWriteBytesPerSec = newSample.IoWriteKbPerSec != null ? newSample.IoWriteKbPerSec * 1024 : null,  // Convert KB to bytes
                ServerRequestsPerSec = serverRequestsPerSec,
                ErrorsPerSec = null,  // Not available from RavenDB SNMP

                Timestamp = newSample.Timestamp
            };

            _previousSample = newSample;
            return rates;
        }
    }

    /// <summary>
    /// Resets the cache, forcing the next sample to be treated as the first.
    /// </summary>
    public void Reset()
    {
        lock (_lock)
        {
            _previousSample = null;
        }
    }

}

/// <summary>
/// Represents computed rate metrics from SNMP samples.
/// Gauge metrics (CPU, memory, load) are copied directly.
/// Counter metrics are converted to per-second rates.
/// </summary>
public sealed class SnmpRates
{
    public DateTime Timestamp { get; init; }

    // Gauge-type metrics (instantaneous values)
    public double? MachineCpu { get; init; }
    public double? ProcessCpu { get; init; }
    public long? ManagedMemoryMb { get; init; }
    public long? UnmanagedMemoryMb { get; init; }
    public long? DirtyMemoryMb { get; init; }
    public double? Load1Min { get; init; }
    public double? Load5Min { get; init; }
    public double? Load15Min { get; init; }

    // Rate metrics (per second)
    public double? IoReadOpsPerSec { get; init; }
    public double? IoWriteOpsPerSec { get; init; }
    public double? IoReadBytesPerSec { get; init; }
    public double? IoWriteBytesPerSec { get; init; }
    public double? ServerRequestsPerSec { get; init; }
    public double? ErrorsPerSec { get; init; }

    public bool IsEmpty =>
        MachineCpu == null &&
        ProcessCpu == null &&
        ManagedMemoryMb == null &&
        UnmanagedMemoryMb == null &&
        IoReadOpsPerSec == null &&
        ServerRequestsPerSec == null;
}