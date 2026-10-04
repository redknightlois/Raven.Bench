using System;
using System.Collections.Generic;
using RavenBench.Core.Metrics;

namespace RavenBench.Core.Reporting;

public static class SnmpSummaryBuilder
{
    /// <param name="windows">The measurement windows; the totals and averages integrate only time inside them.</param>
    public static (List<SnmpTimeSeries>?, SnmpAggregations?) Build(List<ServerMetrics>? history, IReadOnlyList<MeasurementWindow>? windows)
    {
        if (history == null || history.Count == 0)
            return (null, null);
        if (windows == null)
            throw new ArgumentNullException(nameof(windows), "An SNMP history needs the measurement windows it was polled in.");

        var timeSeries = new List<SnmpTimeSeries>(history.Count);
        foreach (var sample in history)
        {
            timeSeries.Add(new SnmpTimeSeries
            {
                Timestamp = sample.Timestamp,
                MachineCpu = sample.MachineCpu,
                ProcessCpu = sample.ProcessCpu,
                ManagedMemoryMb = sample.ManagedMemoryMb,
                UnmanagedMemoryMb = sample.UnmanagedMemoryMb,
                DirtyMemoryMb = sample.DirtyMemoryMb,
                Load1Min = sample.Load1Min,
                ServerSnmpRequestsPerSec = sample.ServerSnmpRequestsPerSec,
                SnmpIoReadOpsPerSec = sample.SnmpIoReadOpsPerSec,
                SnmpIoWriteOpsPerSec = sample.SnmpIoWriteOpsPerSec,
                SnmpIoReadBytesPerSec = sample.SnmpIoReadBytesPerSec,
                SnmpIoWriteBytesPerSec = sample.SnmpIoWriteBytesPerSec
            });
        }

        var (totalReadOps, averageReadOps) = IntegrateAndAverage(history, windows, h => h.SnmpIoReadOpsPerSec);
        var (totalWriteOps, averageWriteOps) = IntegrateAndAverage(history, windows, h => h.SnmpIoWriteOpsPerSec);
        var (totalReadBytes, averageReadBytes) = IntegrateAndAverage(history, windows, h => h.SnmpIoReadBytesPerSec);
        var (totalWriteBytes, averageWriteBytes) = IntegrateAndAverage(history, windows, h => h.SnmpIoWriteBytesPerSec);

        var aggregations = new SnmpAggregations
        {
            TotalSnmpIoReadOps = totalReadOps,
            AverageSnmpIoReadOpsPerSec = averageReadOps,
            TotalSnmpIoWriteOps = totalWriteOps,
            AverageSnmpIoWriteOpsPerSec = averageWriteOps,
            TotalSnmpIoReadBytes = totalReadBytes,
            AverageSnmpIoReadBytesPerSec = averageReadBytes,
            TotalSnmpIoWriteBytes = totalWriteBytes,
            AverageSnmpIoWriteBytesPerSec = averageWriteBytes
        };

        return (timeSeries, aggregations);
    }

    // Each sample's rate holds over the interval since the previous sample; only the part of that interval inside a window counts.
    // The average is the total over the integrated seconds, so total = average × integrated window time.
    private static (double? Total, double? Average) IntegrateAndAverage(List<ServerMetrics> history, IReadOnlyList<MeasurementWindow> windows, Func<ServerMetrics, double?> selector)
    {
        double total = 0;
        double seconds = 0;

        for (int i = 1; i < history.Count; i++)
        {
            if (selector(history[i]) is not { } value)
                continue;

            var from = history[i - 1].Timestamp;
            var to = history[i].Timestamp;
            foreach (var window in windows)
            {
                var overlap = (Min(to, window.End) - Max(from, window.Start)).TotalSeconds;
                if (overlap <= 0)
                    continue;
                total += value * overlap;
                seconds += overlap;
            }
        }

        return seconds > 0 ? (total, total / seconds) : (null, null);
    }

    private static DateTime Min(DateTime a, DateTime b) => a < b ? a : b;
    private static DateTime Max(DateTime a, DateTime b) => a > b ? a : b;
}
