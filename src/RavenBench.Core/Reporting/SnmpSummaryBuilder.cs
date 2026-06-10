using System;
using System.Collections.Generic;
using RavenBench.Core.Metrics;

namespace RavenBench.Core.Reporting;

public static class SnmpSummaryBuilder
{
    public static (List<SnmpTimeSeries>?, SnmpAggregations?) Build(List<ServerMetrics>? history)
    {
        if (history == null || history.Count == 0)
            return (null, null);

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

        var (totalReadOps, averageReadOps) = IntegrateAndAverage(history, h => h.SnmpIoReadOpsPerSec);
        var (totalWriteOps, averageWriteOps) = IntegrateAndAverage(history, h => h.SnmpIoWriteOpsPerSec);
        var (totalReadBytes, averageReadBytes) = IntegrateAndAverage(history, h => h.SnmpIoReadBytesPerSec);
        var (totalWriteBytes, averageWriteBytes) = IntegrateAndAverage(history, h => h.SnmpIoWriteBytesPerSec);

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

    private static (double? Total, double? Average) IntegrateAndAverage(List<ServerMetrics> history, Func<ServerMetrics, double?> selector)
    {
        double total = 0;
        double sum = 0;
        int count = 0;
        bool integrated = false;

        for (int i = 0; i < history.Count; i++)
        {
            var value = selector(history[i]);
            if (value == null)
                continue;

            sum += value.Value;
            count++;

            if (i == 0)
                continue;

            double elapsedSeconds = (history[i].Timestamp - history[i - 1].Timestamp).TotalSeconds;
            if (elapsedSeconds > 0)
            {
                total += value.Value * elapsedSeconds;
                integrated = true;
            }
        }

        return (integrated ? total : null, count > 0 ? sum / count : null);
    }
}
