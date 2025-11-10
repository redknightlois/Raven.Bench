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
                ServerSnmpRequestsPerSec = sample.ServerSnmpRequestsPerSec,
                SnmpIoReadOpsPerSec = sample.SnmpIoReadOpsPerSec,
                SnmpIoWriteOpsPerSec = sample.SnmpIoWriteOpsPerSec,
                SnmpIoReadBytesPerSec = sample.SnmpIoReadBytesPerSec,
                SnmpIoWriteBytesPerSec = sample.SnmpIoWriteBytesPerSec
            });
        }

        var (totalReadOps, countReadOps) = history.SumAndCount(h => h.SnmpIoReadOpsPerSec);
        var (totalWriteOps, countWriteOps) = history.SumAndCount(h => h.SnmpIoWriteOpsPerSec);
        var (totalReadBytes, countReadBytes) = history.SumAndCount(h => h.SnmpIoReadBytesPerSec);
        var (totalWriteBytes, countWriteBytes) = history.SumAndCount(h => h.SnmpIoWriteBytesPerSec);

        var aggregations = new SnmpAggregations
        {
            TotalSnmpIoReadOps = countReadOps > 0 ? totalReadOps : null,
            AverageSnmpIoReadOpsPerSec = countReadOps > 0 ? totalReadOps / countReadOps : null,
            TotalSnmpIoWriteOps = countWriteOps > 0 ? totalWriteOps : null,
            AverageSnmpIoWriteOpsPerSec = countWriteOps > 0 ? totalWriteOps / countWriteOps : null,
            TotalSnmpIoReadBytes = countReadBytes > 0 ? totalReadBytes : null,
            AverageSnmpIoReadBytesPerSec = countReadBytes > 0 ? totalReadBytes / countReadBytes : null,
            TotalSnmpIoWriteBytes = countWriteBytes > 0 ? totalWriteBytes : null,
            AverageSnmpIoWriteBytesPerSec = countWriteBytes > 0 ? totalWriteBytes / countWriteBytes : null
        };

        return (timeSeries, aggregations);
    }
}
