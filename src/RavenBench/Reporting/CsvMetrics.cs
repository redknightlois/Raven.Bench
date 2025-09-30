using System;
using System.Collections.Generic;
using System.Linq;
using RavenBench.Reporting;
using RavenBench.Util;

namespace RavenBench.Reporting
{
    public record CsvField(
        string Name,
        Func<BenchmarkSummary, bool> IsVisible,
        Func<StepResult, object?> ValueSelector
    );

    public static class CsvMetrics
    {
        public static readonly IReadOnlyList<CsvField> AllFields = new List<CsvField>
        {
            new("Concurrency", _ => true, s => s.Concurrency),
            new("Throughput", _ => true, s => s.Throughput),
            new("ErrorRate", _ => true, s => s.ErrorRate),
            new("BytesOut", _ => true, s => s.BytesOut),
            new("BytesIn", _ => true, s => s.BytesIn),
            new("Raw.P50", _ => true, s => s.Raw.P50),
            new("Raw.P90", _ => true, s => s.Raw.P90),
            new("Raw.P95", _ => true, s => s.Raw.P95),
            new("Raw.P99", _ => true, s => s.Raw.P99),
            new("Normalized.P50", _ => true, s => s.Normalized.P50),
            new("Normalized.P90", _ => true, s => s.Normalized.P90),
            new("Normalized.P95", _ => true, s => s.Normalized.P95),
            new("Normalized.P99", _ => true, s => s.Normalized.P99),
            new("ClientCpu", _ => true, s => s.ClientCpu),
            new("NetworkUtilization", _ => true, s => s.NetworkUtilization),
            
            // Server metrics
            new("ServerCpu", summary => !summary.Options.SnmpEnabled, s => s.ServerCpu),
            new("ServerMemoryMB", summary => !summary.Options.SnmpEnabled, s => s.ServerMemoryMB),
            new("ServerRequestsPerSec", summary => !summary.Options.SnmpEnabled, s => s.ServerRequestsPerSec),
            new("ServerIoReadOps", summary => !summary.Options.SnmpEnabled, s => s.ServerIoReadOps),
            new("ServerIoWriteOps", summary => !summary.Options.SnmpEnabled, s => s.ServerIoWriteOps),
            new("ServerIoReadKb", summary => !summary.Options.SnmpEnabled, s => s.ServerIoReadKb),
            new("ServerIoWriteKb", summary => !summary.Options.SnmpEnabled, s => s.ServerIoWriteKb),

            // SNMP metrics
            new("MachineCpu", summary => summary.Options.SnmpEnabled, s => s.MachineCpu),
            new("ProcessCpu", summary => summary.Options.SnmpEnabled, s => s.ProcessCpu),
            new("ManagedMemoryMb", summary => summary.Options.SnmpEnabled, s => s.ManagedMemoryMb),
            new("UnmanagedMemoryMb", summary => summary.Options.SnmpEnabled, s => s.UnmanagedMemoryMb),
            new("DirtyMemoryMb", summary => summary.Options.SnmpEnabled && summary.Options.Snmp.Profile == SnmpProfile.Extended, s => s.DirtyMemoryMb),
            new("Load1Min", summary => summary.Options.SnmpEnabled && summary.Options.Snmp.Profile == SnmpProfile.Extended, s => s.Load1Min),
            new("SnmpIoReadOpsPerSec", summary => summary.Options.SnmpEnabled, s => s.SnmpIoReadOpsPerSec),
            new("SnmpIoWriteOpsPerSec", summary => summary.Options.SnmpEnabled, s => s.SnmpIoWriteOpsPerSec),
            new("SnmpIoReadBytesPerSec", summary => summary.Options.SnmpEnabled, s => s.SnmpIoReadBytesPerSec),
            new("SnmpIoWriteBytesPerSec", summary => summary.Options.SnmpEnabled, s => s.SnmpIoWriteBytesPerSec),
            new("ServerSnmpRequestsPerSec", summary => summary.Options.SnmpEnabled, s => s.ServerSnmpRequestsPerSec),
            new("SnmpErrorsPerSec", summary => summary.Options.SnmpEnabled && summary.Options.Snmp.Profile == SnmpProfile.Extended, s => s.SnmpErrorsPerSec),
        };

        public static List<CsvField> GetVisibleFields(BenchmarkSummary summary)
        {
            return AllFields
                .Where(c => c.IsVisible(summary))
                .ToList();
        }
    }
}