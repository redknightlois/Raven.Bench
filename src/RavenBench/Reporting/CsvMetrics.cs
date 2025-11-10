using System;
using System.Collections.Generic;
using System.Linq;
using RavenBench.Core.Reporting;
using RavenBench.Core;

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
            new("TargetThroughput", _ => true, s => s.TargetThroughput),
            new("ErrorRate", _ => true, s => s.ErrorRate),
            new("BytesOut", _ => true, s => s.BytesOut),
            new("BytesIn", _ => true, s => s.BytesIn),
            new("Raw.P50", _ => true, s => s.Raw.P50),
            new("Raw.P75", _ => true, s => s.Raw.P75),
            new("Raw.P90", _ => true, s => s.Raw.P90),
            new("Raw.P95", _ => true, s => s.Raw.P95),
            new("Raw.P99", _ => true, s => s.Raw.P99),
            new("Raw.P999", _ => true, s => s.Raw.P999),
            new("Raw.P9999", _ => true, s => s.P9999),
            new("Raw.PMax", _ => true, s => s.PMax),
            new("Normalized.P50", _ => true, s => s.Normalized.P50),
            new("Normalized.P75", _ => true, s => s.Normalized.P75),
            new("Normalized.P90", _ => true, s => s.Normalized.P90),
            new("Normalized.P95", _ => true, s => s.Normalized.P95),
            new("Normalized.P99", _ => true, s => s.Normalized.P99),
            new("Normalized.P999", _ => true, s => s.Normalized.P999),
            new("Normalized.P9999", _ => true, s => s.NormalizedP9999),
            new("Normalized.PMax", _ => true, s => s.NormalizedPMax),
            new("SampleCount", _ => true, s => s.SampleCount),
            new("CorrectedCount", _ => true, s => s.CorrectedCount),
            new("ScheduledOperations", _ => true, s => s.ScheduledOperations),
            new("MaxTimestamp", _ => true, s => s.MaxTimestamp?.ToString("O")),
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

            // Query metadata - visible when any step has query metadata
            new("QueryOperations", summary => summary.Steps.Any(s => s.QueryOperations.HasValue), s => s.QueryOperations),
            new("MinResultCount", summary => summary.Steps.Any(s => s.MinResultCount.HasValue), s => s.MinResultCount),
            new("MaxResultCount", summary => summary.Steps.Any(s => s.MaxResultCount.HasValue), s => s.MaxResultCount),
            new("AvgResultCount", summary => summary.Steps.Any(s => s.AvgResultCount.HasValue), s => s.AvgResultCount),
            new("TotalResults", summary => summary.Steps.Any(s => s.TotalResults.HasValue), s => s.TotalResults),
            new("StaleQueryCount", summary => summary.Steps.Any(s => s.StaleQueryCount.HasValue), s => s.StaleQueryCount),
            new("QueryProfile", summary => summary.Steps.Any(s => s.QueryProfile.HasValue), s => s.QueryProfile?.ToString()),
            new("PrimaryIndex", summary => summary.Steps.Any(s => s.IndexUsage != null && s.IndexUsage.Count > 0), s => s.IndexUsage?.OrderByDescending(kvp => kvp.Value).FirstOrDefault().Key),
            new("PrimaryIndexUsage", summary => summary.Steps.Any(s => s.IndexUsage != null && s.IndexUsage.Count > 0), s => s.IndexUsage?.OrderByDescending(kvp => kvp.Value).FirstOrDefault().Value),
        };

        public static List<CsvField> GetVisibleFields(BenchmarkSummary summary)
        {
            return AllFields
                .Where(c => c.IsVisible(summary))
                .ToList();
        }
    }
}