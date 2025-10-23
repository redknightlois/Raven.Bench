using System;
using System.Collections.Generic;
using System.Linq;
using RavenBench.Core.Reporting;
using RavenBench.Core;
using Spectre.Console;

namespace RavenBench.Cli
{
    [Flags]
    public enum TableScope
    {
        None = 0,
        Normalized = 1,
        Raw = 2,
        Both = Normalized | Raw
    }

    public record MetricColumn(
        string Header,
        Func<BenchmarkSummary, bool> IsVisible,
        Func<StepResult, string> ValueSelector,
        TableScope Scope,
        Action<IColumn>? ConfigureColumn = null
    );

    public static class TableMetrics
    {
        private static string FormatNumber(double? value, string format = "F1") => value?.ToString(format) ?? "N/A";
        private static string FormatInt(long? value) => value?.ToString() ?? "N/A";
        private static string FormatPercent(double value) => (value * 100).ToString("F1");

        public static readonly IReadOnlyList<MetricColumn> AllColumns = new List<MetricColumn>
        {
            // Common Columns
            new("Concurrency", _ => true, s => s.Concurrency.ToString(), TableScope.Both),
            new("Thr/s", _ => true, s => s.Throughput.ToString("F0"), TableScope.Both, c => c.RightAligned()),
            new("Server Req/s", summary => summary.Options.SnmpEnabled && summary.Options.Snmp.Profile == SnmpProfile.Extended && summary.Steps.Any(s => s.ServerSnmpRequestsPerSec.HasValue), s => FormatNumber(s.ServerSnmpRequestsPerSec, "F0"), TableScope.Both, c => c.RightAligned()),
            
            // Latency
            new("p50 ms", _ => true, s => FormatNumber(s.Normalized.P50), TableScope.Normalized, c => c.RightAligned()),
            new("p90 ms", _ => true, s => FormatNumber(s.Normalized.P90), TableScope.Normalized, c => c.RightAligned()),
            new("p99 ms", _ => true, s => FormatNumber(s.Normalized.P99), TableScope.Normalized, c => c.RightAligned()),
            new("p999 ms", _ => true, s => FormatNumber(s.Normalized.P999), TableScope.Normalized, c => c.RightAligned()),
            new("p99.99 ms", _ => true, s => FormatNumber(s.NormalizedP9999), TableScope.Normalized, c => c.RightAligned()),
            new("pMax ms", _ => true, s => FormatNumber(s.NormalizedPMax), TableScope.Normalized, c => c.RightAligned()),
            new("p50 ms", _ => true, s => FormatNumber(s.Raw.P50), TableScope.Raw, c => c.RightAligned()),
            new("p90 ms", _ => true, s => FormatNumber(s.Raw.P90), TableScope.Raw, c => c.RightAligned()),
            new("p99 ms", _ => true, s => FormatNumber(s.Raw.P99), TableScope.Raw, c => c.RightAligned()),
            new("p999 ms", _ => true, s => FormatNumber(s.Raw.P999), TableScope.Raw, c => c.RightAligned()),
            new("p99.99 ms", _ => true, s => FormatNumber(s.P9999), TableScope.Raw, c => c.RightAligned()),
            new("pMax ms", _ => true, s => FormatNumber(s.PMax), TableScope.Raw, c => c.RightAligned()),

            new("Errors %", summary => summary.Steps.Any(s => s.ErrorRate > 0), s => s.ErrorRate == 0 ? "N/A" : (s.ErrorRate * 100).ToString("F1"), TableScope.Both, c => c.RightAligned()),
            
            // Client Metrics
            new("Client CPU %", _ => true, s => FormatPercent(s.ClientCpu), TableScope.Both, c => c.RightAligned()),
            new("Client Net %", _ => true, s => FormatPercent(s.NetworkUtilization), TableScope.Both, c => c.RightAligned()),

            // Server Metrics (non-SNMP)
            new("Server CPU %", summary => !summary.Options.SnmpEnabled, s => FormatNumber(s.ServerCpu), TableScope.Both, c => c.RightAligned()),
            new("Server Mem MB", summary => !summary.Options.SnmpEnabled, s => FormatInt(s.ServerMemoryMB), TableScope.Both, c => c.RightAligned()),
            
            // Server IO (non-SNMP)
            new("Server IO R", summary => summary.Steps.Any(s => s.ServerIoReadOps.HasValue), s => FormatInt(s.ServerIoReadOps), TableScope.Both, c => c.RightAligned()),
            new("Server IO W", summary => summary.Steps.Any(s => s.ServerIoWriteOps.HasValue), s => FormatInt(s.ServerIoWriteOps), TableScope.Both, c => c.RightAligned()),

            // SNMP Metrics
            new("Machine CPU %", summary => summary.Options.SnmpEnabled, s => FormatNumber(s.MachineCpu), TableScope.Both, c => c.RightAligned()),
            new("Process CPU %", summary => summary.Options.SnmpEnabled, s => FormatNumber(s.ProcessCpu), TableScope.Both, c => c.RightAligned()),
            new("Managed MB", summary => summary.Options.SnmpEnabled, s => FormatInt(s.ManagedMemoryMb), TableScope.Both, c => c.RightAligned()),
            new("Unmanaged MB", summary => summary.Options.SnmpEnabled, s => FormatInt(s.UnmanagedMemoryMb), TableScope.Both, c => c.RightAligned()),
            
            // SNMP Extended
            new("Dirty MB", summary => summary.Options.SnmpEnabled && summary.Options.Snmp.Profile == SnmpProfile.Extended && summary.Steps.Any(s => s.DirtyMemoryMb.HasValue), s => FormatInt(s.DirtyMemoryMb), TableScope.Both, c => c.RightAligned()),
            new("Load 1m", summary => summary.Options.SnmpEnabled && summary.Options.Snmp.Profile == SnmpProfile.Extended && summary.Steps.Any(s => s.Load1Min.HasValue), s => FormatNumber(s.Load1Min, "F2"), TableScope.Both, c => c.RightAligned()),
            new("Err/s", summary => summary.Options.SnmpEnabled && summary.Options.Snmp.Profile == SnmpProfile.Extended && summary.Steps.Any(s => s.SnmpErrorsPerSec.HasValue), s => FormatNumber(s.SnmpErrorsPerSec), TableScope.Both, c => c.RightAligned()),
        };

        public static List<MetricColumn> GetVisibleColumns(BenchmarkSummary summary, TableScope scope)
        {
            if (!summary.Steps.Any())
                return new List<MetricColumn>();

            return AllColumns
                .Where(c => (c.Scope & scope) != 0)
                .Where(c => c.IsVisible(summary))
                .ToList();
        }
    }
}