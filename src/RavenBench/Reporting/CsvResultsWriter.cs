using System.Globalization;

namespace RavenBench.Reporting;

public static class CsvResultsWriter
{
    public static void Write(string path, BenchmarkSummary summary, bool includeSnmp = false)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path) ?? ".");
        using var sw = new StreamWriter(path);
        var baseHeaders = "concurrency,throughput,p50_ms,p90_ms,p95_ms,p99_ms,p50_norm,p90_norm,p95_norm,p99_norm,errors,bytes_out,bytes_in,cpu,net_util,server_cpu,server_mem_mb,server_rps,server_io_read,server_io_write";
        if (includeSnmp)
            baseHeaders += ",machine_cpu,process_cpu,managed_mem_mb,unmanaged_mem_mb";
        sw.WriteLine(baseHeaders);
        foreach (var s in summary.Steps)
        {
            // Use string.Format with invariant culture for consistent CSV output
            var baseLine = string.Format(CultureInfo.InvariantCulture,
                "{0},{1:F2},{2:F2},{3:F2},{4:F2},{5:F2},{6:F2},{7:F2},{8:F2},{9:F2},{10:P4},{11},{12},{13:P2},{14:P2},{15},{16},{17},{18},{19}",
                s.Concurrency, s.Throughput, s.Raw.P50, s.Raw.P90, s.Raw.P95, s.Raw.P99,
                s.Normalized.P50, s.Normalized.P90, s.Normalized.P95, s.Normalized.P99,
                s.ErrorRate, s.BytesOut, s.BytesIn, s.ClientCpu, s.NetworkUtilization,
                s.ServerCpu?.ToString("F2") ?? "N/A",
                s.ServerMemoryMB?.ToString() ?? "N/A",
                s.ServerRequestsPerSec?.ToString("F2") ?? "N/A",
                s.ServerIoReadOps?.ToString() ?? "N/A",
                s.ServerIoWriteOps?.ToString() ?? "N/A");
            if (includeSnmp)
                baseLine += string.Format(", {0:F1}, {1:F1}, {2}, {3}",
                    s.MachineCpu ?? 0,
                    s.ProcessCpu ?? 0,
                    s.ManagedMemoryMb?.ToString() ?? "N/A",
                    s.UnmanagedMemoryMb?.ToString() ?? "N/A");
            sw.WriteLine(baseLine);
        }
    }
}

