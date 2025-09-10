using System.Globalization;

namespace RavenBench.Reporting;

public static class CsvResultsWriter
{
    public static void Write(string path, BenchmarkSummary summary)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path) ?? ".");
        using var sw = new StreamWriter(path);
        sw.WriteLine("concurrency,throughput,p50_ms,p90_ms,p95_ms,p99_ms,errors,bytes_out,bytes_in,cpu,net_util,server_cpu,server_mem_mb,server_rps,server_io_read,server_io_write");
        foreach (var s in summary.Steps)
        {
            // Use string.Format with invariant culture for consistent CSV output
            sw.WriteLine(string.Format(CultureInfo.InvariantCulture,
                "{0},{1:F2},{2:F2},{3:F2},{4:F2},{5:F2},{6:P4},{7},{8},{9:P2},{10:P2},{11},{12},{13},{14},{15}",
                s.Concurrency, s.Throughput, s.P50Ms, s.P90Ms, s.P95Ms, s.P99Ms,
                s.ErrorRate, s.BytesOut, s.BytesIn, s.ClientCpu, s.NetworkUtilization,
                s.ServerCpu?.ToString("F2") ?? "N/A", 
                s.ServerMemoryMB?.ToString() ?? "N/A", 
                s.ServerRequestsPerSec?.ToString("F2") ?? "N/A",
                s.ServerIoReadOps?.ToString() ?? "N/A",
                s.ServerIoWriteOps?.ToString() ?? "N/A"));
        }
    }
}

