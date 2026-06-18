using RavenBench.Core.Reporting;
using RavenBench.Core;

namespace RavenBench.Analysis;

/// <summary>
/// Analyzes benchmark results to determine performance bottlenecks and reliability issues.
/// Combines attribution analysis (what's limiting performance) with invariant validation (reliability warnings).
/// </summary>
public static class ResultAnalyzer
{
    public sealed class Report
    {
        public string Verdict { get; init; } = "unknown";
        public List<string> Warnings { get; } = new();
        public bool UnreliableBeyondKnee { get; set; }
    }

    /// <summary>
    /// Performs complete analysis of benchmark results including performance attribution and reliability warnings.
    /// </summary>
    public static Report Analyze(BenchmarkRun run, StepResult? knee, RunOptions opts, BenchmarkSummary summary)
    {
        var report = new Report
        {
            Verdict = BuildVerdict(run, knee, opts)
        };

        // "end-of-range" marks the fallback knee when the ramp ended without degradation
        if (knee != null && knee.Reason != "end-of-range")
        {
            report.UnreliableBeyondKnee = true;
            report.Warnings.Add($"Stop trusting numbers past C={knee.Concurrency} (knee).");
        }

        foreach (var s in summary.Steps)
        {
            if (s.ErrorRate > 0.05)
                report.Warnings.Add($"High error rate at C={s.Concurrency}: {s.ErrorRate:P2}");
        }

        return report;
    }

    private static string BuildVerdict(BenchmarkRun run, StepResult? knee, RunOptions opts)
    {
        if (knee == null) return "unknown";

        var s = knee;
        if (s.NetworkBytesMeasured && s.NetworkUtilization >= 0.85 && s.Raw.P95 > 0 && s.Throughput > 0 && IsLoopbackUrl(opts.Url) == false)
            return $"network-limited at ~{opts.LinkMbps:F0} Mb/s (est.)";
        if (s.ClientCpu >= 0.85)
            return "client-limited (CPU)";
        // ProcessCpu (SNMP) / ServerCpu (admin endpoint) are 0..100%; null compares false.
        var serverCpu = s.ProcessCpu ?? s.ServerCpu;
        if (serverCpu >= 85.0)
            return "server-limited (CPU)";
        if (serverCpu.HasValue)
            return "unknown (not CPU- or network-bound at the knee)";
        return "unknown (collect server counters for attribution)";
    }

    public static bool IsLoopbackUrl(string url)
    {
        if (string.IsNullOrWhiteSpace(url))
            return false;
        if (Uri.TryCreate(url, UriKind.Absolute, out var uri) == false)
            return false;
        var host = uri.Host;
        if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
            return true;
        if (System.Net.IPAddress.TryParse(host, out var ip))
            return System.Net.IPAddress.IsLoopback(ip);
        return false;
    }
}