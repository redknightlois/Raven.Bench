using RavenBench.Reporting;
using RavenBench.Util;

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

        // Add reliability warnings
        if (knee != null)
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
        if (s.NetworkUtilization >= 0.85 && s.P95Ms > 0 && s.Throughput > 0)
            return $"network-limited at ~{opts.LinkMbps:F0} Mb/s (est.)";
        if (s.ClientCpu >= 0.85)
            return "client-limited (CPU)";
        // Without server counters, we can't attribute server/async/disk yet.
        return "unknown (collect server counters for attribution)";
    }
}