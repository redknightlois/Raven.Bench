using System;
using System.Linq;
using RavenBench.Core;

namespace RavenBench;

/// <summary>
/// Formats warmup stability diagnostics into human-readable console output.
/// </summary>
public static class WarmupStabilityReporter
{
    public static void Report(WarmupSummary summary, int stepNumber)
    {
        if (summary == null) throw new ArgumentNullException(nameof(summary));
        if (summary == WarmupSummary.Skipped || summary.Iterations.Count == 0)
            return;

        var iterations = summary.Iterations;
        Console.WriteLine($"[Warmup] Step {stepNumber}: {iterations.Count} iteration(s), converged={summary.Converged}");

        foreach (var iteration in iterations)
        {
            var p95Ms = iteration.P95Micros / 1000.0;
            var p99Ms = iteration.P99Micros / 1000.0;
            var maxMs = iteration.MaxMicros / 1000.0;
            Console.WriteLine(
                $"[Warmup]   #{iteration.Iteration} duration={iteration.Duration.TotalSeconds:F1}s " +
                $"throughput={iteration.Throughput:F1} ops/s errorRate={iteration.ErrorRate * 100:F2}% " +
                $"p95={p95Ms:F2}ms p99={p99Ms:F2}ms max={maxMs:F2}ms");
        }

        if (summary.Converged == false && summary.Reason != WarmupFailureReason.None)
        {
            Console.WriteLine($"[Warmup]   warning: warmup did not converge (reason: {summary.Reason})");
        }
    }
}
