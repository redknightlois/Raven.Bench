using RavenBench.Reporting;

namespace RavenBench.Analysis;

public static class KneeFinder
{
    /// <summary>
    /// Finds the knee where added concurrency no longer produces meaningful throughput gains
    /// and latency rises significantly. Heuristics:
    /// - Don’t declare a knee before entering a "danger zone" (p50 >= 100 ms)
    /// - Primary rule: Δthr below threshold while Δp95 above threshold
    /// - Confirmation: if the next step still increases throughput >3% vs prev, defer knee
    /// - Always stop on excessive errors
    /// </summary>
    public static StepResult? FindKnee(IReadOnlyList<StepResult> steps, double dThr, double dP95, double maxErr)
    {
        const double P50DangerMs = 100.0;
        const double NextRecoveryThreshold = 0.03; // 3%

        if (steps.Count == 0)
            return null;
        if (steps.Count == 1)
        {
            var only = steps[0];
            only.Reason = "single-step";
            return only;
        }

        for (int i = 1; i < steps.Count; i++)
        {
            var prev = steps[i - 1];
            var cur = steps[i];

            // Respect error ceiling immediately
            if (cur.ErrorRate > maxErr)
            {
                prev.Reason = $"errors>{maxErr:P1}";
                return prev;
            }

            // Don’t consider knees until we are in danger zone
            var inDanger = prev.Raw.P50 >= P50DangerMs || cur.Raw.P50 >= P50DangerMs;
            if (inDanger == false)
                continue;

            var dthr = prev.Throughput > 0 ? (cur.Throughput - prev.Throughput) / prev.Throughput : 0.0;
            var dp95 = prev.Raw.P95 > 0 ? (cur.Raw.P95 - prev.Raw.P95) / (prev.Raw.P95 + 1e-9) : 0.0;

            // Smoothing with previous deltas if possible
            if (i >= 2)
            {
                var p2 = steps[i - 2];
                var dthrAvg = 0.5 * ((prev.Throughput - p2.Throughput) / Math.Max(1e-9, p2.Throughput)) + 0.5 * dthr;
                var dp95Avg = 0.5 * ((prev.Raw.P95 - p2.Raw.P95) / Math.Max(1e-9, p2.Raw.P95)) + 0.5 * dp95;
                if (dthrAvg < dThr && dp95Avg > dP95)
                {
                    // If the next step still recovers >3% throughput vs prev, defer
                    if (i + 1 < steps.Count)
                    {
                        var next = steps[i + 1];
                        var rec = prev.Throughput > 0 ? (next.Throughput - prev.Throughput) / prev.Throughput : 0.0;
                        if (rec > NextRecoveryThreshold)
                            continue;
                    }
                    prev.Reason = $"Δthr<{dThr:P0} & Δp95>{dP95:P0} (smoothed)";
                    return prev;
                }
            }

            // Direct rule
            if (dthr < dThr && dp95 > dP95)
            {
                if (i + 1 < steps.Count)
                {
                    var next = steps[i + 1];
                    var rec = prev.Throughput > 0 ? (next.Throughput - prev.Throughput) / prev.Throughput : 0.0;
                    if (rec > NextRecoveryThreshold)
                        continue;
                }
                prev.Reason = $"Δthr<{dThr:P0} & Δp95>{dP95:P0}";
                return prev;
            }

            // Monotonic degradation
            if (cur.Throughput < prev.Throughput && cur.Raw.P95 > prev.Raw.P95)
            {
                prev.Reason = "Thr↓ & p95↑";
                return prev;
            }
        }

        var last = steps[^1];
        last.Reason = "end-of-range";
        return last;
    }
}

