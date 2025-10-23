using RavenBench.Core.Reporting;

namespace RavenBench.Analysis;

public static class KneeFinder
{
    /// <summary>
    /// Finds the knee where added concurrency no longer produces meaningful quality gains.
    /// Quality = throughput / P99.9 latency (higher is better).
    /// Heuristics:
    /// - Don't declare a knee before entering a "danger zone" (p50 >= 100 ms)
    /// - Primary rule: quality degrades (current quality < previous quality)
    /// - Confirmation: if the next step still recovers quality >3% vs prev, defer knee
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

        // Helper to calculate quality score (throughput / P99.9)
        double Quality(StepResult s) => s.Raw.P999 > 0 ? s.Throughput / s.Raw.P999 : 0.0;

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

            // Don't consider knees until we are in danger zone
            var inDanger = prev.Raw.P50 >= P50DangerMs || cur.Raw.P50 >= P50DangerMs;
            if (inDanger == false)
                continue;

            var prevQuality = Quality(prev);
            var curQuality = Quality(cur);

            // Quality degradation: current quality is lower than previous
            if (curQuality < prevQuality * 0.95) // 5% degradation threshold
            {
                // If the next step still recovers quality >3% vs prev, defer
                if (i + 1 < steps.Count)
                {
                    var next = steps[i + 1];
                    var nextQuality = Quality(next);
                    var recovery = prevQuality > 0 ? (nextQuality - prevQuality) / prevQuality : 0.0;
                    if (recovery > NextRecoveryThreshold)
                        continue;
                }
                prev.Reason = $"Quality↓ (Q={prevQuality:F1} → {curQuality:F1})";
                return prev;
            }

            // Smoothing with previous deltas if possible
            if (i >= 2)
            {
                var p2 = steps[i - 2];
                var p2Quality = Quality(p2);
                var dQualityPrev = p2Quality > 0 ? (prevQuality - p2Quality) / p2Quality : 0.0;
                var dQualityCur = prevQuality > 0 ? (curQuality - prevQuality) / prevQuality : 0.0;
                var dQualityAvg = 0.5 * dQualityPrev + 0.5 * dQualityCur;

                // If smoothed quality gain is minimal or negative
                if (dQualityAvg < 0.02) // Less than 2% quality gain
                {
                    if (i + 1 < steps.Count)
                    {
                        var next = steps[i + 1];
                        var nextQuality = Quality(next);
                        var recovery = prevQuality > 0 ? (nextQuality - prevQuality) / prevQuality : 0.0;
                        if (recovery > NextRecoveryThreshold)
                            continue;
                    }
                    prev.Reason = $"Quality stagnant (smoothed ΔQ={dQualityAvg:P1})";
                    return prev;
                }
            }
        }

        var last = steps[^1];
        last.Reason = "end-of-range";
        return last;
    }
}

