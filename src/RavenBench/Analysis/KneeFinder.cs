using RavenBench.Core.Reporting;

namespace RavenBench.Analysis;

public static class KneeFinder
{
    /// <summary>Starts the reason of a knee that the next step's client-bound marking stopped.</summary>
    public const string ClientBoundReason = "client-bound";

    /// <summary>
    /// Finds the knee where added concurrency no longer produces meaningful quality gains.
    /// Quality = throughput / P99.9 latency (higher is better).
    /// Heuristics:
    /// - Don't declare a knee before entering a "danger zone" (p50 >= 100 ms)
    /// - Primary rule: quality degrades (current quality < previous quality)
    /// - Confirmation: if the next step still recovers quality >3% vs prev, defer knee
    /// - Always stop on excessive errors or on an invalid (client-bound) step: such a step is never the
    ///   knee, so a run whose first step is one has no knee (null)
    /// - A ramp that ends without a knee reports its last step, degraded when that step is in the
    ///   danger zone or has errors
    /// </summary>
    public static StepResult? FindKnee(IReadOnlyList<StepResult> steps, double maxErr)
    {
        const double P50DangerMs = 100.0;
        const double NextRecoveryThreshold = 0.03; // 3%

        // Helper to calculate quality score (throughput / P99.9)
        double Quality(StepResult s) => s.Raw.P999 > 0 ? s.Throughput / s.Raw.P999 : 0.0;

        for (int i = 0; i < steps.Count; i++)
        {
            if (steps[i].ErrorRate > maxErr)
                return i == 0 ? null : Knee(steps[i - 1], $"errors>{maxErr:P1}", degraded: true);
            if (steps[i].InvalidReason != null)
                return i == 0 ? null : Knee(steps[i - 1], $"{ClientBoundReason}: {steps[i].InvalidReason}", degraded: true);
            if (i == 0)
                continue;

            var prev = steps[i - 1];
            var cur = steps[i];

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
                return Knee(prev, $"Quality↓ (Q={prevQuality:F1} → {curQuality:F1})", degraded: true);
            }

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
                    return Knee(prev, $"Quality stagnant (smoothed ΔQ={dQualityAvg:P1})", degraded: true);
                }
            }
        }

        if (steps.Count == 0)
            return null;
        var last = steps[^1];
        var end = steps.Count == 1 ? "single-step: no comparison between steps" : "end-of-range";
        return last.Raw.P50 >= P50DangerMs || last.ErrorRate > 0
            ? Knee(last, $"{end}, degraded (P50={last.Raw.P50:F1} ms, errors={last.ErrorRate:P2})", degraded: true)
            : Knee(last, end, degraded: false);
    }

    private static StepResult Knee(StepResult step, string reason, bool degraded)
    {
        step.Reason = reason;
        step.KneeDegraded = degraded;
        return step;
    }
}

