using RavenBench.Reporting;

namespace RavenBench.Analysis;

public static class KneeFinder
{
    /// <summary>
    /// Finds the "knee" point in benchmark results where performance degrades.
    /// The knee is detected when throughput gains flatten while latency increases significantly,
    /// indicating the system has reached its optimal concurrency level.
    /// </summary>
    /// <param name="steps">Benchmark results ordered by increasing concurrency</param>
    /// <param name="dThr">Minimum relative throughput increase threshold (e.g., 0.05 = 5%)</param>
    /// <param name="dP95">Maximum relative P95 latency increase threshold (e.g., 0.20 = 20%)</param>
    /// <param name="maxErr">Maximum acceptable error rate before stopping</param>
    /// <returns>The step representing the knee point, or null if no knee found</returns>
    public static StepResult? FindKnee(IReadOnlyList<StepResult> steps, double dThr, double dP95, double maxErr)
    { 
        
        if (steps.Count < 2) return steps.LastOrDefault();
        
        for (int i = 1; i < steps.Count; i++)
        {
            var prev = steps[i - 1];
            var cur = steps[i];
            var dthr = prev.Throughput > 0 ? (cur.Throughput - prev.Throughput) / prev.Throughput : 0;
            var dp95 = prev.P95Ms > 0 ? (cur.P95Ms - prev.P95Ms) / (prev.P95Ms + 1e-9) : 0;
            
            // smoothing: if we have 3 points, use averaged deltas to avoid micro-wiggles
            if (i >= 2)
            {
                var p2 = steps[i - 2];
                
                // Calculate smoothed deltas by averaging current step-to-step delta with 
                // the delta from 2 steps ago, reducing sensitivity to single-step noise
                var dthrAvg = 0.5 * ((prev.Throughput - p2.Throughput) / Math.Max(1e-9, p2.Throughput)) + 0.5 * dthr;
                var dp95Avg = 0.5 * ((prev.P95Ms - p2.P95Ms) / Math.Max(1e-9, p2.P95Ms)) + 0.5 * dp95;
                if ((dthrAvg < dThr && dp95Avg > dP95) || cur.ErrorRate > maxErr)
                {
                    prev.Reason = (cur.ErrorRate > maxErr) ? $"errors>{maxErr:P1}" : $"Δthr<{dThr:P0} & Δp95>{dP95:P0} (smoothed)";
                    return prev;
                }
            }
            if ((dthr < dThr && dp95 > dP95) || cur.ErrorRate > maxErr)
            {
                prev.Reason = (cur.ErrorRate > maxErr) ? $"errors>{maxErr:P1}" : $"Δthr<{dThr:P0} & Δp95>{dP95:P0}";
                return prev;
            }
            
            if (cur.Throughput < prev.Throughput && cur.P95Ms > prev.P95Ms)
            {
                prev.Reason = "Thr↓ & p95↑";
                return prev;
            }
        }
        var last = steps.Last();
        last.Reason = "end-of-range";
        return last;
    }
}
