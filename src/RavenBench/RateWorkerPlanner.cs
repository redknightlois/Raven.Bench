using RavenBench.Core;

namespace RavenBench;

internal static class RateWorkerPlanner
{
    /// <summary>
    /// Sizes the rate workers by Little's Law from the target operation rate and a service time in seconds per operation.
    /// The service time is the mean latency the previous step observed, or the baseline RTT before any step ran.
    /// An automatic count grows at most 2x over <paramref name="previousAutoWorkers"/>; an explicit RateWorkers option wins.
    /// </summary>
    internal static int ResolveRateWorkerCount(RunOptions opts, int targetRps, long baselineLatencyMicros, double? observedServiceTimeSeconds = null, int? previousAutoWorkers = null)
    {
        if (opts == null)
            throw new ArgumentNullException(nameof(opts));

        if (targetRps <= 0)
            return Math.Max(1, opts.RateWorkers ?? 32);

        if (opts.RateWorkers.HasValue)
            return Math.Max(1, opts.RateWorkers.Value);

        const double fallbackBaselineSeconds = 0.002; // Assume 2 ms RTT when calibration is unavailable
        var baselineSeconds = baselineLatencyMicros > 0
            ? Math.Max(baselineLatencyMicros / 1_000_000.0, 1e-6)
            : fallbackBaselineSeconds;

        // Little's Law: concurrency ~= throughput * latency. Add 1.5x headroom to absorb jitter.
        var effectiveSeconds = observedServiceTimeSeconds.HasValue && observedServiceTimeSeconds.Value > 0
            ? Math.Max(observedServiceTimeSeconds.Value, baselineSeconds)
            : baselineSeconds;

        var estimatedConcurrency = targetRps * effectiveSeconds;
        var plannedWorkers = (int)Math.Ceiling(Math.Max(estimatedConcurrency * 1.5, 1));

        const int minWorkers = 32;
        const int maxWorkers = 16384;

        plannedWorkers = Math.Clamp(plannedWorkers, minWorkers, maxWorkers);
        return previousAutoWorkers.HasValue ? Math.Min(plannedWorkers, previousAutoWorkers.Value * 2) : plannedWorkers;
    }
}
