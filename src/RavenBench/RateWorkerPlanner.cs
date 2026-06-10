using RavenBench.Core;

namespace RavenBench;

internal static class RateWorkerPlanner
{
    internal static int ResolveRateWorkerCount(RunOptions opts, int targetRps, long baselineLatencyMicros)
    {
        return ResolveRateWorkerCount(opts, targetRps, baselineLatencyMicros, observedServiceTimeSeconds: null);
    }

    internal static int ResolveRateWorkerCount(RunOptions opts, int targetRps, long baselineLatencyMicros, double? observedServiceTimeSeconds)
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

        if (plannedWorkers < minWorkers)
            return minWorkers;

        if (plannedWorkers > maxWorkers)
            return maxWorkers;

        return plannedWorkers;
    }
}
