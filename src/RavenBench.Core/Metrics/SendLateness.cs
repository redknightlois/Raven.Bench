using RavenBench.Core.Reporting;

namespace RavenBench.Core.Metrics;

/// <summary>
/// A rate step whose requests left late by a large fraction of their latency measures the client's
/// pacing rather than the server.
/// </summary>
public static class SendLateness
{
    /// <summary>Median send lateness at or above this fraction of median latency marks the step client-bound.</summary>
    public const double Threshold = 0.5;

    /// <summary>
    /// Why a rate step with this send lateness must not be published, or null when it is publishable
    /// or the step has no schedule.
    /// </summary>
    public static string? MarkingFor(Percentiles? lateness, double latencyP50Ms)
    {
        if (lateness is not { } late || late.P50 < Threshold * latencyP50Ms || late.P50 <= 0)
            return null;

        return $"client-bound: requests left a median {late.P50:F3} ms (p99 {late.P99:F3} ms) after their scheduled time against a median latency of {latencyP50Ms:F3} ms, at or above the {Threshold:P0} threshold; the load host did not keep the schedule, so this number must not be published";
    }
}
