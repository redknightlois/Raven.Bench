using System;
using System.Collections.Generic;
using System.Linq;

namespace RavenBench.Core;

/// <summary>
/// Aggregated warmup execution metadata describing convergence and stability state.
/// </summary>
public sealed class WarmupSummary
{
    public static WarmupSummary Skipped { get; } = new WarmupSummary(Array.Empty<WarmupDiagnostics>(), converged: false, WarmupFailureReason.Disabled);

    internal WarmupSummary(IReadOnlyList<WarmupDiagnostics> iterations, bool converged, WarmupFailureReason reason)
    {
        Iterations = iterations ?? throw new ArgumentNullException(nameof(iterations));
        Converged = converged;
        Reason = reason;
    }

    public IReadOnlyList<WarmupDiagnostics> Iterations { get; }
    public bool Converged { get; }
    public WarmupFailureReason Reason { get; }
}

public enum WarmupFailureReason
{
    None = 0,
    Disabled,
    MaxIterations,
    HighErrorRate,
    LatencyInstability
}

/// <summary>
/// Provides heuristics to evaluate warmup stability across iterations.
/// </summary>
public static class WarmupStabilityHeuristics
{
    private const double MaxAllowedP95DriftRatio = 0.10; // 10% drift permitted between iterations
    private const double MaxErrorRateDuringWarmup = 0.20; // Abort if >=20% errors during warmup

    public static bool HasConverged(IReadOnlyList<WarmupDiagnostics> iterations)
    {
        if (iterations == null || iterations.Count == 0)
            return false;

        if (iterations.Any(i => i.ErrorRate >= MaxErrorRateDuringWarmup))
            return false;

        if (iterations.Count == 1)
            return true;

        var current = iterations[^1];
        var previous = iterations[^2];

        if (previous.SampleCount == 0 || current.SampleCount == 0)
            return false;

        var baseline = Math.Max(previous.P95Micros, 1.0);
        var driftRatio = Math.Abs(current.P95Micros - previous.P95Micros) / baseline;
        return driftRatio <= MaxAllowedP95DriftRatio;
    }

    public static WarmupSummary BuildSummary(
        IReadOnlyList<WarmupDiagnostics> iterations,
        bool requireConvergence,
        int maxIterations)
    {
        if (iterations == null || iterations.Count == 0)
            return WarmupSummary.Skipped;

        if (iterations.Any(i => i.ErrorRate >= MaxErrorRateDuringWarmup))
            return new WarmupSummary(iterations, converged: false, WarmupFailureReason.HighErrorRate);

        if (requireConvergence == false)
            return new WarmupSummary(iterations, converged: true, WarmupFailureReason.None);

        // Check if we've reached max iterations
        if (iterations.Count >= maxIterations)
        {
            // If we had to use all iterations without early convergence, that's a failure
            // even if the last check happened to pass
            return new WarmupSummary(iterations, converged: false, WarmupFailureReason.MaxIterations);
        }

        // Check convergence for cases where we haven't hit max iterations yet
        if (HasConverged(iterations))
            return new WarmupSummary(iterations, converged: true, WarmupFailureReason.None);

        // Did not converge
        return new WarmupSummary(iterations, converged: false, WarmupFailureReason.LatencyInstability);
    }
}
