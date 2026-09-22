namespace RavenBench.Core.Metrics;

/// <summary>
/// The single home of the load-host saturation threshold. The per-step invalid marking and the
/// client-limited verdict both read it, so they cannot disagree. The scale is the 0..1 fraction of
/// the load host's total CPU capacity that <see cref="ProcessCpuTracker"/> reports.
/// </summary>
public static class ClientSaturation
{
    /// <summary>
    /// A load host at or above this fraction of its capacity is saturated: the step measures the
    /// load host rather than the server, so its number must not be published.
    /// </summary>
    public const double Threshold = 0.85;

    /// <summary>True when the load host's CPU over a step's measurement window reached the threshold.</summary>
    public static bool IsSaturated(double clientCpu) => clientCpu >= Threshold;

    /// <summary>
    /// Why a step with this load-host CPU must not be published, or null when it is publishable.
    /// The one place a step's validity is decided.
    /// </summary>
    public static string? MarkingFor(double clientCpu) =>
        IsSaturated(clientCpu)
            ? $"client-bound: the load host spent {clientCpu:P1} of its CPU over this step's measurement window, at or above the {Threshold:P0} saturation threshold; this number must not be published"
            : null;

    /// <summary>
    /// The console line an invalid step prints on the normal output. It names the step, the ycsb
    /// run that produced it when there is one, and why the number must not be published.
    /// </summary>
    public static string ConsoleLine(int stepNumber, int concurrency, string? runName, string reason)
    {
        var identity = runName != null
            ? $"Step {stepNumber} of run '{runName}' (concurrency {concurrency})"
            : $"Step {stepNumber} (concurrency {concurrency})";

        return $"[Raven.Bench] {identity} is INVALID: {reason}";
    }
}
