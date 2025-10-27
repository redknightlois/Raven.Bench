using RavenBench.Core;

namespace RavenBench.Core.Reporting;

/// <summary>
/// Checks if multiple benchmark summaries are compatible for comparison.
/// Compatible runs must have the same workload profile, dataset, and query profile.
/// Transport and HTTP version are ALLOWED to differ - that's the point of comparison!
/// </summary>
/// <remarks>
/// The comparison feature is designed to contrast different configurations:
/// - Client vs Raw transport
/// - HTTP/1.1 vs HTTP/2.0 vs HTTP/3.0
/// - Different compression settings
/// - Before/after performance changes
///
/// Therefore, we only enforce compatibility on workload characteristics (profile, dataset, query type),
/// not on transport/protocol configuration which is what we want to compare.
/// </remarks>
public static class RunCompatibilityChecker
{
    /// <summary>
    /// Determines if the provided benchmark summaries can be compared.
    /// </summary>
    /// <param name="summaries">The benchmark summaries to check.</param>
    /// <returns>True if all summaries are compatible; otherwise, false.</returns>
    public static bool AreComparable(params BenchmarkSummary[] summaries)
    {
        if (summaries.Length < 2)
            return true;

        var baseline = summaries[0];
        return summaries.All(s => IsCompatible(baseline, s));
    }

    /// <summary>
    /// Ensures that the provided benchmark summaries are compatible, throwing an exception if not.
    /// </summary>
    /// <param name="summaries">The benchmark summaries to check.</param>
    /// <exception cref="InvalidOperationException">Thrown if the summaries are not compatible.</exception>
    public static void EnsureComparable(params BenchmarkSummary[] summaries)
    {
        if (AreComparable(summaries))
            return;

        // Provide detailed error message about what's incompatible
        var baseline = summaries[0];
        var incompatibilities = new List<string>();

        for (int i = 1; i < summaries.Length; i++)
        {
            var other = summaries[i];
            if (baseline.Options.Profile != other.Options.Profile)
                incompatibilities.Add($"Run {i + 1} has different workload profile: {baseline.Options.Profile} vs {other.Options.Profile}");

            if (baseline.Options.Dataset != other.Options.Dataset)
                incompatibilities.Add($"Run {i + 1} has different dataset: {baseline.Options.Dataset} vs {other.Options.Dataset}");

            if (baseline.Options.QueryProfile != other.Options.QueryProfile)
                incompatibilities.Add($"Run {i + 1} has different query profile: {baseline.Options.QueryProfile} vs {other.Options.QueryProfile}");
        }

        var message = "Benchmark summaries are not compatible for comparison. " +
                      "They must have the same workload profile, dataset, and query profile.\n" +
                      string.Join("\n", incompatibilities);

        throw new InvalidOperationException(message);
    }

    /// <summary>
    /// Checks if two summaries have compatible workload characteristics.
    /// Transport, HTTP version, and compression settings are allowed to differ.
    /// </summary>
    private static bool IsCompatible(BenchmarkSummary baseline, BenchmarkSummary other)
    {
        // Only enforce matching workload characteristics, not transport/protocol config
        // The whole point of comparison is to see how different transports/HTTP versions perform!
        return baseline.Options.Profile == other.Options.Profile &&
               baseline.Options.Dataset == other.Options.Dataset &&
               baseline.Options.QueryProfile == other.Options.QueryProfile;
    }
}