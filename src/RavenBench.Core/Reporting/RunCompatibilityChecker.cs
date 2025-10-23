using RavenBench.Core;

namespace RavenBench.Core.Reporting;

/// <summary>
/// Checks if multiple benchmark summaries are compatible for comparison.
/// Compatible runs must have the same profile, dataset, transport, HTTP version, and query profile.
/// </summary>
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
        if (!AreComparable(summaries))
            throw new InvalidOperationException("Benchmark summaries are not compatible for comparison. They must have the same profile, dataset, transport, HTTP version, and query profile.");
    }

    private static bool IsCompatible(BenchmarkSummary baseline, BenchmarkSummary other)
    {
        return baseline.Options.Profile == other.Options.Profile &&
               baseline.Options.Dataset == other.Options.Dataset &&
               baseline.Options.Transport == other.Options.Transport &&
               baseline.EffectiveHttpVersion == other.EffectiveHttpVersion &&
               baseline.Options.QueryProfile == other.Options.QueryProfile;
    }
}